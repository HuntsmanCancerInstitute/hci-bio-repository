#!/usr/bin/env perl

use warnings;
use strict;
use English qw(-no_match_vars);
use IO::File;
use File::Find;
use File::Spec;
use POSIX qw(strftime);
use Date::Parse;
use Text::CSV;
use List::Util qw(mesh);
use Getopt::Long;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use RepoCatalog;
use RepoProject;

our $VERSION = 7.0;



######## Documentation
my $doc = <<END;

A general script to process and inventory GNomEx project folders, both
Analysis and Experiment Request types.

It will recursively scan a project folder and generate a manifest CSV
file of the fastq files including file and sequencing metadata. 


Version: $VERSION

Example usage:
    
    process_project.pl --cat Catalog.db --id 1234R
    
Required: 
    -c --cat <path>        Provide path to metadata catalog database
    -p --project <text>    GNomEx ID to the project

Options:
    --nozip                Do not generate zip archive list file
    --nodelete             Do not generate remove list file
    --verbose              Tell me everything!
 
END



######## Process command line options
my $cat_file;
my $id;
my $do_scan = 1;
my $do_zip  = 1;
my $do_del  = 1;
my $verbose;

if (scalar(@ARGV) > 1) {
	GetOptions(
		'c|catalog=s'   => \$cat_file,
		'p|project=s'   => \$id,
		'scan!'         => \$do_scan,
		'zip!'          => \$do_zip,
		'delete!'       => \$do_del,
		'verbose!'      => \$verbose,
	) or die "please recheck your options!\n\n$doc\n";
}
else {
	print $doc;
	exit;
}


######## Global variables
# these are needed since the File::Find callback doesn't accept pass through variables
my $path;
my $request;
my @removelist;
my @ziplist;
my @ten_x_crap;
my %checksums;
my $failure_count     = 0;
my $runfolder_warning = 0; # Gigantic Illumina RunFolder present
my $autoanal_warning  = 0; # Auto Analysis folder present
my $post_zip_size     = 0;
my $max_zip_size      = 200000000; # 200 MB
my $start_time        = time;

# our sequence machine IDs to platform technology lookup
my %machinelookup = (
	'D00550'  => 'Illumina HiSeq 2500',
	'D00294'  => 'Illumina HiSeq 2500',
	'A00421'  => 'Illumina NovaSeq 6000',
	'M05774'  => 'Illumina MiSeq',
	'M00736'  => 'Illumina MiSeq',
	'DQNZZQ1' => 'Illumina HiSeq 2000',
	'HWI-ST1117' => 'Illumina HiSeq 2000',
	'LH00227' => 'Illumina NovaSeq X',
);

# hash of files and data collected from scanning
# filedata{clean_name}{key} = value
# keys: File Type sample_id platform platform_unit_id paired_end Size Date MD5 clean 
# status = 1: from manifest, 2: from manifest verified, 3: new
my %filedata;




######## Check options

if ( not $id and scalar(@ARGV) ) {
	$id = shift @ARGV;
}

# grab all parameters from the catalog database if provided
if ($cat_file and $id) {
	
	# first check path
	if ($cat_file !~ m|^/|) {
		# catalog file path is not from root
		$cat_file = File::Spec->catfile( File::Spec->rel2abs(), $cat_file);
	}
	
	# find entry in catalog and collect information
	my $Catalog = RepoCatalog->new($cat_file) or 
		die "Cannot open catalog file '$cat_file'!\n";
	my $Entry = $Catalog->entry($id) or 
			die "No Catalog entry for $id\n";
	$path = $Entry->path;
	$request = $Entry->is_request;
}
else {
	print " ! A catalog file and project ID is required\n";
	exit 1;
}





# external commands
my ($gzipper, $bgzipper);
{
	# preferentially use threaded gzip compression
	$gzipper = `which pigz`;
	chomp $gzipper;
	if ($gzipper) {
		$gzipper .= ' -p 4'; # run with four cores
	}
	else {
		# default to ordinary gzip
		$gzipper = 'gzip';
	}
	
	# bgzip is desirable when we auto compress certain files
	$bgzipper = `which bgzip`;
	chomp $bgzipper;
	if ($bgzipper) {
		$bgzipper .= ' -@ 4'; # run with four cores
	}
	else {
		# default to using standard gzip compression
		$bgzipper = $gzipper;
	}
}





####### Initiate Project

# Initialize
my $Project = RepoProject->new($path, $verbose) or 
	die "unable to initiate Repository Project!\n";

printf " > working on %s at %s\n", $Project->id, $Project->given_dir;
printf "   using parent directory %s\n", $Project->parent_dir if $verbose;



# file paths
if ($verbose) {
	printf " =>  manifest file: %s\n", $Project->manifest_file;
	printf " =>    remove file: %s or %s\n", $Project->remove_file, 
		$Project->alt_remove_file;
	printf " =>  zip list file: %s or %s\n", $Project->zip_file, $Project->alt_zip_file;
	
}


# removed file hidden folder
if (-e $Project->delete_folder) {
	if ($do_scan) {
		print "! cannot re-scan if deleted files hidden folder exists!\n";
		$do_scan = 0;
		$failure_count++;
	}
}

# zipped file hidden folder
if ( -e $Project->zip_folder or -e $Project->zip_file ) {
	if ($do_scan) {
		print " ! cannot re-scan if zipped file or hidden folder exists!\n";
		$do_scan = 0;
		$failure_count++;
	}
}





######## Main functions

# change to the given directory
printf " > changing to %s\n", $Project->given_dir if $verbose;
chdir $Project->given_dir or die sprintf("cannot change to %s!\n", $Project->given_dir);

# check for files that shouldn't be here
if ( -e $Project->zip_file ) {
	printf " ! Cannot scan because Archive Zip file %s exists\n", $Project->zip_file;
	$failure_count++;
	$do_scan = 0;
}
if ( -e $Project->ziplist_file ) {
	printf " ! Cannot scan because Archive Zip list file %s exists\n",
		$Project->ziplist_file;
	$failure_count++;
	$do_scan = 0;
}
if ( -e $Project->remove_file ) {
	printf " ! Cannot scan because Remove list file %s exists\n", $Project->remove_file;
	$failure_count++;
	$do_scan = 0;
}

# scan the directory
if ($do_scan) {

	# scan the directory
	printf " > scanning %s in directory %s\n", $Project->id, $Project->parent_dir;
	scan_directory();
	
	# update scan time stamp
	if ($cat_file and -e $cat_file and not $failure_count) {
		my $Catalog = RepoCatalog->new($cat_file);
		if ( $Catalog ) {
			my $Entry = $Catalog->entry($Project->id) ;
			$Entry->scan_datestamp(time);
			print " > updated Catalog scan date stamp\n";
		}
	}
}




######## Finished
if ($failure_count) {
	printf " ! finished with %s with %d failures in %.1f minutes\n\n", $Project->id,
		$failure_count, (time - $start_time)/60;
	
}
else {
	printf " > finished with %s in %.1f minutes\n\n", $Project->id, 
		(time - $start_time)/60;
}


exit;






####### Functions ##################

sub scan_directory {
	
	# load manifest file
	my $manifest_file = $Project->manifest_file;
	if ( -e $manifest_file ) {
		my $csv = Text::CSV->new();
		my $fh  = IO::File->new( $manifest_file ) or
			die " Cannot read manifest file '$manifest_file'! $OS_ERROR";
		my $header = $csv->getline($fh);
		while ( my $data = $csv->getline($fh) ) {
			my %file         = mesh $header, $data;
			my $name         = $file{File};
			$file{ftime}     = str2time( $file{Date} );
# 			printf "   => converted %s to effectively %s\n", $file{Date},
# 				strftime( "%B %d, %Y %H:%M:%S", localtime( $file{ftime} ) );
			$file{status}    = 1;
			$filedata{$name} = \%file;
		}
		$fh->close;
	}
	
	### search directory recursively using File::Find 
	# remember that we are in the project directory, so we search current directory ./
	# results from the recursive search are processed with callback() function and 
	# written to global variables - the callback doesn't support passed data 
	find( {
			follow => 0, # do not follow symlinks
			wanted => \&callback,
		  }, '.'
	);
	
	# confirm
	if (not scalar keys %filedata) {
		print "  > nothing found!\n";
		return;
	}

	# check paired fastq status
	my $paired_fastq = 0;
	foreach my $f (keys %filedata) {
		if ( exists $filedata{$f}{paired_end} and $filedata{$f}{paired_end} eq '2' ) {
			$paired_fastq = 1;
			last;
		}
	}
	
	### Generate manifest file
	my @manifest;
	push @manifest, join(',', qw(File Type Archived Size Date MD5 sample_id paired_end
								platform platform_unit_id));
	my %dupmd5; # for checking for errors
	my $new_file_count      = 0;
	my $existing_file_count = 0;
	my $removed_file_count  = 0;
	
	# walk through collected file list
	foreach my $f (sort {$a cmp $b} keys %filedata) {

		# check file status
		if (not exists $filedata{$f}{status}) {
			print " ! ERROR: no status for $f\n";
		}
		elsif ($filedata{$f}{status} == 1) {
			$removed_file_count++;
			next;
		}
		elsif ($filedata{$f}{status} == 2) {
			$existing_file_count++;
		}
		elsif ($filedata{$f}{status} == 3) {
			$new_file_count++;
			$filedata{$f}{Date} = strftime( "%B %d, %Y %H:%M:%S",
				localtime( $filedata{$f}{ftime} ) );
		}
		
		# first check for md5 checksum
		my $md5 = $filedata{$f}{MD5} || q();
		if (not $md5) {
			# we don't have a checksum for this file yet
			# check in the checksums hash for the file name without the path
			my (undef, undef, $filename) = File::Spec->splitpath($f);
			if (exists $checksums{$filename}) {
				# we have it
				$md5 = $checksums{$filename};
			}
			else {
				# don't have it, need to calculate
				$md5 = $Project->calculate_file_checksum($f);
			}
			$filedata{$f}{MD5} = $md5;
		}
		
		# check for accidental duplicate checksums - this is a real problem
		# we do this for all files, but only if they're greater than 1 MB in size
		# there are lots of little text files, like scripts and such, that are identical
		# by their nature, so we're really only interested in the big ones
		if ($filedata{$f}{Size} > 1048576 ) {
			if ( exists $dupmd5{$md5} ) {
				printf "  ! duplicate md5 checksum $md5 for $f\n";
			}
			else {
				$dupmd5{$md5} += 1;
			}
		}
		
		# fastq-specific values
		my ($pair, $machine, $laneid);
		if ( $paired_fastq and $filedata{$f}{Type} eq 'Fastq' ) {
			if ( exists $filedata{$f}{paired_end} ) {
				$pair = $filedata{$f}{paired_end};
			}
			else {
				# we should have paired status, sigh
				$pair = q(-);
			}
			$machine = $filedata{$f}{platform} || q(-);
			$laneid  = $filedata{$f}{platform_unit_id} || q(-);
		}
		elsif ( $filedata{$f}{Type} eq 'Fastq' ) {
			$pair = '-';
			$machine = $filedata{$f}{platform} || q(-);
			$laneid  = $filedata{$f}{platform_unit_id} || q(-);
		}
		else {
			$pair    = q();
			$machine = q();
			$laneid  = q();
		}
		
		# add to the list
		push @manifest, join(',', 
			qq("$f"),
			$filedata{$f}{Type} || q(),
			$filedata{$f}{Archived} || q(N),
			$filedata{$f}{Size},
			sprintf( qq("%s"), $filedata{$f}{Date} ),
			$md5,
			$filedata{$f}{sample_id},
			$pair,
			$machine,
			$laneid,
		);
		if ( $filedata{$f}{Archived} and $filedata{$f}{Archived} eq 'Y' ) {
			push @ziplist, $f;
		}
	}	
	
	# print summary
	if ($new_file_count) {
		printf "  > Added %d new files to manifest\n", $new_file_count;
	}
	if ($existing_file_count) {
		printf "  > Retained %d existing files to manifest\n", $existing_file_count;
	}
	if ($removed_file_count) {
		printf "  > Removed %d files from manifest\n", $removed_file_count;
	}
	
	### Write files
	# manifest
	my $fh = IO::File->new($Project->manifest_file, 'w') or 
		die sprintf("unable to write file %s: $OS_ERROR\n", $Project->manifest_file);
	foreach (@manifest) {
		$fh->printf("%s\n", $_);
	}
	$fh->close;
	
	# remove list
	$fh = IO::File->new($Project->alt_remove_file, 'w') or 
		die sprintf("unable to write file %s: $OS_ERROR\n", $Project->alt_remove_file);
	foreach (@removelist) {
		$fh->printf("%s\n", $_);
	}
	$fh->close;
	printf "  > wrote %d files to remove list %s\n", scalar(@removelist), 
		$Project->alt_remove_file;

	# zip list
	if ($do_zip) {
		$fh = IO::File->new($Project->alt_ziplist_file, 'w') or 
			die sprintf("unable to write file %s: $OS_ERROR\n",
			$Project->alt_ziplist_file);
		foreach (@ziplist) {
			$fh->printf("%s\n", $_);
		}
		$fh->close;
		printf "  > wrote %d files to zip list %s\n", scalar(@ziplist), 
			$Project->alt_ziplist_file;
	}	

	return 1;
}

sub callback {
	my $file = $_;
	# print "  > find callback on $file for $clean_name\n" if $verbose;

	# generate a clean name for recording
	my $clean_name = $File::Find::name;
	$clean_name =~ s|^\./||; # strip the beginning ./ from the name to clean it up
		
	### Ignore certain files
	if (-d $file) {
		# skip directories
		if ( $file =~ /^\./ and $file ne '.' ) {
			# hidden directory, this cannot be good
			print "   ! hidden directory $clean_name\n";
		}
		print "   > skipping directory $clean_name\n" if $verbose;
		return;
	}
	elsif ($file eq $Project->manifest_file) {
		return;
	}
	elsif ($file eq $Project->notice_file) {
		return;
	}
	elsif ($file eq $Project->ziplist_file) {
		return;
	}
	elsif ($file eq $Project->zip_file) {
		return;
	}
	elsif ($file =~ /libsnappyjava\.so$/xi) {
		# devil java spawn, delete!!!!
		print "   ! deleting java file $clean_name\n";
		unlink $file;
		return;
	}
	elsif ($file =~ m/(?: fdt | fdtCommandLine ) \.jar $/x) {
		# fdt files, don't need
		print "   ! deleting java file $clean_name\n";
		unlink $file;
		return;
	}
	elsif ($file eq '.DS_Store' or $file eq 'Thumbs.db') {
		# Windows and Mac file browser devil spawn, delete these immediately
		print "   ! deleting unnecessary file $clean_name\n" if $verbose;
		unlink $file;
		return;
	}
	elsif ($file eq '.snakemake_timestamp') {
		# Auto Analysis snakemake droppings
		print "   ! deleting unnecessary file $clean_name\n" if $verbose;
		unlink $file;
		return;
	}
	elsif ( $file =~ /~$/ ) {
		# files ending in ~ are typically backup copies of an edited text file
		# these can be safely deleted
		print "   ! deleting backup file $clean_name\n";
		unlink $file;
		return;
	}
	elsif ( $file =~ /^\./ ) {
		# hidden files
		print "   ! deleting hidden file $clean_name\n";
		unlink $file;
		return;
	}
	elsif ( -l $file  ) {
		if ( $request and $clean_name =~ /^ AutoAnalysis_\w+ \/ /x ) {
			# autoanalysis symlinks are temporary and should automatically be cleaned up
			# ignore for now
		}
		else {
			print "   ! marking to delete symbolic link $clean_name\n";
			push @removelist, $file;
		}
		return;
	}
	
	# check manifest hash
	if ( exists $filedata{$clean_name} ) {
		my $old_size = $filedata{$clean_name}{Size};
		my $old_time = $filedata{$clean_name}{ftime};
		my ($cur_time, $cur_size) = get_file_stats($file);

		# check the file date and size
		# this may have issues with standard / daylight savings time conversions
		# tolerate a delta of 3600 seconds or 1 hour
		my $diff = abs( $cur_time - $old_time );
# 		printf "   => difference in time is %s, in size %s\n", $cur_time - $old_time, $cur_size - $old_size;
		if ( $diff == 0 and $cur_size == $old_size ) {
			# files are equivalent
			$filedata{$clean_name}{status} = 2;
			print "   > using pre-existing manifest entry $clean_name\n" if $verbose;
			return;
		}
		elsif ( $diff < 3601 and $cur_size == $old_size ) {
			# file time within one hour tolerance
			$filedata{$clean_name}{status} = 2;
			print "   > using pre-existing manifest entry $clean_name\n" if $verbose;
			return;
		}
		elsif ( $cur_size == $old_size ) {
			print "   ! same size, time delta $diff for file $clean_name\n";
		}
		else {
			print "   > re-processing updated file $clean_name\n" if $verbose;
		}
	}
	
	# continue processing based on project type
	if ($request) {
		if ($clean_name =~ /^ AutoAnalysis_\w+ \/ /x) {
			return analysis_callback($file, $clean_name);
		}
		else {
			return request_callback($file, $clean_name);
		}
	}
	else {
		return analysis_callback($file, $clean_name);
	}
}



# find callback
sub request_callback {
	my ($file, $clean_name) = @_;
	print "  > request callback for $clean_name\n" if $verbose;
	
	# file metadata
	my ($type, $sample, $machineID, $laneID, $pairedID);
	
	# check file
	if ($clean_name =~ m/^ (?: bioanalysis | Sample.?QC | Library.?QC | Sequence.?QC | Cell.Prep.QC ) \/ /x) {
		# these are QC samples in a bioanalysis or Sample of Library QC folder
		# directly under the main project 
		print "   > skipping QC file $clean_name\n" if $verbose;
		return;
	}
	elsif ($clean_name =~ /^ RunFolder/x) {
		# a few external requesters want the entire original RunFolder 
		# these folders typically have over 100K files!!!!
		# do not print even if verbose is turned on
		# print one warning and add to remove list
		if ($runfolder_warning) {
			push @removelist, $clean_name;
			return;
		}
		else {
			print " ! Illumina RunFolder present - skipping contents\n";
			$runfolder_warning = 1;
			push @removelist, $clean_name;
			return;
		}
	}
	elsif ($clean_name =~ /^ upload_staging/x) {
		# somebody directly uploaded files to this directory!
		print "   ! uploaded file $clean_name\n";
		$failure_count++;
		return;
	}
	elsif ($file =~ / samplesheet \. \w+ /xi) {
		# file run sample sheet, can safely ignore
		$type = 'document';
		$sample = q();
		$laneID = q();
		$pairedID = q();
		$machineID = q();
	}
	elsif ($file =~ / \. (?: xlsx | numbers | docx | pdf | csv | html ) $/x) {
		# some stray request spreadsheet file or report
		$type = 'document';
		$sample = q();
		$laneID = q();
		$pairedID = q();
		$machineID = q();
	}
	elsif ($file =~ /\.sh$/) {
		# processing shell script
		$type = 'script';
		$sample = q();
		$laneID = q();
		$pairedID = q();
		$machineID = q();
	}
	elsif ( $file =~ /\.txt$/ and $file !~ /md5/ ) {
		# additional stray files but not md5 files!
		$type = 'document';
		$sample = q();
		$laneID = q();
		$pairedID = q();
		$machineID = q();
	}
	elsif ($file =~ /\.zip$/) {
		# some Zip file - caution
		$type = 'zip';
		$sample = q();
		$laneID = q();
		$pairedID = q();
		$machineID = q();
	}
	### Possible Fastq file types
	# 15945X8_190320_M05774_0049_MS7833695-50V2_S1_L001_R2_001.fastq.gz
	# new style: 16013X1_190529_D00550_0563_BCDLULANXX_S12_L001_R1_001.fastq.gz
	# NovoaSeqX: 21185X1_20230810_LH00227_0005_A227HG7LT3_S42_L004_R1_001.fastq.gz
	elsif ($file =~ m/^ (\d{4,5} [xXP] \d+ ) _\d+ _( [LHADM]{1,2}\d+ ) _\d+ _[A-Z\d\-]+ _S\d+ _L(\d+) _R(\d) _001 \. fastq \.gz$/x) {
		$type = 'Fastq';
		$sample = $1;
		$machineID = $2;
		$laneID = $3;
		$pairedID = $4;
		if ($pairedID == 2) {
			my $file2 = $file;
			$file2 =~ s/_R2_/_R3_/;
			if ( -e $file2 ) {
				# this is likely a UMI fastq file, check sizes
				my $size1 = (stat $file)[7];
				my $size2 = (stat $file2)[7];
				if ( $size2 > ($size1 * 3) ) {
					# yes, most likely
					$pairedID = 'UMI';
				}
			}
		}
		elsif ($pairedID == 3) {
			# most likely the 2nd read
			my $file2 = $file;
			$file2 =~ s/_R3_/_R2_/;
			if ( -e $file2 ) {
				# this is likely a UMI fastq file, check sizes
				my $size1 = (stat $file)[7];
				my $size2 = (stat $file2)[7];
				if ( $size1 > ($size2 * 3) ) {
					# yes, most likely
					$pairedID = 2;
				}
			}
		}
	}
	# new style index: 15603X1_181116_A00421_0025_AHFM7FDSXX_S4_L004_I1_001.fastq.gz
	elsif ($file =~ m/^ (\d{4,5} [xX] \d+) _\d+ _( [LHADM]{1,2}\d+ ) _\d+ _[A-Z\d\-]+ _S\d+ _L(\d+) _I(\d) _001 \. fastq \.gz$/x) {
		$type = 'Fastq';
		$sample = $1;
		$machineID = $2;
		$laneID = $3;
		$pairedID = "index$4";
	}
	# new old style HiSeq: 15079X10_180427_D00294_0392_BCCEA1ANXX_R1.fastq.gz
	elsif ($file =~ m/^ ( \d{4,5} [xX] \d+ ) _\d+ _( [ADM]\d+ ) _\d+ _[A-Z\d\-]+ _R(\d) \. (?: txt | fastq ) \.gz$/x){
		$type = 'Fastq';
		$sample = $1;
		$machineID = $2;
		$laneID = 1;
		$pairedID = $3;
	}
	# old style, single-end: 15455X2_180920_D00294_0408_ACCFVWANXX_2.txt.gz
	elsif ($file =~ m/^ ( \d{4,5} [xX] \d+ ) _\d+ _( [ADM] \d+ ) _\d+ _[A-Z\d]+ _(\d) \.txt \.gz $/x) {
		$type = 'Fastq';
		$sample = $1;
		$machineID = $2;
		$laneID = $3;
	}
	# old style, paired-end: 15066X1_180427_D00294_0392_BCCEA1ANXX_5_1.txt.gz
	elsif ($file =~ m/^ ( \d{4,5} [xX] \d+ ) _\d+ _( [ADM] \d+ ) _\d+ _[A-Z\d]+ _(\d) _[12] \.txt \.gz$/x) {
		$type = 'Fastq';
		$sample = $1;
		$machineID = $2;
		$laneID = $3;
		$pairedID = $4;
	}
	# 10X genomics and MiSeq read file: 15454X1_S2_L001_R1_001.fastq.gz, sometimes not gz????
	elsif ($file =~ m/^ ( \d{4,5} [xX] \d+ ) _S\d+ _L(\d+) _R(\d) _001 \.fastq (?:\.gz)? $/x) {
		$type = 'Fastq';
		$sample = $1;
		$laneID = $2;
		$pairedID = $3;
		# must grab the machine ID from the read name
		my $head = $file =~ m/\.gz$/ ? qx(gzip -dc $file | head -n 1) : qx(head -n 1 $file);
		if ($head =~ /^ @ ( [ADM]\d+ ) :/x) {
			$machineID = $1;
		}
	}
	# 10X genomics index file: 15454X1_S2_L001_I1_001.fastq.gz
	elsif ($file =~ m/^ ( \d{4,5} [xX] \d+ ) _S\d+ _L(\d+) _I(\d) _001 \.fastq \.gz $/x) {
		$type = 'Fastq';
		$sample = $1;
		$laneID = $2;
		$pairedID = "index$3";
		# must grab the machine ID from the read name
		my $head = qx(gzip -dc $file | head -n 1);
		if ($head =~ /^ @ ( [ADM]\d+ ) :/x) {
			$machineID = $1;
		}
	}
	# another MiSeq file: 15092X7_180424_M00736_0255_MS6563328-300V2_R1.fastq.gz
	elsif ($file =~ m/^ ( \d{4,5} [xX] \d+ ) _\d+ _( [ADM] \d+ ) _\d+ _[A-Z\d\-]+ _R(\d) \.fastq \.gz $/x) {
		$type = 'Fastq';
		$sample = $1;
		$machineID = $2;
		$pairedID = $3;
		$laneID = 1;
	}
	# crazy name: GUPTA_S1_L001_R1_001.fastq.gz
	elsif ($file =~ m/^ ( \w+ ) _S\d+ _L(\d+) _R(\d )_00\d \.fastq \.gz $/x) {
		$type = 'Fastq';
		$sample = $1;
		$laneID = $2;
		$pairedID = $3;
		# must grab the machine ID from the read name
		my $head = qx(gzip -dc $file | head -n 1);
		if ($head =~ /^ @ ( [ADM]\d+ ) :/x) {
			$machineID = $1;
		}
	}
	# crazy index: GUPTA_S1_L001_I1_001.fastq.gz
	elsif ($file =~ m/^ ( \w+ ) _S\d+ _L(\d+) _I(\d) _00\d \.fastq \.gz $/x) {
		$type = 'Fastq';
		$sample = $1;
		$laneID = $2;
		$pairedID = "index$3";
		# must grab the machine ID from the read name
		my $head = qx(gzip -dc $file | head -n 1);
		if ($head =~ /^ @ ( [ADM]\d+ ) :/x) {
			$machineID = $1;
		}
	}
	# really old single-end file: 9428X9_120926_SN1117_0117_AC168KACXX_8.txt.gz
	elsif ($file =~ m/^ ( \d{4,5} [xX] \d+ ) _\d+ _SN\d+ _\d+ _[A-Z\d]+ _(\d) \.txt \.gz $/x) {
		$type = 'Fastq';
		$sample = $1;
		$laneID = $2;
		$pairedID = 1;
		# must grab the machine ID from the read name
		my $head = qx(gzip -dc $file | head -n 1);
		if ($head =~ /^ @ ( [ADM]\d+ ) :/x) {
			$machineID = $1;
		}
	}
	# undetermined file: Undetermined_S0_L001_R1_001.fastq.gz
	elsif ($file =~ m/^ Undetermined _.+ \.fastq \.gz $/x) {
		$type = 'Fastq';
		$sample = 'undetermined';
		if ($file =~ m/_L(\d+)/) {
			$laneID = $1;
		}
		if ($file =~ m/_R([12])/) {
			$pairedID = $1;
		}
		# must grab the machine ID from the read name
		my $head = qx(gzip -dc $file | head -n 1);
		if ($head =~ /^ @ ( [ADM]\d+ ) :/x) {
			$machineID = $1;
		}
	}
	# I give up! catchall for other weirdo fastq files!!!
	elsif ($file =~ m/ .+ \. (?: fastq | fq ) \.gz $/xi) {
		$type = 'Fastq';
		# I can't extract metadata information
		# but at least it will get recorded in the manifest and list files
		print "   ! processing unrecognized Fastq file $clean_name\n";
		$sample = q();
		$laneID = q();
		$pairedID = q();
		$machineID = q();
	}
	# single checksum file
	elsif ($file =~ m/\.gz\.md5$/) {
		my $fh = IO::File->new($file);
		my $line = $fh->getline;
		my ($m, undef) = split(/\s+/, $line, 2);
		$fh->close;
		push @removelist, $clean_name;
		$clean_name =~ s/\.md5$//;
		$checksums{$clean_name} = $m;
		print "   > processed md5 file for $clean_name\n" if $verbose;
		return; # do not continue
	}
	# multiple checksum file - with or without datetime stamp in front - ugh
	elsif ($file =~ 
m/^ (?: \d{4} \. \d\d \. \d\d _ \d\d \. \d\d \. \d\d \. )? md5 (?: sum)? [\._] .* \. (?: txt | out ) $/x
	) {
		my $fh = IO::File->new($file);
		while (my $line = $fh->getline) {
			my ($md5, $fastqpath) = split(/\s+/, $line);
			# unfortunately, this may or may not be the current given file path
			# so we must go solely on the filename, with the assumption that there 
			# are not additional files with the same name!!!!
			my (undef, undef, $fastqname) = File::Spec->splitpath($fastqpath);
			$checksums{$fastqname} = $md5;
		}
		$fh->close;
		print "   > processed md5 file $clean_name\n" if $verbose;
		push @removelist, $clean_name;
		return; # do not continue
	}
	elsif ($clean_name =~ /^ Fastq \/ .+ \. (?: xml | csv ) $/x) {
		# other left over files from de-multiplexing
		print "   ! leftover demultiplexing file $clean_name\n";
		$type = 'document';
		$sample = q();
		$laneID = q();
		$pairedID = q();
		$machineID = q();
	}
	else {
		# programmer error!
		print "   ! unrecognized file $clean_name\n";
		$failure_count++;
		return;
	}
	
	# stats on the file
	my ($date, $size) = get_file_stats($file);
	
	# clean up sample identifier as necessary
	$sample =~ s/P/X/;
	
	### Record the collected file information
	$filedata{$clean_name}{Type}             = $type;
	$filedata{$clean_name}{ftime}            = $date;
	$filedata{$clean_name}{Size}             = $size;
	$filedata{$clean_name}{Archive}          = 'N';
	$filedata{$clean_name}{status}           = 3;
	if ($type eq 'Fastq') {
		$filedata{$clean_name}{sample_id}        = $sample || q(-);
		$filedata{$clean_name}{platform}         = $machinelookup{$machineID} || q(-);
		$filedata{$clean_name}{platform_unit_id} = $laneID || q(-);
		$filedata{$clean_name}{paired_end}       = $pairedID || q(-);
	}
	push @removelist, $clean_name;  # by default we remove all request files  

	print "   > processed $type file $clean_name\n" if $verbose;
}


sub analysis_callback {
	my ($file, $clean_name) = @_;
	print "  > analysis callback for $clean_name\n" if $verbose;

	# special file types to delete
	if ($file =~ /\.sra$/i) {
		# what the hell are SRA files doing in here!!!????
		print "   ! marking to delete SRA file $clean_name\n";
		push @removelist, $clean_name;
		return;
	}
	elsif ( $clean_name =~ /.+ \/ fork\d \/ .+/x ) {
		# these are left over 10X Genomics temporary processing files
		# they are not needed and do not need to be saved
		# add to custom remove list
		print "     marking 10X Genomics temporary file for deletion\n" if $verbose;
		push @removelist, $clean_name;
		push @ten_x_crap, $clean_name;
		return;
	}
	elsif ( $clean_name =~ /_STARtmp\// ) {
		# left over STAR alignment droppings, delete
		push @removelist, $clean_name;
		return;
	}
	elsif ( $clean_name =~ m/ \w+ \.stat \/ \w+ \. (?: cnt | model | theta ) $/x) {
		# STAR alignment statistic files, not worth keeping as they're uninterpretable
		push @removelist, $clean_name;
		return;
	}
	
	### metadata and stats on the file
	my ($filetype, $zip);
	my ($date, $size) = get_file_stats($file);
	
	### Possible file types
	# assign general file category type based on the extension
	# this should catch most files, but there's always weirdos and miscellaneous files
	# this will also dictate zip file status
	
	if ($file =~ /\. (?: bw | bigwig | bb | bigbed | hic) $/xi) {
		# an indexed analysis file
		$filetype = 'BrowserTrack';
		$zip = 0;
	}
	elsif ($file =~ /\. (?: bam | cram | sam\.gz ) $/xi) {
		# an alignment file
		$filetype = 'Alignment';
		$zip = 0;
	}
	elsif ( $file =~ /\. (?: bai | csi | crai | tbi ) $/xi) {
		# index files
		# these are arguably not necessary and can be regenerated relatively easily
		push @removelist, $clean_name;
		return;
	}
	elsif ($file =~ /\.sam$/i) {
		# an uncompressed alignment - why do these exist??
		$filetype = 'Alignment';
		if ($size > 1048576) {
			# file bigger than 1 MB, let's compress it separately
			my $command = sprintf "%s \"%s\"", $gzipper, $file;
			if (system($command)) {
				print "   ! failed to automatically compress '$clean_name': $OS_ERROR\n";
				$zip = 1; 
			}
			else {
				# succesfull compression! update values
				print "   > automatically gzip compressed $file\n";
				$file  .= '.gz';
				$clean_name .= '.gz';
				$zip = 0;
				($date, $size) = get_file_stats($file);
			}
		}
		else {
			# otherwise we'll leave it for inclusion in the zip archive
			$zip = 0;
		}
	}
	elsif ($file =~ /\.vcf.gz$/i) {
		# compressed variant file
		$filetype = 'Variant';
		$zip = 0;
	}
	elsif ($file =~ /\. (?: vcf | maf ) $/xi) {
		# uncompressed variant file, either VCF or MAF
		if ($size > 1048576) {
			# file bigger than 1 MB, let's compress it separately
			my $command = sprintf "%s \"%s\"", $bgzipper, $file;
			if (system($command)) {
				print "   ! failed to automatically compress '$clean_name': $OS_ERROR\n";
				$zip = 1; 
			}
			else {
				# succesfull compression! update values
				print "   > automatically compressed $file\n";
				$file  .= '.gz';
				$clean_name .= '.gz';
				$zip = 0;
				($date, $size) = get_file_stats($file);
			}
		}
		else {
			# otherwise we'll leave it for inclusion in the zip archive
			$zip = 1;
		}
		$filetype = 'Variant';
	}
	elsif ($file =~ /\. [cv] loupe$/xi) {
		# 10X genomics loupe file
		$filetype = 'Analysis';
		$zip = 0;
	}
	elsif ($file =~ /\. (?: fq | fastq ) (?: \.gz)? $/xi) {
		# fastq file
		if ($file =~ /^ \d{4,6} X \d{1,3} _/x) {
			print "   ! Possible HCI Fastq file detected! $clean_name\n";
		}
		$filetype = 'Fastq';
		if ($file !~ /\.gz$/i) {
			# file not compressed!!!????? let's compress it separately
			my $command = sprintf "%s \"%s\"", $gzipper, $file;
			if (system($command)) {
				print "   ! failed to automatically compress '$clean_name': $OS_ERROR\n";
				$zip = 1; 
			}
			else {
				# succesfull compression! update values
				print "   > automatically gzip compressed $clean_name\n";
				$file  .= '.gz';
				$clean_name .= '.gz';
				($date, $size) = get_file_stats($file);
			}
		}
		
		# check the size
		if ($size > 1048576) {
			# Bigger than 1 MB, leave it out
			$zip = 0;
		}
		else {
			# otherwise we'll include it in the zip archive
			$zip = 1;
		}
		
		# set fastq specific data, look for paired state
		my $paired = q(-);
		if ( $file =~ /r? ([12]) \./xi ) {
			$paired = $1;
		}
		$filedata{$clean_name}{paired_end}       = $paired;
		$filedata{$clean_name}{platform_unit_id} = q(-);
		$filedata{$clean_name}{platform}         = q(-);
		$filedata{$clean_name}{sample_id}        = q(-);
	}
	elsif ($file =~ /^Unmapped \.out \.mate ([12]) .*$/xi) {
		# unmapped fastq from STAR - seriously, does anyone clean up their droppings?
		$filetype = 'Fastq';
		my $paired = $1;
		if ($file !~ /\.gz$/i) {
			# file not compressed!!!????? let's compress it
			my $command = sprintf "%s \"%s\"", $gzipper, $file;
			if (system($command)) {
				print "   ! failed to automatically compress '$clean_name': $OS_ERROR\n";
				$zip = 1; 
			}
			else {
				# succesfull compression! update values
				print "   > automatically gzip compressed $clean_name\n";
				$file  .= '.gz';
				$clean_name .= '.gz';
				($date, $size) = get_file_stats($file);
			}
		}
		
		# check the size
		if ($size > 1048576) {
			# Bigger than 1 MB, leave it out
			$zip = 0;
		}
		else {
			# otherwise we'll include it in the zip archive
			$zip = 1;
		}
		
		# set fastq specific data
		$filedata{$clean_name}{paired_end}       = $paired;
		$filedata{$clean_name}{platform_unit_id} = q(-);
		$filedata{$clean_name}{platform}         = q(-);
		$filedata{$clean_name}{sample_id}        = q(-);
	}
	elsif ($file =~ /\. (?: fa | fasta | fai | ffn | dict ) (?: \.gz )? $/xi) {
		# sequence file of some sort
		$filetype = 'Sequence';
		if ($file !~ /\.gz$/i and $size > 1048576 ) {
			# file not compressed!!!????? let's compress it
			my $command = sprintf "%s \"%s\"", $gzipper, $file;
			if (system($command)) {
				print "   ! failed to automatically compress '$clean_name': $OS_ERROR\n";
				$zip = 1; 
			}
			else {
				# succesfull compression! update values
				print "   > automatically gzip compressed $clean_name\n";
				$file  .= '.gz';
				$clean_name .= '.gz';
				($date, $size) = get_file_stats($file);
			}
		}
		
		# check the size
		if ($size > 1048576) {
			# Bigger than 1 MB, leave it out
			$zip = 0;
		}
		else {
			# otherwise we'll include it in the zip archive
			$zip = 1;
		}
	}
	elsif ($file =~ /\. (?: bed | bed\d+ | gtf | gff | gff\d | narrowpeak | broadpeak | gappedpeak | refflat | genepred | ucsc) (?:\.gz)? $/xi) {
		$filetype = 'Annotation';
		if ($file =~ /\.gz$/ and $size > 10485760) {
			# do not archive if compressed and bigger 10 MB
			$zip = 0;
		}
		else {
			$zip = 1;
		}
	}
	elsif ($file =~ /\. (?: sh | pl | py | pyc | r | rmd | rscript | awk | sm | sing ) $/xi) {
		$filetype = 'Script';
		$zip = 1;
	}
	elsif ($file eq 'cmd.txt') {
		# pysano command script
		$filetype = 'Script';
		$zip = 1;
	}
	elsif ($file =~ /\. (?: txt | tsv | tab | csv | cdt | counts | results | cns | cnr | cnn | md | log | biotypes | summary | rna_metrics | out | err | idxstats? ) (?:\.gz)? $/xi) {
		# general analysis text files, may be compressed
		$filetype = 'Text';
		$zip = 1;
	}
	elsif ($file =~ /\. (?: wig | bg | bdg | bedgraph ) (?:\.gz)? $/xi) {
		$filetype = 'Analysis';
		$zip = 1;
	}
	elsif ($file =~ /\. mpileup.* \.gz $/xi) {
		# compressed mpileup files ok?, some people stick in text between mpileup and gz
		$filetype = 'Analysis';
		$zip = 0;
	}
	elsif ($file =~ /\. (?: bar | bar\.zip | useq | swi | swi\.gz | egr | ser | mpileup | motif | cov | mtx | mtx\.gz ) $/xi) {
		$filetype = 'Analysis';
		$zip = 1;
	}
	elsif ($file =~ /\. (?: xls | ppt | pptx | doc | docx | rout | rda | rdata | rds | rproj | xml | json | json\.gz | html | pzfx ) $/xi) {
		$filetype = 'Results';
		$zip = 1;
	}
	elsif ($file =~ /\. (?: pdf | ps | eps | png | jpg | jpeg | gif | tif | tiff | svg | ai ) $/xi) {
		$filetype = 'Image';
		$zip = 1;
	}
	elsif ($file =~ /\. (?: xlsx | h5 | hd5 | hdf5 ) $/xi) {
		# leave out certain result files from zip archive just to be nice
		$filetype = 'Results';
		$zip = 0;
	}
	elsif ($file =~ /\. (?: tar |tar\.gz | tar\.bz2 | zip ) $/xi) {
		$filetype = 'Archive';
		if ($file =~ /fastqc \.zip $/x) {
			# no need keeping fastqc zip files separate
			$zip = 1; 
		}
		elsif ( $size < 10485760 ) {
			# go ahead and store the zip if it is under 10 MB
			# includes many AutoAnalysis LogsRunScripts.zip files
			$zip = 1;
		}
		else {
			# something else
			$zip = 0; 
		}
	}
	elsif ($file =~ /\. (?: bt2 | amb | ann | bwt | pac | nix | novoindex | index ) $/x) {
		$filetype = 'AlignmentIndex';
		$zip = 1; # zip I guess?
	}
	elsif ($file =~ / \.vcf \.idx $/xi) {
		# a GATK-style index for non-tabix, uncompressed VCF files
		# This index isn't useful for browsing or for anything other than GATK
		# and it will auto-recreate anyway, so toss
		push @removelist, $clean_name;
		return;
	}
	else {
		# catchall
		$filetype = 'Other';
		if ($size > $max_zip_size) {
			# it's pretty big
			printf "   ! Large unknown file $clean_name at %.1fG\n", $size / 1073741824;
		}
		$zip = 1;
	}
	
	
	# Check files for Zip archive files
	if (not $do_zip) {
		# we're not zipping anything so turn it off
		$zip = 0; 
	}
	
	### Record the collected file information
	$filedata{$clean_name}{Type}     = $filetype;
	$filedata{$clean_name}{MD5}      = $Project->calculate_file_checksum($file);
	$filedata{$clean_name}{ftime}    = $date;
	$filedata{$clean_name}{Size}     = $size;
	$filedata{$clean_name}{Archived} = $zip ? 'Y' : 'N';
	$filedata{$clean_name}{status}   = 3;
	
	# Check for sample ID
	if ( $clean_name =~ / (\d{4,6}X\d{1,3}) [\.\-_\/] /x ) {
		$filedata{$clean_name}{sample_id} = $1;
	}
	else {
		$filedata{$clean_name}{sample_id} = q();
	}
	
	# removed everything except for any browser tracks
	if (not $zip and $filetype ne 'BrowserTrack') {
		push @removelist, $clean_name;
	}

	print "     processed $filetype file\n" if $verbose;
}


sub get_file_stats {
	# stats on the file
	my $file = shift;
	my @st = stat($file);
	# my $date = strftime("%B %d, %Y %H:%M:%S", localtime($st[9]));
	return ($st[9], $st[7]); # date, size
}



__END__

=head1 AUTHOR

 Timothy J. Parnell, PhD
 Cancer Bioinformatics Shared Resource
 Huntsman Cancer Institute
 University of Utah
 Salt Lake City, UT, 84112

This package is free software; you can redistribute it and/or modify
it under the terms of the Artistic License 2.0.  





