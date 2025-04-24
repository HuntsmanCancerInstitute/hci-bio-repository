#!/usr/bin/env perl

use warnings;
use strict;
use English qw(-no_match_vars);
use IO::File;
use File::Find;
use File::Spec;
use File::Copy qw(move);
use POSIX qw(strftime);
use Date::Parse;
use Text::CSV;
use List::Util qw(mesh);
use Getopt::Long;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use RepoCatalog;
use RepoProject;

our $VERSION = 7.7;



######## Documentation
my $doc = <<END;

A general script to process and inventory GNomEx project folders, both
Analysis and Experiment Request types.

It will recursively scan a project folder and generate a MANIFEST.csv
file of the contents. Path, file size, file date, and MD5 checksum is
included in the manifest. Fastq metadata, when available, are also 
included.
 
An ARCHIVE_LIST.txt and REMOVE_LIST.txt files will also be written to
the parent directory, if or when appropriate. 

This is intended to be run on GNomEx project folders only, although
it can be run on custom directories if given a path.

Version: $VERSION

Example usage:
    
    process_project.pl --cat Catalog.db --id 1234R
    
Required: 
    -c --cat <path>        Provide path to metadata catalog database
    -p --project <text>    GNomEx ID to the project

Options:
    --zip --nozip          Do or do not generate zip archive list file
    --delete --nodelete    Do or do not generate remove list file
    --verbose              Tell me everything!

Manual inventory of a custom directory:
    --path <path>          Scan the given custom directory. Best if 
                             provided a full path. Do not set --catalog
                             and --project. It will be treated as an
                             Analysis project.
                             
 
END



######## Process command line options
my $cat_file;
my $id;
my $path;
my $do_zip;
my $do_del;
my $verbose;

if (scalar(@ARGV) > 1) {
	GetOptions(
		'c|catalog=s'   => \$cat_file,
		'p|project=s'   => \$id,
		'path=s'        => \$path,
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
my $request;
my @removelist;
my @ziplist;
my %checksums;
my $failure_count     = 0;
my $runfolder_warning = 0; # Gigantic Illumina RunFolder present
my $autoanal_warning  = 0; # warnings about AutoAnalysis folders
my $upload_warning    = 0; # GNomEx upload folder
my $post_zip_size     = 0;
my $max_zip_size      = 200000000; # 200 MB
my $start_time        = time;

# our sequence machine IDs to platform technology lookup
my %machinelookup = (
	'D00550'     => 'Illumina HiSeq 2500',
	'D00294'     => 'Illumina HiSeq 2500',
	'A00421'     => 'Illumina NovaSeq 6000',
	'M05774'     => 'Illumina MiSeq',
	'M00736'     => 'Illumina MiSeq',
	'DQNZZQ1'    => 'Illumina HiSeq 2000',
	'HWI-ST1117' => 'Illumina HiSeq 2000',
	'LH00227'    => 'Illumina NovaSeq X',
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
		$cat_file = File::Spec->rel2abs($cat_file);
	}
	
	# find entry in catalog and collect information
	my $Catalog = RepoCatalog->new($cat_file) or 
		die "Cannot open catalog file '$cat_file'!\n";
	my $Entry = $Catalog->entry($id) or 
			die "No Catalog entry for $id\n";
	$path = $Entry->path;
	$request = $Entry->is_request;
	
	# set default options
	unless (defined $do_zip) {
		if ($Entry->core_lab) {
			$do_zip = 1;
		}
		else {
			$do_zip = 0;
		}
	}
	unless (defined $do_del) {
		$do_del = 1;
	}
}
elsif ($path) {
	# we have a non-canonical non-repository directory
	$request = 0;
	unless (defined $do_zip) {
		$do_zip = 0;
	}
	unless (defined $do_del) {
		$do_del = 0;
	}
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

printf " > Working on %s at %s\n", $Project->id, $Project->given_dir;
printf "   Using parent directory %s\n", $Project->parent_dir if $verbose;



# file paths
if ($verbose) {
	printf " =>  manifest file: %s\n", $Project->manifest_file;
	printf " =>    remove file: %s or %s\n", $Project->remove_file, 
		$Project->alt_remove_file;
	printf " =>  zip list file: %s or %s\n", $Project->zip_file, $Project->alt_zip_file;
	
}


# check for removed file hidden folder
if (-e $Project->delete_folder) {
	print "! Cannot re-scan if deleted files hidden folder exists!\n";
	exit 1;
}

# check for zipped file hidden folder
if ( -e $Project->zip_folder or -e $Project->zip_file ) {
	print " ! Cannot re-scan if zipped file or hidden folder exists!\n";
	exit 1;
}





######## Main functions

# change to the given directory
printf " > Changing to %s\n", $Project->given_dir if $verbose;
chdir $Project->given_dir or die sprintf("cannot change to %s!\n", $Project->given_dir);

# check for files that shouldn't be here
if ( -e $Project->zip_file ) {
	printf " ! Cannot scan because Archive Zip file %s exists\n", $Project->zip_file;
	exit 1;
}
if ( -e $Project->ziplist_file ) {
	printf " ! Cannot scan because Archive Zip list file %s exists\n",
		$Project->ziplist_file;
	exit 1;
}
if ( -e $Project->remove_file ) {
	printf " ! Cannot scan because Remove list file %s exists\n", $Project->remove_file;
	exit 1;
}

# scan the directory
printf " > Scanning...\n";
scan_directory();

if ($failure_count) {
	printf " ! Finished with %s with %d failures in %.1f minutes\n\n", $Project->id,
		$failure_count, (time - $start_time)/60;
	exit 1;
}
else {
	# update catalog time stamp
	if ( $cat_file and -e $cat_file ) {
		my $Catalog = RepoCatalog->new($cat_file);
		if ( $Catalog ) {
			my $Entry = $Catalog->entry($Project->id);
			if ($Entry) {
				$Entry->scan_datestamp(time);
				print " > Updated Catalog scan date stamp\n";
			}
		}
	}
	printf " > Finished with %s in %.1f minutes\n\n", $Project->id, 
		(time - $start_time)/60;
	exit 0;
}









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
			$file{status}    = 1;  # default status is to remove
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
		if (-e $Project->manifest_file) {
			unlink $Project->manifest_file;
		}
		if (-e $Project->alt_remove_file) {
			unlink $Project->alt_remove_file;
		}
		if (-e $Project->alt_ziplist_file) {
			unlink $Project->alt_ziplist_file;
		}
		return;
	}

	# check paired fastq status
	my $paired_fastq = 0;
	foreach my $f (keys %filedata) {
		if ( exists $filedata{$f}{paired_end} and 
			( $filedata{$f}{paired_end} eq '2' or
			$filedata{$f}{paired_end} eq 'interleaved' )
		 ) {
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
			$failure_count++;
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
			# this happens with Fastq files because it's usually already calculated
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
		if ( $paired_fastq and lc $filedata{$f}{Type} eq 'fastq' ) {
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
		elsif ( lc $filedata{$f}{Type} eq 'fastq' ) {
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
			sprintf( qq("%s"), $machine),
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
	if (scalar @manifest) {
		my $fh = IO::File->new($Project->manifest_file, 'w') or 
			die sprintf("unable to write file %s: $OS_ERROR\n", $Project->manifest_file);
		foreach (@manifest) {
			$fh->printf("%s\n", $_);
		}
		$fh->close;
	}
	elsif (-e $Project->manifest_file) {
		unlink $Project->manifest_file;
	}
	
	# remove list
	if ( $do_del and scalar @removelist) {
		my $alt_file = $Project->alt_remove_file;
		if ($alt_file) {
			my $fh = IO::File->new($alt_file, 'w') or 
				die sprintf("unable to write file %s: $OS_ERROR\n",
				$alt_file);
			foreach (@removelist) {
				$fh->printf("%s\n", $_);
			}
			$fh->close;
			printf "  > wrote %d files to remove list %s\n", scalar(@removelist), 
				$alt_file;
		}
	}
	elsif (-e $Project->alt_remove_file) {
		unlink $Project->alt_remove_file;
	}

	# zip list
	if ( $do_zip and scalar(@ziplist) ) {
		my $zip_file = $Project->alt_ziplist_file;
		if ($zip_file) {
			my $fh = IO::File->new($zip_file, 'w') or 
				die sprintf("unable to write file %s: $OS_ERROR\n",
				$zip_file);
			foreach (@ziplist) {
				$fh->printf("%s\n", $_);
			}
			$fh->close;
			printf "  > wrote %d files to zip list %s\n", scalar(@ziplist), 
				$zip_file;
		}
	}
	elsif (-e $Project->alt_ziplist_file) {
		unlink $Project->alt_ziplist_file;
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
			unless ( $file eq '.snakemake' or $file eq '.GQueryIndex' ) {
				# we specifically handle snakemake and GQuery droppings below
				# otherwise warn the user
				print "   ! hidden directory $clean_name\n";
			}
		}
		if ( $file eq 'upload_staging' or $file eq 'upload_in_progress' ) {
			unless ($upload_warning) {
				print "   ! $file directory exists\n";
				$upload_warning = 1;
			}
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
	elsif ($file eq 'ora_decompression_README.txt') {
		push @removelist, $clean_name;
		return;
	}
	elsif ($file =~ /libsnappyjava\.so$/xi) {
		# devil java spawn, delete!!!!
		print "   ! deleting java file $clean_name\n";
		unless (unlink $file) {
			push @removelist, $clean_name;
		}
		return;
	}
	elsif ($file =~ m/( fdt | fdtCommandLine ) \.jar $/xn) {
		# fdt files, don't need
		print "   ! deleting java file $clean_name\n";
		unless (unlink $file) {
			push @removelist, $clean_name;
		}
		return;
	}
	elsif ($file eq '.DS_Store' or $file eq 'Thumbs.db') {
		# Windows and Mac file browser devil spawn, delete these immediately
		print "   ! deleting unnecessary file $clean_name\n" if $verbose;
		unless (unlink $file) {
			push @removelist, $clean_name;
		}
		return;
	}
	elsif ($file eq '.snakemake_timestamp') {
		# Auto Analysis snakemake droppings
		print "   ! deleting unnecessary file $clean_name\n" if $verbose;
		unless (unlink $file) {
			push @removelist, $clean_name;
		}
		return;
	}
	elsif ( $file =~ /~$/ ) {
		# files ending in ~ are typically backup copies of an edited text file
		# these can be safely deleted
		print "   ! deleting backup file $clean_name\n";
		unless (unlink $file) {
			push @removelist, $clean_name;
		}
		return;
	}
	elsif ( $file =~ /^\./ ) {
		# hidden files
		print "   ! deleting hidden file $clean_name\n";
		unless (unlink $file) {
			push @removelist, $clean_name;
		}
		return;
	}
	elsif ( -l $file  ) {
		if ( $request and $clean_name =~ /^ AutoAnalysis_\w+ \/ /x ) {
			# autoanalysis symlinks are temporary and should automatically be cleaned up
			# ignore for now
		}
		else {
			print "   ! marking to delete symbolic link $clean_name\n";
			push @removelist, $clean_name;
		}
		return;
	}
	elsif ( $file eq 'RUNME' and $clean_name =~ /^ AutoAnalysis_\w+ \/ /x ) {
		# temporary AutoAnalysis run script
		# ignore for now, it should be cleaned up automatically
		return;
	}
	elsif ( $file eq 'COMPLETE' and $clean_name =~ /^ AutoAnalysis_\w+ \/ /x ) {
		# an AutoAnalysis control file
		push @removelist, $clean_name;
		return;
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
	if ($clean_name =~ 
m/^ ( Sample.?QC | Library.?QC | Sequence.?QC | Cell.Prep.QC | MolecDiag.QC ) \/ /xn
	) {
		# these are QC samples in a bioanalysis or Sample of Library QC folder
		# directly under the main project 
		print "   > skipping QC file $clean_name\n" if $verbose;
		return;
	}
	elsif ($clean_name =~ /^ \w+ _AutoAnalysis _ \w+/x) {
		# usually an unwanted or depreciated AutoAnalysis
		# david puts 'depreciated' or 'DontUse' or some other prefix
		# print a warning, but only once
		# the warning only works for the first weird AutoAnalysis folder, subsequent ones
		# do not get another warning, but hopefully that's ok??
		if ($autoanal_warning) {
			push @removelist, $clean_name;
			return;
		}
		elsif ($clean_name =~ /^ ( \w+ _ AutoAnalysis  _ \w+ ) /x) {
			print " ! marking contents in $1 for deletion\n";
			$autoanal_warning = 1;
			push @removelist, $clean_name;
			return;
		}
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
	elsif ( $clean_name eq 'Fastq/log.out' ) {
		# left over file droppings, usually rsync output, that certain people like to 
		# leave behind without cleaning up after themselves - how rude
		push @removelist, $clean_name;
		return;
	}
	elsif ($file =~ / samplesheet \. \w+ /xi) {
		# file run sample sheet, can safely ignore
		$type = 'document';
	}
	elsif ( $file =~ / \. ( xlsx | numbers | docx | pdf | csv | html ) $/xn ) {
		# some stray request spreadsheet file or report
		$type = 'document';
	}
	elsif ($file =~ /\.sh$/) {
		# processing shell script
		$type = 'script';
	}
	elsif ( $file =~ /\.txt$/ and $file !~ /md5/ ) {
		# additional stray files but not md5 files!
		$type = 'document';
	}
	elsif ($file =~ /\.zip$/) {
		# some Zip file - caution
		$type = 'zip';
	}
	# ora reference compressed interleaved fastq file
	elsif ($file =~ m/^ (\d{4,5} [xXPG] \d+ ) _\d+ _( [LHADM]{1,2}\d+ ) _\d+ _[A-Z\d\-]+ _S\d+ _L(\d+) _R\-interleaved_001 \.fastq\.ora $/x) {
		$type   = 'Fastq';
		$sample = $1;
		$machineID = $2;
		$laneID = $3;
		$pairedID = q(interleaved);
	}
	### Possible Fastq file types
	# 15945X8_190320_M05774_0049_MS7833695-50V2_S1_L001_R2_001.fastq.gz
	# new style: 16013X1_190529_D00550_0563_BCDLULANXX_S12_L001_R1_001.fastq.gz
	# NovoaSeqX: 21185X1_20230810_LH00227_0005_A227HG7LT3_S42_L004_R1_001.fastq.gz
	elsif ($file =~ m/^ (\d{4,5} [xXPG] \d+ ) _\d+ _( [LHADM]{1,2}\d+ ) _\d+ _[A-Z\d\-]+ _S\d+ _L(\d+) _R(\d) _001 \. fastq \. (?: gz | ora ) $/x) {
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
	elsif ($file =~ m/^ (\d{4,5} [xX] \d+) _\d+ _( [LHADM]{1,2}\d+ ) _\d+ _[A-Z\d\-]+ _S\d+ _L(\d+) _I(\d) _001 \. fastq \. (?: gz | ora ) $/x) {
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
	elsif ($file =~ m/ .+ \. ( fastq | fq ) \.gz $/xin) {
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
	elsif ($file =~ m/ ( \.md5 | \.md5sum ) $/x) {
		my $ext = $1;
		my $fh = IO::File->new($file);
		my $line = $fh->getline;
		my ($m, undef) = split(/\s+/, $line, 2);
		$fh->close;
		push @removelist, $clean_name;
		$clean_name =~ s/$ext//;
		$checksums{$clean_name} = $m;
		print "   > processed md5 file for $clean_name\n" if $verbose;
		return; # do not continue
	}
	# multiple checksum file - with or without datetime stamp in front - ugh
	elsif ($file =~ 
m/^ ( \d{4} \. \d\d \. \d\d _ \d\d \. \d\d \. \d\d \. )? md5 (sum)? .* \. (txt | out ) $/xn
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
	elsif ($clean_name =~ /^ Fastq \/ .+ \. ( xml | csv ) $/xn) {
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
	$sample =~ s/[PG]/X/;
	
	### Record the collected file information
	my $status = 3;
	if ( exists $filedata{$clean_name} ) {
		# check if the files are unchanged
		my $old_size = $filedata{$clean_name}{Size} || 0;
		my $old_time = $filedata{$clean_name}{ftime} || 0;
		my $diff = abs( $date - $old_time );
		if ( $diff < 3601 and $size == $old_size ) {
			# files are equivalent taking into account a possible one hour difference
			$status = 2;
		}
	}
	$filedata{$clean_name}{Type}             = $type;
	$filedata{$clean_name}{ftime}            = $date;
	$filedata{$clean_name}{Size}             = $size;
	$filedata{$clean_name}{Archive}          = 'N';
	$filedata{$clean_name}{status}           = $status;
	$filedata{$clean_name}{MD5}              = q();
	if ($type eq 'Fastq') {
		$filedata{$clean_name}{sample_id}        = $sample || q(-);
		$filedata{$clean_name}{platform_unit_id} = $laneID || q(-);
		$filedata{$clean_name}{paired_end}       = $pairedID || q(-);
		if ( $machineID and exists $machinelookup{$machineID} ) {
			$filedata{$clean_name}{platform}     = $machinelookup{$machineID};
		}
		else {
			$filedata{$clean_name}{platform}     = q(?);
		}
	}
	else {
		$filedata{$clean_name}{sample_id}        = q();
		$filedata{$clean_name}{platform_unit_id} = q();
		$filedata{$clean_name}{paired_end}       = q();
		$filedata{$clean_name}{platform}         = q();
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
		print "   ! marking to delete 10X Genomics temporary file\n" if $verbose;
		push @removelist, $clean_name;
		return;
	}
	elsif ( $clean_name =~ /_STARtmp\// ) {
		# left over STAR alignment droppings, delete
		push @removelist, $clean_name;
		return;
	}
	elsif ( $clean_name =~ m/ \w+ \.stat \/ \w+ \. ( cnt | model | theta ) $/xn) {
		# STAR alignment statistic files, not worth keeping as they're uninterpretable
		push @removelist, $clean_name;
		return;
	}
	elsif ( $file =~ /^ \d{4,6} X \d{1,3} \. [12] \. fq $/x ) {
		# an uncompressed temporary fastq file from a hciR pipeline
		# these should be automatically deleted unless the pipeline failed
		# attempt to delete immediately because we don't want these taking up space
		if (unlink $file) {
			printf "   ! deleted temp fastq %s\n", $clean_name;
		}
		else {
			printf "   ! marking to delete temp fastq %s\n", $clean_name;
			push @removelist, $clean_name;
		}
		return;
	}
	elsif ( $file eq 'Aligned.sortedByCoord.out.bam' ) {
		# STAR output bam file, normally removed by hciR alignment pipeline
		printf "   ! marking to delete temp STAR bam %s\n", $clean_name;
		push @removelist, $clean_name;
		return;
	}
	elsif ( $file eq 'Aligned.toTranscriptome.out.bam' ) {
		# STAR output bam file, normally removed by hciR alignment pipeline
		printf "   ! marking to delete temp STAR bam %s\n", $clean_name;
		push @removelist, $clean_name;
		return;
	}
	elsif ( $file eq 'Signal.Unique.str1.out.bg' ) {
		# STAR output bedgraph file, normally removed by hciR alignment pipeline
		printf "   ! marking to delete temp STAR bedgraph %s\n", $clean_name;
		push @removelist, $clean_name;
		return;
	}
	elsif ( $file eq 'Signal.UniqueMultiple.str1.out.bg' ) {
		# STAR output bedgraph file, normally removed by hciR alignment pipeline
		printf "   ! marking to delete temp STAR bedgraph %s\n", $clean_name;
		push @removelist, $clean_name;
		return;
	}
	elsif ( $file eq 'uniq.bg' ) {
		# STAR output bedgraph file, normally removed by hciR alignment pipeline
		printf "   ! marking to delete temp bedgraph %s\n", $clean_name;
		push @removelist, $clean_name;
		return;
	}
	elsif ( $file eq 'mult.bg' ) {
		# STAR output bedgraph file, normally removed by hciR alignment pipeline
		printf "   ! marking to delete temp bedgraph %s\n", $clean_name;
		push @removelist, $clean_name;
		return;
	}
	elsif ( $clean_name =~ m| / \. snakemake / |x ) {
		# snakemake log files do not need be kept - they're temporary
		push @removelist, $clean_name;
		return;
	}
	elsif ( $clean_name =~ m| / \. GQueryIndex / |x ) {
		# GQuery index files, it's a hidden directory so temporary and not worth keeping
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
	
	if ($file =~ /\. (bw | bigwig | bb | bigbed | hic) $/xin) {
		# an indexed analysis file
		$filetype = 'BrowserTrack';
		$zip = 0;
	}
	elsif ($file =~ /\. ( bam | cram | sam\.gz ) $/xin) {
		# an alignment file
		$filetype = 'Alignment';
		$zip = 0;
	}
	elsif ( $file =~ /\. ( bai | csi | crai | tbi ) $/xin) {
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
	elsif ($file =~ /\. ( vcf | maf ) $/xin) {
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
	elsif ($file =~ /\. [cv] loupe $/xi) {
		# 10X genomics loupe file
		$filetype = 'Analysis';
		$zip = 0;
	}
	elsif ($file =~ /\. loom $/xi) {
		# Velocyto loom file
		$filetype = 'Analysis';
		$zip = 0;
	}
	elsif ($file =~ /^ unmapped \.out \.mate ([12]) /xi) {
		# unmapped fastq from STAR - may need to be renamed and/or compressed
		my $paired = $1;
		if ($file =~ /\. ( fq | fastq ) \.gz $/xn) {
			# perfect, has extension and is compressed, nothing to do
			$filetype = 'Fastq';
		}
		elsif ($file =~ /\. ( fq | fastq ) $/xn) {
			# right extension, not compressed
			$filetype = 'Fastq';
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
		elsif ($file =~ /\.mate [12] $/xi) {
			# no extension but likely fastq, not compressed
			$filetype = 'Fastq';
			my $new_file  = $file . '.fastq';
			my $new_clean = $clean_name . '.fastq';
			if ( move($file, $new_file) ) {
				my $command = sprintf "%s \"%s\"", $gzipper, $new_file;
				if (system($command)) {
					print "   ! failed to automatically compress '$new_clean': $OS_ERROR\n";
					$zip = 1; 
				}
				else {
					$new_file  .= '.gz';
					$new_clean .= '.gz';
					print "   > automatically gzip compressed '$clean_name' to '$new_file'\n";
					$file       = $new_file;
					$clean_name = $new_clean;
					($date, $size) = get_file_stats($file);
				}
			}
			else {
				print "   ! failed to rename and compress '$clean_name': $OS_ERROR\n";
			}
			
		}
		elsif ($file =~ /\. mate [12] \. gz $/xi) {
			# no extension but at least compressed
			$filetype = 'Fastq';
		}
		else {
			# something else? ok, whatever
			# leave as is, I guess
			$filetype = 'Other';
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
		if ($filetype eq 'Fastq') {
			$filedata{$clean_name}{paired_end}       = $paired;
			$filedata{$clean_name}{platform_unit_id} = q(-);
			$filedata{$clean_name}{platform}         = q(-);
			$filedata{$clean_name}{sample_id}        = q(-);
		}
	}
	elsif ($file =~ /\. ( fq | fastq ) (\.gz)? $/xin) {
		# fastq file
		if ($file =~ /^ \d{4,6} X \d{1,3} /x) {
			if ($file =~ /_umi \.fastq \.gz $/x) {
				# looks like a merged UMI fastq file. I guess keep it?
			}
			elsif ($file =~ /^ \d{4,6} X \d{1,3} _ \d{6} _ .+ _R\d_001 \.fastq\.gz$/x) {
				print "   ! marking to delete probable HCI Fastq file $clean_name\n";
				push @removelist, $clean_name;
				return;
			}
			else {
				print "   ! possible HCI Fastq file $clean_name\n";
			}
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
		if ( $file =~ / [\.\-_] r? ([12]) [\.\-_] /xi ) {
			$paired = $1;
		}
		$filedata{$clean_name}{paired_end}       = $paired;
		$filedata{$clean_name}{platform_unit_id} = q(-);
		$filedata{$clean_name}{platform}         = q(-);
		$filedata{$clean_name}{sample_id}        = q(-);
	}
	elsif ($file =~ /\. ( fa | fasta | ffn ) (\.gz )? $/xin) {
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
	elsif ($file =~ /\. (fai | dict) (\.gz)? $/xin) {
		# sequence index, do not need to keep
		push @removelist, $clean_name;
		return;
	}
	elsif ($file =~ /\. ( bed | bed\d+ | gtf | gff | gff\d | narrowpeak | broadpeak | gappedpeak | refflat | genepred | ucsc) (\.gz)? $/xin) {
		$filetype = 'Annotation';
		if ($file =~ /\.gz$/ and $size > 10485760) {
			# do not archive if compressed and bigger 10 MB
			$zip = 0;
		}
		else {
			$zip = 1;
		}
	}
	elsif ($file =~ /\. ( sh | pl | py | r | rmd | rscript | awk | sm | sing ) $/xin) {
		$filetype = 'Script';
		$zip = 1;
	}
	elsif ($file eq 'cmd.txt') {
		# pysano command script
		$filetype = 'Script';
		$zip = 1;
	}
	elsif ($file =~ /\. ( txt | tsv | tab | csv | cdt | counts | results | cns | cnr | cnn | md | log | biotypes | summary | rna_metrics | out | err | idxstats? ) (\.gz)? $/xin) {
		# general analysis text files, may be compressed
		$filetype = 'Text';
		$zip = 1;
	}
	elsif ($file =~ /\. ( wig | bg | bdg | bedgraph ) (\.gz)? $/xin) {
		$filetype = 'Analysis';
		$zip = 1;
	}
	elsif ($file =~ /\. mpileup.* \.gz $/xi) {
		# compressed mpileup files ok?, some people stick in text between mpileup and gz
		$filetype = 'Analysis';
		$zip = 0;
	}
	elsif ($file =~ /\. ( bar | bar\.zip | useq | swi | swi\.gz | egr | ser | mpileup | motif | cov | mtx | mtx\.gz ) $/xin) {
		$filetype = 'Analysis';
		$zip = 1;
	}
	elsif ($file =~ /\. ( xls | ppt | pptx | doc | docx | rout | rda | rdata | rds | rproj | xml | yaml | json | json\.gz | seg| html | pzfx ) $/xin) {
		$filetype = 'Results';
		$zip = 1;
	}
	elsif ($file =~ /\. ( pdf | ps | eps | png | jpg | jpeg | gif | tif | tiff | svg | ai ) $/xin) {
		$filetype = 'Image';
		$zip = 1;
	}
	elsif ($file =~ /\. ( xlsx | h5 | hd5 | hdf5 | h5ad ) $/xin) {
		# leave out certain result files from zip archive just to be nice
		$filetype = 'Results';
		$zip = 0;
	}
	elsif ($file =~ /\. bismark \. cov $/xi) {
		$filetype = 'Analysis';
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
			$zip = 0;
		}
	}
	elsif ($file =~ /\. bismark \. cov \.gz $/xi) {
		$filetype = 'Analysis';
		$zip = 0;
	}
	elsif ($file =~ /\. ( zip | tar | tar\.gz | tar\.bz2 | tgz ) $/xin) {
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
	elsif ($file =~ /\. ( \d\.bt2 | fa(sta)?\.amb | fa(sta)?\.ann | fa(sta)?\.bwt | fa(sta)?\.pac | nix | novoindex | index | fa(sta)?\.0123 | fa(sta)?\.bwt\.2bit\.64 ) $/xn) {
		# alignment indexes can be rebuilt - discard
		push @removelist, $clean_name;
		return;
	}
	elsif ($file =~ / \.vcf \.idx $/xi) {
		# a GATK-style index for non-tabix, uncompressed VCF files
		# This index isn't useful for browsing or for anything other than GATK
		# and it will auto-recreate anyway, so toss
		push @removelist, $clean_name;
		return;
	}
	elsif ($file =~ /\.pyc$/i) {
		# compiled python file!!?
		push @removelist, $clean_name;
		return;
	}
	elsif ($file =~ /\.sif$/i) {
		# singularity container file
		$filetype = 'SingularityFile';
		$zip = 0;
	}
	else {
		# catchall
		$filetype = 'Other';
		if ($size > $max_zip_size) {
			# it's too big to zip
			printf "   ! Large unknown file $clean_name at %.1fG\n", $size / 1073741824;
			$zip = 0;
		}
		else {
			$zip = 1;
		}
	}
	
	# sanity check - this should not be empty
	unless ($filetype) {
		print "   ! Logic error! No filetype for $clean_name\n";
		$failure_count++;
	}
	
	# Check files for Zip archive files
	if (not $do_zip) {
		# we're not zipping anything so turn it off
		$zip = 0; 
	}
	
	### Record the collected file information
	my $status = 3;
	if ( exists $filedata{$clean_name} ) {
		# check if the files are unchanged
		my $old_size = $filedata{$clean_name}{Size} || 0;
		my $old_time = $filedata{$clean_name}{ftime} || 0;
		my $diff = abs( $date - $old_time );
		if ( $diff < 3601 and $size == $old_size ) {
			# files are equivalent taking into account a possible one hour difference
			$status = 2;
		}
	}
	$filedata{$clean_name}{Type}     = $filetype;
	$filedata{$clean_name}{ftime}    = $date;
	$filedata{$clean_name}{Size}     = $size;
	$filedata{$clean_name}{Archived} = $zip ? 'Y' : 'N';
	$filedata{$clean_name}{status}   = $status;
	if ($status == 3) {
		$filedata{$clean_name}{MD5}  = $Project->calculate_file_checksum($file);
	}
	
	# Check for sample ID
	if ( $clean_name =~ / (\d{4,6}X\d{1,3}) [\.\-_\/] /x ) {
		$filedata{$clean_name}{sample_id} = $1;
	}
	else {
		$filedata{$clean_name}{sample_id} = q();
	}
	
	# removed everything except for any browser tracks
	if (not $zip) {
		if ($filetype eq 'BrowserTrack') {
			# we only keep browser tracks for Analysis projects, not Request projects
			push @removelist, $clean_name if $request;
		}
		else {
			push @removelist, $clean_name;
		}
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





