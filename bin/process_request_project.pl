#!/usr/bin/env perl

use warnings;
use strict;
use English qw(-no_match_vars);
use IO::File;
use File::Find;
use File::Spec;
use POSIX qw(strftime);
use Getopt::Long;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use Net::SB;
use RepoCatalog;
use RepoProject;
use Emailer;

our $VERSION = 5.7;

# shortcut variable name to use in the find callback
use vars qw(*fname);
*fname = *File::Find::name;



######## Documentation
my $doc = <<END;

A script to process GNomEx Experiment Request project folders for 
Seven Bridges upload.

This will generate a manifest CSV file from the fastq files, and 
optionally upload the files using SB tools, and optionally then hide 
the Fastq files in a hidden directory in preparation for eventual 
deletion from the server.

This requires collecting data from the GNomEx LIMS database and 
passing certain values on to this script for inclusion as metadata 
in the metadata Manifest CSV file. Alternatively, simply provide 
a Catalog database file with the project ID.

A Markdown description is generated for the Seven Bridges Project 
using the GNomEx metadata, including user name, strategy, title, 
and group name.

Version: $VERSION

Example usage:
    process_request_project.pl [options] /Repository/MicroarrayData/2019/1234R
    
    process_request_project.pl --cat Request.db [options] 1234R

Options:
 
 Main functions - not exclusive
    --scan              Scan the project folder and generate the manifest
    --upload            Prepare the project for upload
    --hide              Hide the Fastq files in hidden deletion folder

 Metadata
    --cat <path>        Provide path to metadata catalog database
    --first <text>      User first name for the owner of the project
    --last <text>       User last name for the owner of the project
    --email <text>      User email address for notifications
    --strategy "text"   GNomEx database application value. This is a long 
                        and varied text field, so must be protected by 
                        quoting. It will be distilled into a single-word 
                        value based on regular expression of keywords.
    --title "text"      GNomEx Request Name or title. This is a long text 
                        field, so must be protected by quoting.
    --group "text"      GNomEx Request group or ProjectName. This is a long 
                        text field, so must be protected by quoting.

 Options
    --desc "text"       Description for new SB project when uploading. 
                        Can be Markdown text.
    --notify            Send email to user and PI when uploading
    --verbose           Tell me everything!
 
 Seven Bridges
    --division <text>   The Seven Bridges division name

 Paths
    --cred <path>       Path to the Seven Bridges credentials file. 
                        Default is ~/.sevenbridges/credentials. Each profile 
                        should be named after the SB division.

END



######## Process command line options
my $path;
my $scan;
my $hide_files;
my $upload;
my $cat_file;
my $userfirst     = q();
my $userlast      = q();
my $email_address = q();
my $strategy;
my $title         = q();
my $group         = q();
my $description   = q();
my $sb_division   = q();
my $cred_path     = q();
my $send_email;
my $verbose;

if (scalar(@ARGV) > 1) {
	GetOptions(
		'scan!'         => \$scan,
		'hide!'         => \$hide_files,
		'upload!'       => \$upload,
		'catalog=s'     => \$cat_file,
		'first=s'       => \$userfirst,
		'last=s'        => \$userlast,
		'strategy=s'    => \$strategy,
		'title=s'       => \$title,
		'group=s'       => \$group,
		'desc=s'        => \$description,
		'notify!'       => \$send_email,
		'division=s'    => \$sb_division,
		'cred=s'        => \$cred_path,
		'verbose!'      => \$verbose,
	) or die "please recheck your options!\n\n$doc\n";
	$path = shift @ARGV;
}
else {
	print $doc;
	exit;
}




######## Check options

# grab all parameters from the catalog database if provided
if ($cat_file) {
	
	# first check path
	if ($cat_file !~ m|^/|) {
		# catalog file path is not from root
		$cat_file = File::Spec->catfile( File::Spec->rel2abs(), $cat_file);
	}
	
	# find entry in catalog and collect information
	my $Catalog = RepoCatalog->new($cat_file) or 
		die "Cannot open catalog file '$cat_file'!\n";
	if ($path =~ /(\d{4,5}R)/) {
		my $id = $1;
		my $Entry = $Catalog->entry($id) or 
			die "No Catalog entry for $id\n";
		# collect metadata
		if (not $userfirst) {
			$userfirst = $Entry->user_first;
		}
		if (not $userlast) {
			$userlast = $Entry->user_last;
		}
		if (not $email_address) {
			$email_address = $Entry->user_email;
		}
		if (not $strategy) {
			$strategy = $Entry->request_application;
		}
		if (not $title) {
			$title = $Entry->name;
		}
		if (not $group) {
			$group = $Entry->group;
		}
		if (not $sb_division) {
			$sb_division = $Entry->division;
		}
		$path = $Entry->path;
	}
	else {
		die "unrecognized project identifier '$path'!\n";
	}
}


if ($upload) {
	die "must provide a SB division name!\n" unless $sb_division;
	die "must provide a title for SB project!\n" unless $title;
}




######## Global variables
# these are needed since the File::Find callback doesn't accept pass through variables
my $start_time = time;
my @removelist;
my %filedata;
my %checksums;
my $failure_count = 0;
my $runfolder_warning; # Gigantic Illumina RunFolder present

# our sequence machine IDs to platform technology lookup
my %machinelookup = (
	'D00550'  => 'Illumina HiSeq', # 2500
	'D00294'  => 'Illumina HiSeq', # 2500
	'A00421'  => 'Illumina NovaSeq', # 6000
	'M05774'  => 'Illumina MiSeq',
	'M00736'  => 'Illumina MiSeq',
	'DQNZZQ1' => 'Illumina HiSeq', # 2000
	'HWI-ST1117' => 'Illumina HiSeq', # 2000
);

# experimental strategy
# the SB metadata expects simple value, so define this by matching with regex
# values are suggested by SB documentation, except for Single-cell-Seq
# the GNomEx application is too varied for anything more complicated. 
my $experimental_strategy;
if ($strategy) {
	if ($strategy =~ /10X Genomics/) {
		$experimental_strategy = 'Single-Cell-Seq';
	}
	elsif ($strategy =~ /mirna/i) {
		$experimental_strategy = 'miRNA-Seq';
	}
	elsif ($strategy =~ /rna/i) {
		$experimental_strategy = 'RNA-Seq';
	}
	elsif ($strategy =~ m/(?: methyl | bisulfite )/xi) {
		$experimental_strategy = 'Bisulfite-Seq';
	}
	elsif ($strategy =~ /(?: exon | exome | capture)/xi) {
		$experimental_strategy = 'WXS';
	}
	elsif ($strategy =~ /(?: dna | chip | atac)/xi) {
		$experimental_strategy = 'DNA-Seq';
	}
	else {
		$experimental_strategy = 'Not available';
	}
}
else {
	$experimental_strategy = 'Not available';
}




####### Initiate Project

# Initialize
my $Project = RepoProject->new($path, $verbose) or 
	die "unable to initiate Repository Project!\n";

# check project ID
if ($Project->given_dir =~ m/A \d{1,5} \/? $/x) {
	# looks like an analysis project
	die "given path is an Analysis project! Stopping!\n";
}

printf " > working on %s at %s\n", $Project->id, $Project->given_dir;
printf "   using parent directory %s\n", $Project->parent_dir if $verbose;


# given application paths
if ($verbose) {
	print " => SB credentials path: $cred_path\n" if $cred_path;
}


# file paths
if ($verbose) {
	printf " =>  manifest file: %s\n", $Project->manifest_file;
	printf " =>    remove file: %s or %s\n", $Project->remove_file, $Project->alt_remove_file;
}


# removed file hidden folder
printf " => deleted folder: %s\n", $Project->delete_folder if $verbose;
if (-e $Project->delete_folder) {
	if ($hide_files) {
		print " ! deleted files hidden folder already exists! Will not move deleted files\n";
		$hide_files = 0; # do not want to move zipped files
		$failure_count++;
	} 
	if ($scan) {
		print "! cannot re-scan if deleted files hidden folder exists!\n";
		$scan = 0;
		$failure_count++;
	}
	if ($upload) {
		print "! cannot upload if deleted files hidden folder exists!\n";
		$upload = 0;
		$failure_count++;
	}
}






######## Main functions

# change to the given directory
printf " > changing to %s\n", $Project->given_dir if $verbose;
chdir $Project->given_dir or die sprintf("cannot change to %s!\n", $Project->given_dir);


# scan the directory
if ($scan) {
	# this will also run the zip function
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


# upload files to Seven Bridges
if ($upload) {
	if (-e $Project->manifest_file) {
		printf " > uploading %s project files to $sb_division\n", $Project->id;
		upload_files();
	}
	else {
		print " ! No manifest file! Cannot upload files\n";
		$failure_count++;
	}
}


# hide files
if ($hide_files) {
	if (-e $Project->alt_remove_file) {
		printf " > moving files to %s\n", $Project->delete_folder;
		$failure_count += $Project->hide_deleted_files;
	}
	else {
		print " ! No deleted files to hide\n";
		$failure_count++;
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
	else {
		printf "  > processed %d files\n", scalar keys %filedata;
	}
	
	
	### Generate manifest file
	# check for pairs
	my $is_paired = 0;
	foreach (keys %filedata) {
		if ($filedata{$_}{pairedID} eq '2') {
			# string comparison instead of numeric because it might be null
			$is_paired = 1; # true
			last;
		}
	}

	# compile list
	my @manifest;
	my %dupmd5; # for checking for errors
	push @manifest, join(',', qw(File sample_id investigation library_id platform 
								platform_unit_id paired_end quality_scale 
								experimental_strategy UserFirstName UserLastName Size Date MD5));
	foreach my $f (sort {$a cmp $b} keys %filedata) {
		
		# first check for md5 checksum
		my $md5;
		if (exists $filedata{$f}{md5}) {
			$md5 = $filedata{$f}{md5};
		}
		else {
			# we don't have a checksum for this file yet
			# check in the checksums hash for the file name without the path
			my (undef, undef, $filename) = File::Spec->splitpath($f);
			if (exists $checksums{$filename}) {
				# we have it
				$md5 = $checksums{$filename};
			}
			else {
				# don't have it!!!????? geez. ok, calculate it, this might take a while
				$md5 = $Project->calculate_file_checksum($f);
			}
		}
		
		# check for accidental duplicate checksums - this is a problem
		if (exists $dupmd5{$md5}) {
			print "  ! duplicate md5 checksum $md5 for $f\n";
		}
		$dupmd5{$md5} += 1;
		
		# add to the list
		push @manifest, join(',', 
			sprintf("\"%s\"", $filedata{$f}{clean}),
			$filedata{$f}{sample},
			$Project->id,
			$filedata{$f}{sample},
			sprintf("\"%s\"", $machinelookup{$filedata{$f}{machineID}} || q() ),
			$filedata{$f}{laneID},
			$is_paired ? $filedata{$f}{pairedID} : '-',
			'sanger',
			$experimental_strategy,
			sprintf("\"%s\"", $userfirst),
			sprintf("\"%s\"", $userlast),
			$filedata{$f}{size},
			sprintf("\"%s\"", $filedata{$f}{date}),
			$md5,
		);
		push @removelist, $filedata{$f}{clean};
	}	
	
	
	### Write files
	# manifest
	my $fh = IO::File->new($Project->manifest_file, 'w') or 
		die sprintf("unable to write manifest file %s: $OS_ERROR\n", $Project->manifest_file);
	foreach (@manifest) {
		$fh->print("$_\n");
	}
	$fh->close;
	printf "  > wrote %d files to manifest %s\n", scalar(@manifest) - 1, 
		$Project->manifest_file;
	
	# remove list
	$fh = IO::File->new($Project->alt_remove_file, 'w') or 
		die sprintf("unable to write manifest file %s: $OS_ERROR\n", $Project->alt_remove_file);
	foreach (@removelist) {
		$fh->print("$_\n");
	}
	$fh->close;
	printf "  > wrote %d files to remove list %s\n", scalar(@removelist), 
		$Project->alt_remove_file;
	
	return 1;
}


# find callback
sub callback {
	my $file = $_;
	print "  > find callback on $file for $fname\n" if $verbose;

	# generate a clean name for recording
	my $clean_name = $fname;
	$clean_name =~ s|^\./||; # strip the beginning ./ from the name to clean it up
	
	
	### Ignore certain files
	if (-l $file) {
		# we will delete symlinks
		print "   ! deleting symbolic link $clean_name\n";
		unlink $file;
		return;
	}
	elsif (-d $file) {
		# skip directories
		print "   > skipping directory\n" if $verbose;
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
		print "   ! deleting file browser metadata file\n" if $verbose;
		unlink $file;
		return;
	}
	elsif ($file eq $Project->remove_file) {
		return;
	}
	elsif ($file eq $Project->notice_file) {
		return;
	}
	elsif ($file eq $Project->manifest_file) {
		return;
	}
	elsif ($fname =~ m/^\. \/ (?: bioanalysis | Sample.?QC | Library.?QC | Sequence.?QC | Cell.Prep.QC ) (?:.?\w+)? \/ /x) {
		# these are QC samples in a bioanalysis or Sample of Library QC folder
		# directly under the main project 
		print "   > skipping bioanalysis file $fname\n" if $verbose;
		return;
	}
	elsif ($fname =~ /^\. \/ RunFolder/x) {
		# a few external requesters want the entire original RunFolder 
		# these folders typically have over 100K files!!!!
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
	elsif ($fname =~ /^ \. \/ upload_staging/x) {
		# somebody directly uploaded files to this directory!
		print "   ! skipping uploaded file $fname\n";
		return;
	}
	elsif ($fname =~ / samplesheet \. \w+ /xi) {
		# file run sample sheet, can safely ignore
		print "   > skipping file $fname\n" if $verbose;
		return;
	}
	elsif ($file =~ /\.xlsx$/) {
		# some stray request spreadsheet file
		print "   ! skipping $file\n";
		return;
	}
	elsif ( $file =~ /\.txt$/ and $file !~ /md5/ ) {
		# additional stray files but not md5 files!
		print "   ! skipping $file\n";
		return;
	}
	elsif ($file =~ /\.pdf$/) {
		# some PDF files 
		print "   ! skipping $file\n";
		return;
	}
	
	
	### Possible Fastq file types
	my ($sample, $machineID, $laneID, $pairedID);
	# 15945X8_190320_M05774_0049_MS7833695-50V2_S1_L001_R2_001.fastq.gz
	# new style: 16013X1_190529_D00550_0563_BCDLULANXX_S12_L001_R1_001.fastq.gz
	if ($file =~ m/^ (\d{4,5} [xX] \d+ ) _\d+ _( [ADM]\d+ ) _\d+ _[A-Z\d\-]+ _S\d+ _L(\d+) _R(\d) _001 \. (?: txt | fastq ) \.gz$/x) {
		$sample = $1;
		$machineID = $2;
		$laneID = $3;
		$pairedID = $4;
	}
	# new style index: 15603X1_181116_A00421_0025_AHFM7FDSXX_S4_L004_I1_001.fastq.gz
	elsif ($file =~ m/^ (\d{4,5} [xX] \d+) _\d+ _( [ADM]\d+ ) _\d+ _[A-Z\d\-]+ _S\d+ _L(\d+) _I\d _001 \. (?: txt | fastq ) \.gz$/x) {
		$sample = $1;
		$machineID = $2;
		$laneID = $3;
		$pairedID = 3;
	}
	# new old style HiSeq: 15079X10_180427_D00294_0392_BCCEA1ANXX_R1.fastq.gz
	elsif ($file =~ m/^ ( \d{4,5} [xX] \d+ ) _\d+ _( [ADM]\d+ ) _\d+ _[A-Z\d\-]+ _R(\d) \. (?: txt | fastq ) \.gz$/x){
		$sample = $1;
		$machineID = $2;
		$laneID = 1;
		$pairedID = $3;
	}
	# old style, single-end: 15455X2_180920_D00294_0408_ACCFVWANXX_2.txt.gz
	elsif ($file =~ m/^ ( \d{4,5} [xX] \d+ ) _\d+ _( [ADM] \d+ ) _\d+ _[A-Z\d]+ _(\d) \.txt \.gz $/x) {
		$sample = $1;
		$machineID = $2;
		$laneID = $3;
	}
	# old style, paired-end: 15066X1_180427_D00294_0392_BCCEA1ANXX_5_1.txt.gz
	elsif ($file =~ m/^ ( \d{4,5} [xX] \d+ ) _\d+ _( [ADM] \d+ ) _\d+ _[A-Z\d]+ _(\d) _[12] \.txt \.gz$/x) {
		$sample = $1;
		$machineID = $2;
		$laneID = $3;
		$pairedID = $4;
	}
	# 10X genomics and MiSeq read file: 15454X1_S2_L001_R1_001.fastq.gz, sometimes not gz????
	elsif ($file =~ m/^ ( \d{4,5} [xX] \d+ ) _S\d+ _L(\d+) _R(\d) _001 \.fastq (?:\.gz)? $/x) {
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
	elsif ($file =~ m/^ ( \d{4,5} [xX] \d+ ) _S\d+ _L(\d+) _I1 _001 \.fastq \.gz $/x) {
		$sample = $1;
		$laneID = $2;
		$pairedID = 3;
		# must grab the machine ID from the read name
		my $head = qx(gzip -dc $file | head -n 1);
		if ($head =~ /^ @ ( [ADM]\d+ ) :/x) {
			$machineID = $1;
		}
	}
	# another MiSeq file: 15092X7_180424_M00736_0255_MS6563328-300V2_R1.fastq.gz
	elsif ($file =~ m/^ ( \d{4,5} [xX] \d+ ) _\d+ _( [ADM] \d+ ) _\d+ _[A-Z\d\-]+ _R(\d) \.fastq \.gz $/x) {
		$sample = $1;
		$machineID = $2;
		$pairedID = $3;
		$laneID = 1;
	}
	# crazy name: GUPTA_S1_L001_R1_001.fastq.gz
	elsif ($file =~ m/^ ( \w+ ) _S\d+ _L(\d+) _R(\d )_00\d \.fastq \.gz $/x) {
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
	elsif ($file =~ m/^ ( \w+ ) _S\d+ _L(\d+) _I\d _00\d \.fastq \.gz $/x) {
		$sample = $1;
		$laneID = $2;
		$pairedID = 3;
		# must grab the machine ID from the read name
		my $head = qx(gzip -dc $file | head -n 1);
		if ($head =~ /^ @ ( [ADM]\d+ ) :/x) {
			$machineID = $1;
		}
	}
	# really old single-end file: 9428X9_120926_SN1117_0117_AC168KACXX_8.txt.gz
	elsif ($file =~ m/^ ( \d{4,5} [xX] \d+ ) _\d+ _SN\d+ _\d+ _[A-Z\d]+ _(\d) \.txt \.gz $/x) {
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
		# I can't extract metadata information
		# but at least it will get recorded in the manifest and list files
		print "   ! processing unrecognized Fastq file $fname\n";
		$sample = q();
		$laneID = q();
		$pairedID = q();
		$machineID = q();
	}
	# single checksum file
	elsif ($file =~ m/\.gz\.md5$/) {
		my $fh = IO::File->new($file);
		my $line = $fh->getline;
		my ($md5, undef) = split(/\s+/, $line);
		$fh->close;
		$fname =~ s/\.md5$//;
		$filedata{$fname}{md5} = $md5;
		print "   > processed md5 file\n" if $verbose;
		push @removelist, $clean_name;
		return; # do not continue
	}
	# multiple checksum file
	elsif ($file =~ m/^ md5.* \. (?: txt | out ) $/x) {
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
		print "   > processed md5 file\n" if $verbose;
		push @removelist, $clean_name;
		return; # do not continue
	}
	elsif ($fname =~ /^ \. \/ Fastq \/ .+ \. (?: xml | csv ) $/x) {
		# other left over files from de-multiplexing
		print "   ! skipping demultiplexing file $fname\n";
		return;
	}
	elsif ($file eq $Project->ziplist_file) {
		# really old projects might still have these - keep it
		print "   ! old archive list file $file present\n";
	}
	elsif ($file eq $Project->zip_file) {
		# really old projects might still have these - keep it
		print "   ! old archive zip file $file present\n";
	}
	else {
		# programmer error!
		print "   ! unrecognized file $fname\n";
		$failure_count++;
		return;
	}
	
	# stats on the file
	my @st = stat($file);
	
	### Record the collected file information
	$filedata{$fname}{clean} = $clean_name;
	$filedata{$fname}{sample} = $sample;
	$filedata{$fname}{machineID} = $machineID;
	$filedata{$fname}{laneID} = $laneID;
	$filedata{$fname}{pairedID} = $pairedID;
	$filedata{$fname}{date} = strftime("%B %d, %Y %H:%M:%S", localtime($st[9]));
	$filedata{$fname}{size} = $st[7];
	
	print "   > processed file\n" if $verbose;
}


sub upload_files {
	
	### Grab missing information
	unless ($userfirst and $userlast and $strategy) {
		# this can be grabbed from the first data line in the manifest CSV
		my $fh = IO::File->new($Project->manifest_file) or die "unable to read manifest file!";
		my $h = $fh->getline;
		my $l = $fh->getline;
		my @d = split(/,/, $l);
		$userfirst = $d[9];
		$userlast  = $d[10];
		$strategy  = $d[8];
		$fh->close;
	}
	
	### Initialize SB wrapper
	my $sb = Net::SB->new(
		div     => $sb_division,
		cred    => $cred_path,
	) or die "unable to initialize SB wrapper module!";
	$sb->verbose(1) if $verbose;
	
	
	### Create the project on Seven Bridges
	# first check whether it exists
	# use lower case to mimic the short name that would be used.
	my $sbproject = $sb->get_project(lc($Project->id));
	
	# create the project if it doesn't exist
	if (defined $sbproject) {
		# we must be picking up from a previous upload
		printf "   > using existing SB project %s\n", $sbproject->id;
	}
	else {
		
		# check description
		if (not $description) {
			# generate simple description in markdown
			$description = sprintf "# %s\n## %s\n GNomEx project %s is a %s experiment generated by %s %s",
				$Project->id, $title, $Project->id, $strategy, $userfirst, $userlast;
			if ($group) {
				$description .= " in the group '$group'. ";
			}
			else {
				$description .= ". ";
			}
			$description .= sprintf("Details on the experiment may be found in [GNomEx](https://hci-bio-app.hci.utah.edu/gnomex/?requestNumber=%s).\n", 
				$Project->id);
			$description .= "\n**Warning:** After these files are removed from GNomEx, these may be your only copies. Do not delete!\n";
		}
		
		# create project
		$sbproject = $sb->create_project(
			name        => $Project->id,
			description => $description,
		);
		if ($sbproject and $sbproject->id) {
			printf "   > created SB project %s\n", $sbproject->id;
			# rename the project to something more meaningful
			$sbproject->update(
				name => sprintf("%s: $title", $Project->id)
			);
		}
		else {
			print "   ! failed to make SB project!\n";
			$failure_count++;
			return;
		}
	}
	print "     Ready for upload\n";
	
	return 1;
}


sub add_user_to_sb_project {
	my ($division, $sbproject) = @_;
	
	# find division member
	my $divMember;
	foreach my $member ($division->list_members) {
		if (lc($member->email) eq lc($email_address)) {
			# email matches, that was easy!
			$divMember = $member;
			last;
		}
		elsif (
			lc($member->last_name) eq lc($userlast) and 
			lc($member->first_name) eq lc($userfirst)
		) {
			# first and last names match
			$divMember = $member;
			last;
		}
	}
	return unless ($divMember);
	
	# check if user is already a member of project
	foreach my $member ($sbproject->list_members) {
		if ($member->username eq $divMember->username) {
			# user is already a member of this project!
			# nothing more needs to be done
			return $member->username;
		}
	}
	
	# otherwise add member with full permissions
	my @permissions = (
		'read'      => 'true',
		'copy'      => 'true',
		'write'     => 'true',
		'execute'   => 'true',
		'admin'     => 'true'
	);
	my $pMember = $sbproject->add_member($divMember, @permissions);
	if ($pMember) {
		return $pMember->username;
	}
	
	return;
}




__END__

=head1 AUTHOR

 Timothy J. Parnell, PhD
 Dept of Oncological Sciences
 Huntsman Cancer Institute
 University of Utah
 Salt Lake City, UT, 84112

This package is free software; you can redistribute it and/or modify
it under the terms of the Artistic License 2.0.  





