#!/usr/bin/perl


use strict;
use IO::File;
use File::Spec;
use File::Find;
use File::Copy;
use File::Path qw(make_path);
use POSIX qw(strftime);
use Digest::MD5;
use Getopt::Long;
use FindBin qw($Bin);
use lib $Bin;
use SB;

my $version = 3;

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
in the metadata Manifest CSV file. 

A Markdown description is generated for the Seven Bridges Project 
using the GNomEx metadata, including user name, strategy, title, 
and group name.

Version: $version

Usage:
    process_request_project.pl [options] /Repository/MicroarrayData/2019/1234R

Options:

 Main functions - not exclusive
    --scan              Scan the project folder and generate the manifest
    --upload            Run the sbg-uploader program to upload
    --hide              Hide the Fastq files in hidden deletion folder

 Metadata
    --first <text>      User first name for the owner of the project
    --last <text>       User last name for the owner of the project
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
    --verbose           Tell me everything!
 
 Seven Bridges
    --division <text>   The Seven Bridges division name

 Paths
    --sb <path>         Path to the Seven Bridges command-line api utility sb
    --sbup <path>       Path to the Seven Bridges Java uploader start script,
                        sbg-uploader.sh
    --cred <path>       Path to the Seven Bridges credentials file. 
                        Default is ~/.sevenbridges/credentials. Each profile 
                        should be named after the SB division.

END



######## Process command line options
my $given_dir;
my $scan;
my $hide_files;
my $upload;
my $userfirst;
my $userlast;
my $strategy;
my $title;
my $group;
my $description = q();
my $sb_division;
my $sb_path = q();
my $sbupload_path = q();
my $cred_path = q();
my $verbose;

if (scalar(@ARGV) > 1) {
	GetOptions(
		'scan!'         => \$scan,
		'hide!'         => \$hide_files,
		'upload!'       => \$upload,
		'first=s'       => \$userfirst,
		'last=s'        => \$userlast,
		'strategy=s'    => \$strategy,
		'title=s'       => \$title,
		'group=s'       => \$group,
		'desc=s'        => \$description,
		'division=s'    => \$sb_division,
		'sb=s'          => \$sb_path,
		'sbup=s'        => \$sbupload_path,
		'cred=s'        => \$cred_path,
		'verbose!'      => \$verbose,
	) or die "please recheck your options!\n\n$doc\n";
	$given_dir = shift @ARGV;
}
else {
	print $doc;
	exit;
}


# Start up message for log tracking
print " > working on $given_dir\n";



######## Check options
if ($scan) {
	die "must provide user first name to scan!\n" unless $userfirst;
	die "must provide user last name to scan!\n" unless $userlast;
}
if ($upload) {
	die "must provide a SB division name!\n" unless $sb_division;
	die "must provide a title for SB project!\n" unless $title;
}




######## Global variables
# these are needed since the File::Find callback doesn't accept pass through variables
my $start_time = time;
my @removelist;
my $project;
my %filedata;
my %checksums;

# our sequence machine IDs to platform technology lookup
my %machinelookup = (
	'D00550'  => 'Illumina HiSeq',
	'D00294'  => 'Illumina HiSeq',
	'A00421'  => 'Illumina NovaSeq',
	'M05774'  => 'Illumina MiSeq',
	'M00736'  => 'Illumina MiSeq',
);

# experimental strategy
# the SB metadata expects simple value, so define this by matching with regex
# values are suggested by SB documentation, except for Single-cell-Seq
# the GNomEx application is too varied for anything more complicated. 
my $experimental_strategy;
if ($strategy =~ /10X Genomics/) {
	$experimental_strategy = 'Single-Cell-Seq';
}
elsif ($strategy =~ /mirna/i) {
	$experimental_strategy = 'miRNA-Seq';
}
elsif ($strategy =~ /rna/i) {
	$experimental_strategy = 'RNA-Seq';
}
elsif ($strategy =~ /(?:methyl|bisulfite)/i) {
	$experimental_strategy = 'Bisulfite-Seq';
}
elsif ($strategy =~ /(?:exon|exome|capture)/i) {
	$experimental_strategy = 'WXS';
}
elsif ($strategy =~ /(?:dna|chip|atac)/i) {
	$experimental_strategy = 'DNA-Seq';
}
else {
	$experimental_strategy = 'Not available';
}





####### Check directories

# empirical directories
if ($verbose) {
	print " => SB path: $sb_path\n" if $sb_path;
	print " => SB uploader path: $sbupload_path\n" if $sbupload_path;
	print " => SB credentials path: $cred_path\n" if $cred_path;
}

# check directory
unless ($given_dir =~ /^\//) {
	die "given path does not begin with / Must use absolute paths!\n";
}
unless (-e $given_dir) {
	die "given path $given_dir does not exist!\n";
}

# extract the project ID
if ($given_dir =~ m/(\d{3,5}R)\/?$/) {
	# look for R suffix project identifiers
	# this ignores Request digit suffixes such as 1234R1, 
	# when clients submitted replacement samples
	$project = $1;
}
elsif ($given_dir =~ m/A\d{1,5}\/?$/) {
	# looks like an analysis project
	die "given path is an Analysis project! Stopping!\n";
}
elsif ($given_dir =~ m/(\d{2,4})\/?$/) {
	# old style naming convention without an A prefix or R suffix
	$project = $1;
}
else {
	# non-canonical path, take the last given directory
	my @dir = File::Spec->splitdir($given_dir);
	$project = @dir[-1];
}


# check directory and move into the parent directory if we're not there
my $parent_dir = './';
if ($given_dir =~ m/^(\/Repository\/(?:MicroarrayData|AnalysisData)\/\d{4})\/?/) {
	$parent_dir = $1;
}
elsif ($given_dir =~ m/^(.+)$project\/?$/) {
	$parent_dir = $1;
}
print "   using parent directory $parent_dir\n" if $verbose;

# change to the given directory
print " > changing to $given_dir\n" if $verbose;
chdir $given_dir or die "cannot change to $given_dir!\n";







####### Prepare file names

# file names in project directory
my $manifest_file = $project . "_MANIFEST.csv";
my $remove_file   = $project . "_REMOVE_LIST.txt";
my $notice_file   = "where_are_my_files.txt";

# hidden file names in parent directory
my $alt_remove    = File::Spec->catfile($parent_dir, $project . "_REMOVE_LIST.txt");

if ($verbose) {
	print " =>  manifest file: $manifest_file\n";
	print " =>    remove file: $remove_file or $alt_remove\n";
}


# removed file hidden folder
my $deleted_folder = File::Spec->catfile($parent_dir, $project . "_DELETED_FILES");
print " => deleted folder: $deleted_folder\n" if $verbose;
if (-e $deleted_folder) {
	print " ! deleted files hidden folder already exists! Will not move deleted files\n" if $hide_files;
	$hide_files = 0; # do not want to move zipped files
	print "! cannot re-scan if deleted files hidden folder exists!\n" if $scan;
	$scan = 0;
}

# notification file
my $notice_source_file;
if ($given_dir =~ /MicroarrayData/) {
	$notice_source_file = "/Repository/MicroarrayData/missing_file_notice.txt";
}
elsif ($given_dir =~ /AnalysisData/) {
	$notice_source_file = "/Repository/AnalysisData/missing_file_notice.txt";
}
else {
	# primarily for testing purposes
	$notice_source_file = "~/missing_file_notice.txt";
}






######## Main functions

# scan the directory
if ($scan) {
	# this will also run the zip function
	print " > scanning $project in directory $parent_dir\n";
	scan_directory();
}


# upload files to Seven Bridges
if ($upload) {
	if (-e $manifest_file) {
		print " > uploading $project project files to $sb_division\n";
		upload_files();
	}
	else {
		print " ! No manifest file! Cannot upload files\n";
	}
}


# hide files
if ($hide_files) {
	if (-e $alt_remove) {
		print " > moving files to $deleted_folder\n";
		hide_deleted_files();
	}
	else {
		print " ! No deleted files to hide\n";
	}
}



######## Finished
printf " > finished with $project in %.1f minutes\n\n", (time - $start_time)/60;









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
				$md5 = calculate_checksum($f);
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
			$project,
			$filedata{$f}{sample},
			sprintf("\"%s\"", $machinelookup{$filedata{$f}{machineID}}),
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
	my $fh = IO::File->new($manifest_file, 'w') or 
		die "unable to write manifest file $manifest_file: $!\n";
	foreach (@manifest) {
		$fh->print("$_\n");
	}
	$fh->close;
	
	# remove list
	$fh = IO::File->new($alt_remove, 'w') or 
		die "unable to write manifest file $alt_remove: $!\n";
	foreach (@removelist) {
		$fh->print("$_\n");
	}
	$fh->close;
	
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
	if (-d $file) {
		# skip directories
		print "   > directory, skipping\n" if $verbose;
		return;
	}
	elsif (-l $file) {
		# we will delete symlinks
		print "   > symlink, skipping\n" if $verbose;
		push @removelist, $clean_name;
		return;
	}
	elsif ($file =~ /(?:libsnappyjava|fdt|fdtCommandLine)\.jar/) {
		# devil java spawn, delete!!!!
		print "   > deleting java file\n" if $verbose;
		unlink $file;
		return;
	}
	elsif ($file eq '.DS_Store' or $file eq 'Thumbs.db') {
		# Windows and Mac file browser devil spawn, delete these immediately
		print "   > deleting file browser metadata file\n" if $verbose;
		unlink $file;
		return;
	}
	elsif ($file eq $remove_file) {
		return;
	}
	elsif ($file eq $notice_file) {
		return;
	}
	elsif ($file eq $manifest_file) {
		return;
	}
	elsif ($fname =~ m/^\.\/(?:bioanalysis|Sample.?QC|Library.?QC|Sequence.?QC)\//) {
		# these are QC samples in a bioanalysis or Sample of Library QC folder
		# directly under the main project 
		print "   > skipping bioanalysis file\n" if $verbose;
		return;
	}
	elsif ($fname =~ /^\.\/RunFolder/) {
		# a few external requesters want the entire original RunFolder 
		# these folders typically have over 100K files!!!!
		# immediately stop processing and print warning. These must be handled manually
		die " ! Illumina RunFolder present! Terminating\n\n";
	}
	
	
	### Possible Fastq file types
	my ($sample, $machineID, $laneID, $pairedID);
	# 15945X8_190320_M05774_0049_MS7833695-50V2_S1_L001_R2_001.fastq.gz
	# new style: 16013X1_190529_D00550_0563_BCDLULANXX_S12_L001_R1_001.fastq.gz
	if ($file =~ m/^(\d{4,5}[xX]\d+)_\d+_([ADM]\d+)_\d+_[A-Z\d\-]+_S\d+_L(\d+)_R(\d)_001\.(?:txt|fastq)\.gz$/) {
		$sample = $1;
		$machineID = $2;
		$laneID = $3;
		$pairedID = $4;
	}
	# new style index: 15603X1_181116_A00421_0025_AHFM7FDSXX_S4_L004_I1_001.fastq.gz
	elsif ($file =~ m/^(\d{4,5}[xX]\d+)_\d+_([ADM]\d+)_\d+_[A-Z\d\-]+_S\d+_L(\d+)_I\d_001\.(?:txt|fastq)\.gz$/) {
		$sample = $1;
		$machineID = $2;
		$laneID = $3;
		$pairedID = 3;
	}
	# new old style HiSeq: 15079X10_180427_D00294_0392_BCCEA1ANXX_R1.fastq.gz
	elsif ($file =~ m/^(\d{4,5}[xX]\d+)_\d+_([ADM]\d+)_\d+_[A-Z\d\-]+_R(\d)\.(?:txt|fastq)\.gz$/){
		$sample = $1;
		$machineID = $2;
		$laneID = 1;
		$pairedID = $3;
	}
	# old style, single-end: 15455X2_180920_D00294_0408_ACCFVWANXX_2.txt.gz
	elsif ($file =~ m/^(\d{4,5}[xX]\d+)_\d+_([ADM]\d+)_\d+_[A-Z\d]+_(\d)\.txt\.gz$/) {
		$sample = $1;
		$machineID = $2;
		$laneID = $3;
	}
	# old style, paired-end: 15066X1_180427_D00294_0392_BCCEA1ANXX_5_1.txt.gz
	elsif ($file =~ m/^(\d{4,5}[xX]\d+)_\d+_([ADM]\d+)_\d+_[A-Z\d]+_(\d)_[12]\.txt\.gz$/) {
		$sample = $1;
		$machineID = $2;
		$laneID = $3;
		$pairedID = $4;
	}
	# 10X genomics and MiSeq read file: 15454X1_S2_L001_R1_001.fastq.gz, sometimes not gz????
	elsif ($file =~ m/^(\d{4,5}[xX]\d+)_S\d+_L(\d+)_R(\d)_001\.fastq(?:\.gz)?$/) {
		$sample = $1;
		$laneID = $2;
		$pairedID = $3;
		# must grab the machine ID from the read name
		my $head = $file =~ m/\.gz$/ ? qx(gzip -dc $file | head -n 1) : qx(head -n 1 $file);
		if ($head =~ /^@([ADM]\d+):/) {
			$machineID = $1;
		}
	}
	# 10X genomics index file: 15454X1_S2_L001_I1_001.fastq.gz
	elsif ($file =~ m/^(\d{4,5}[xX]\d+)_S\d+_L(\d+)_I1_001\.fastq\.gz$/) {
		$sample = $1;
		$laneID = $2;
		$pairedID = 3;
		# must grab the machine ID from the read name
		my $head = qx(gzip -dc $file | head -n 1);
		if ($head =~ /^@([ADM]\d+):/) {
			$machineID = $1;
		}
	}
	# another MiSeq file: 15092X7_180424_M00736_0255_MS6563328-300V2_R1.fastq.gz
	elsif ($file =~ m/^(\d{4,5}[xX]\d+)_\d+_([ADM]\d+)_\d+_[A-Z\d\-]+_R(\d)\.fastq\.gz$/) {
		$sample = $1;
		$machineID = $2;
		$pairedID = $3;
		$laneID = 1;
	}
	# crazy name: GUPTA_S1_L001_R1_001.fastq.gz
	elsif ($file =~ m/^(\w+)_S\d+_L(\d+)_R(\d)_00\d\.fastq\.gz$/) {
		$sample = $1;
		$laneID = $2;
		$pairedID = $3;
		# must grab the machine ID from the read name
		my $head = qx(gzip -dc $file | head -n 1);
		if ($head =~ /^@([ADM]\d+):/) {
			$machineID = $1;
		}
	}
	# crazy index: GUPTA_S1_L001_I1_001.fastq.gz
	elsif ($file =~ m/^(\w+)_S\d+_L(\d+)_I\d_00\d\.fastq\.gz$/) {
		$sample = $1;
		$laneID = $2;
		$pairedID = 3;
		# must grab the machine ID from the read name
		my $head = qx(gzip -dc $file | head -n 1);
		if ($head =~ /^@([ADM]\d+):/) {
			$machineID = $1;
		}
	}
	# undetermined file: Undetermined_S0_L001_R1_001.fastq.gz
	elsif ($file =~ m/^Undetermined_.+\.fastq\.gz$/) {
		$sample = 'undetermined';
		if ($file =~ m/_L(\d+)/) {
			$laneID = $1;
		}
		if ($file =~ m/_R([12])/) {
			$pairedID = $1;
		}
		# must grab the machine ID from the read name
		my $head = qx(gzip -dc $file | head -n 1);
		if ($head =~ /^@([ADM]\d+):/) {
			$machineID = $1;
		}
	}
	# I give up! catchall for other weirdo fastq files!!!
	elsif ($file =~ m/.+\.fastq\.gz$/i) {
		# I can't extract metadata information
		# but at least it will get recorded in the manifest and list files
		print "   ! processing unrecognized Fastq file $fname!\n";
		$sample = '';
		$laneID = '';
		$pairedID = '';
		$machineID = '';
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
	elsif ($file =~ m/^md5_.+\.txt$/) {
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
	elsif ($fname =~ /^\.\/Fastq\/.+\.(?:xml|csv)$/) {
		# other left over files from de-multiplexing
		print "   ! skipping demultiplexing file $fname!\n";
		return;
	}
	else {
		# programmer error!
		print "   ! unrecognized file $fname!\n";
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
	
	print "   > processed fastq file\n" if $verbose;
}


sub upload_files {
	
	### Grab missing information
	unless ($userfirst and $userlast and $strategy) {
		# this can be grabbed from the first data line in the manifest CSV
		my $fh = IO::File->new($manifest_file) or die "unable to read manifest file!";
		my $h = $fh->getline;
		my $l = $fh->getline;
		my @d = split(',', $l);
		$userfirst = $d[9];
		$userlast  = $d[10];
		$strategy  = $d[8];
		$fh->close;
	}
	
	### Initialize SB wrapper
	my $sb = SB->new(
		div     => $sb_division,
		sb      => $sb_path,
		cred    => $cred_path,
	) or die "unable to initialize SB wrapper module!";
	$sb->verbose(1) if $verbose;
	
	
	### Create the project on Seven Bridges
	my $sbproject;
	
	# check whether it exists
	foreach my $p ($sb->projects) {
		if ($p->name eq $project) {
			# we found it
			$sbproject = $p;
			printf "   > using existing SB project %s\n", $p->id;
			last;
		}
	}
	
	# create the project if it doesn't exist
	if (not defined $sbproject) {
		
		# check description
		if (not $description) {
			# generate simple description in markdown
			$description = sprintf "# %s\n## %s\n GNomEx project %s is a %s experiment generated by %s %s",
				$project, $title, $project, $strategy, $userfirst, $userlast;
			if ($group) {
				$description .= " in the group '$group'. ";
			}
			else {
				$description .= ". ";
			}
			$description .= sprintf("Details on the experiment may be found in [GNomEx](https://hci-bio-app.hci.utah.edu/gnomex/?requestNumber=%s).\n", 
				$project);
		}
		
		# create project
		$sbproject = $sb->create_project(
			name        => $project,
			description => $description,
		);
		if ($sbproject) {
			printf "   > created SB project %s\n", $sbproject->id;
		}
		else {
			print "   ! failed to make SB project!\n";
			return;
		}
	}
	
	
	### Upload the files
	# upload options
	my @up_options = ('--manifest-file', $manifest_file, '--manifest-metadata', 
					'sample_id', 'investigation', 'library_id', 'platform', 
					'platform_unit_id', 'paired_end', 'quality_scale', 
					'experimental_strategy', 'UserFirstName', 'UserLastName');
	
	# upload command
	my $path = $sbproject->bulk_upload_path($sbupload_path);
	unless ($path) {
		print "   ! no sbg-upload.sh executable path!\n";
		return;
	};
	my $result = $sbproject->bulk_upload(@up_options);
	print $result;
	if ($result =~ /FAILED/) {
		print "   ! upload failed!\n";
	}
	elsif ($result =~ /Done\.\n$/) {
		print "   > upload successful\n";
	}
	else {
		print "   ! upload error!\n";
	}
	return 1;
}


sub hide_deleted_files {
	
	# move the deleted files
	mkdir $deleted_folder;
	
	unless (@removelist) {
		# load the delete file contents
		my $fh = IO::File->new($alt_remove, 'r') or die "can't read $alt_remove! $!\n";
		while (my $line = $fh->getline) {
			chomp($line);
			push @removelist, $line;
		}
		$fh->close;
	}
	
	# process the removelist
	foreach my $file (@removelist) {
		next unless (-e $file);
		my (undef, $dir, $basefile) = File::Spec->splitpath($file);
		my $targetdir = File::Spec->catdir($deleted_folder, $dir);
		make_path($targetdir); 
			# this should safely skip existing directories
			# permissions and ownership inherit from user, not from source
			# return value is number of directories made, which in some cases could be 0!
		print "   moving $file\n" if $verbose;
		move($file, $targetdir) or print "   failed to move! $!\n";
	}
	
	# clean up empty directories
	my $command = sprintf("find %s -type d -empty -delete", $given_dir);
	print "  > executing: $command\n";
	print "    failed! $!\n" if (system($command));
	
	# move the deleted folder back into archive
	move($alt_remove, $remove_file) or print "   failed to move $alt_remove! $!\n";
	
	# put in notice
	if (not -e $notice_file and $notice_source_file) {
		$command = sprintf("ln -s %s %s", $notice_source_file, $notice_file);
		print "   ! failed to link notice file! $!\n" if (system($command));
	}
}


sub calculate_checksum {
	# calculate the md5 checksum
	my $file = shift;
	open (my $fh, '<', $file) or return 1;
		# if we can't open the file, just skip it and return a dummy value
		# we'll likely have more issues with this file later
	binmode ($fh);
	my $md5 = Digest::MD5->new->addfile($fh)->hexdigest;
	close $fh;
	return $md5;
}






