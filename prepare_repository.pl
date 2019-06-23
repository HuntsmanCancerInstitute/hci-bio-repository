#!/usr/bin/perl


use strict;
use File::Spec;
use File::Find;
use IO::File;
use File::Copy;
use POSIX qw(strftime);

# shortcut variable name to use in the find callback
use vars qw(*fname);
*fname = *File::Find::name;

# other predefined variables
use constant {
	SIXMOS   => 60 * 60 * 24 * 180,
	THREEMOS => 60 * 60 * 24 * 90,
	TYPE     => [qw(Other Analysis Alignment Fastq QC)],
};

my $VERSION = 4;

# Documentation
unless (@ARGV) {
	print <<DOC;
  A script to prepare Repository project directories for uploading to Amazon via 
  Seven Bridges.
  
  It will generate the following text files, where ID is project ID number.
    - ID_MANIFEST.txt
    - ID_ARCHIVE_LIST.txt
    - ID_UPLOAD_LIST.txt
    - ID_REMOVE_LIST.txt
    - ID_ARCHIVE.zip
  By default, they are stored in the project directory, or "hidden" by moving 
  them into the parent directory (where GNomEx doesn't look).
  
  It will zip files using the archive list as input. Zip compression is fastest. 
  Zip is performed with the -FS file sync option, so existing archives are updated.
    
  Execute under GNU parallel to speed up the process. Only one path per execution.
    sudo nohup /usr/local/bin/parallel -j 4 -a list.txt \
    $0 1 1 {} > out.txt
  
  
  Usage:  
    $0 <hide> <zip> <path>
    
    <hide>   Boolean 1 or 0 to hide or not hide the files in parent directory
    <zip>    Boolean 1 or 0 to zip the files
    <path>   Path of the project folder, example
               /Repository/MicroarrayData/2010/1234R
               /Repository/AnalysisData/2010/A5678
DOC
	exit;
}


# Passed variables
unless (scalar @ARGV == 3) {
	die "Must pass three options! $0 <hide> <zip> <path>\n";
}
my $hidden = shift @ARGV; 
my $to_zip = shift @ARGV;
my $given_dir = shift @ARGV;



# global arrays for storing file names
# these are needed since the File::Find callback doesn't accept pass through variables
my $start_time = time;
my @manifest;
my %file2manifest; # hash for processing existing manifest
my @ziplist;
my @uploadlist;
my @removelist;
my $youngest_age = 0;
my $project;


# extract the project ID
if ($given_dir =~ m/(A\d{1,5}|\d{3,5}R)\/?$/) {
	# look for A prefix or R suffix project identifiers
	# this ignores Request digit suffixes such as 1234R1, 
	# when clients submitted replacement samples
	$project = $1;
	print " working on $project at $given_dir\n";
}
elsif ($given_dir =~ m/(\d{2,4})\/?$/) {
	# old style naming convention without an A prefix or R suffix
	$project = $1;
	print " working on $project at $given_dir\n";
}
else {
	die "unable to identify project ID for $given_dir!\n";
}

# check directory
unless (-e $given_dir) {
	die "given path $given_dir does not exist!\n";
}


# check directory and move into the parent directory if we're not there
my $start_dir = './';
if ($given_dir =~ s/^(\/Repository\/(?:MicroarrayData|AnalysisData)\/\d{4})\///) {
	$start_dir = $1;
	# print " changing to $start_dir\n";
	chdir $start_dir or die "can not change to $start_dir!\n";
}



# prepare file names in project directory
my $manifest_file = File::Spec->catfile($given_dir, $project . "_MANIFEST.txt");
my $ziplist_file  = File::Spec->catfile($given_dir, $project . "_ARCHIVE_LIST.txt");
my $upload_file   = File::Spec->catfile($given_dir, $project . "_UPLOAD_LIST.txt");
my $remove_file   = File::Spec->catfile($given_dir, $project . "_REMOVE_LIST.txt");
my $zip_file      = File::Spec->catfile($given_dir, $project . "_ARCHIVE.zip");
# prepare hidden file names in parent directory
my $alt_manifest  = File::Spec->catfile($start_dir, $project . "_MANIFEST.txt");
my $alt_zip       = File::Spec->catfile($start_dir, $project . "_ARCHIVE.zip");
my $alt_ziplist   = File::Spec->catfile($start_dir, $project . "_ARCHIVE_LIST.txt");
my $alt_upload    = File::Spec->catfile($start_dir, $project . "_UPLOAD_LIST.txt");
my $alt_remove    = File::Spec->catfile($start_dir, $project . "_REMOVE_LIST.txt");

# unhide or remove existing files
if (-e $alt_zip) {
	move($alt_zip, $zip_file);
}
if (-e $alt_ziplist) {
	unlink($alt_ziplist);
}
if (-e $alt_upload) {
	unlink($alt_upload);
}
if (-e $alt_remove) {
	unlink($alt_remove);
}
if (-e $alt_manifest) {
	move($alt_manifest, $manifest_file);
}

# find existing manifest file
if (-e $manifest_file) {
	# we have a manifest file from a previous run
	load_manifest($manifest_file);
}



# search directory using File::Find 
find(\&callback, $given_dir);


# fill out the removelist based on age
if (time - $youngest_age < THREEMOS) {
	# keep everything here if date is less than three months
}
elsif (time - $youngest_age < SIXMOS) {
	# keep analysis and other files if date is less than six months
	foreach my $m (@manifest) {
		my @fields = split("\t", $m);
		next if ($fields[3] eq 'Alignment' or $fields[3] eq 'Fastq' or $fields[3] eq 'QC');
		push @removelist, $fields[4];
	}
}
else {
	# otherwise remove everything
	foreach my $m (@manifest) {
		my @fields = split("\t", $m);
		next if ($fields[3] eq 'Analysis' or $fields[3] eq 'QC');
		push @removelist, $fields[4];
	}
}

# add more files to list
push @uploadlist, $ziplist_file if @ziplist;
push @uploadlist, $zip_file if @ziplist;
push @uploadlist, $manifest_file if @uploadlist;


# zip archive
write_file($ziplist_file, \@ziplist) if @ziplist;
if (-e $ziplist_file) {
	my ($date, $size, $md5, $age) = get_file_stats($ziplist_file, $ziplist_file);
	push @manifest, join("\t", $md5, $size, $date, 'meta', $ziplist_file);
	
	# now create zip archive if requested
	if ($to_zip) {
		# we will zip zip with fastest compression for speed
		# use file sync option to add, update, and/or delete members in zip archive
		# regardless if it's present or new
		my $command = sprintf("cat %s | zip -1 -FS -@ %s", $ziplist_file, $zip_file);
		system($command);
		if (-e $zip_file) {
			my ($date, $size, $md5, $age) = get_file_stats($zip_file, $zip_file);
			push @manifest, join("\t", $md5, $size, $date, 'meta', $zip_file);
		}
	}
}


# write files
write_file($remove_file, \@removelist) if @removelist;
write_file($upload_file, \@uploadlist) if @uploadlist;
write_file($manifest_file, \@manifest);


# temporarily move files out of directory
if ($hidden) {
	move($manifest_file, $alt_manifest);
	move($ziplist_file, $alt_ziplist);
	move($upload_file, $alt_upload);
	move($remove_file, $alt_remove);
	move($zip_file, $alt_zip) if (-e $zip_file);
}

# finished
printf " finished with $project in %.1f minutes\n", (time - $start_time)/60;




sub callback {
	my $file = $_;
	
	# ignore certain files
	return unless -f $file; # skip directories and symlinks!
	if ($file =~ /\.sra$/i) {
		# what the hell are SRA files doing in here!!!????
		push @removelist, $file;
		return;
	}
	elsif ($file =~ /libsnappyjava|fdt\.jar/) {
		# devil java spawn, delete!!!!
		push @removelist, $file;
		return;
	}
	elsif ($file eq '.DS_Store' or $file eq 'Thumbs.db') {
		# Windows and Mac file browser devil spawn, just ignore these, or maybe delete?
		return;
	}
	if ($fname eq $zip_file) {
		return;
	}
	elsif ($fname eq $manifest_file) {
		return;
	}
	
	# stats on the file
	my ($date, $size, $md5, $age) = get_file_stats($file, $fname);
	
	
	# check age - we are comparing the time in seconds from epoch which is easier
	$youngest_age = $age if ($age > $youngest_age);
	
	# check file type
	my $keeper = 0;
	if ($file =~ /\.(?:bw|bigwig|bb|bigbed|useq)$/i) {
		# an indexed analysis file
		$keeper = 1;
	}
	elsif ($file =~ /\.vcf\.gz$/i) {
		# a vcf file, check to see if there is a corresponding tabix index
		my $i = $file . '.tbi';
		$keeper = 1 if -e $i;
	}
	elsif ($file =~ /\.tbi$/i) {
		# a tabix index file, presumably alongside a vcf file
		my $v = $file;
		$v =~ s/\.tbi$//i;
		if ($v =~ /\.vcf\.gz$/ and -e $v) {
			$keeper = 1;
		} else {
			# some other tabixed file, not necessarily a keeper
			$keeper = 0;
		}
	}
	elsif ($file =~ /\.(?:bam|bai|cram|crai|csi|sam\.gz)$/i) {
		# an alignment file
		$keeper = 2;
	}
	elsif (
		$file =~ m/^.*\d{4,5}[xX]\d+_.+_(?:sequence|sorted)\.txt\.gz(?:\.md5)?$/ or 
		$file =~ m/^\d{4,5}[xX]\d+_.+\.txt\.gz(?:\.md5)?$/ or 
		$file =~ m/\.(?:fastq|fq)(?:\.gz)?$/i
	) {
		# looks like a fastq file
		$keeper = 3;
	}
	elsif ($fname =~ /^$project\/(?:bioanalysis|Sample QC|Library QC)\//) {
		# these are QC samples in a bioanalysis or Sample of Library QC folder
		# directly under the main project 
		# hope there aren't any of these folders in Analysis!!!!!
		$keeper = 4;
	}
	
	# record the manifest information
	push @manifest, join("\t", $md5, $size, $date, TYPE->[$keeper], $fname);
	
	
	# record in appropriate lists
	if ($file =~ /\.txt$/i and not $keeper) {
		# all plain text files get zipped regardless of size
		push @ziplist, $fname;
	}
	elsif (int($size) < 100_000_000 and not $keeper) {
		# all small files under 100 MB get zipped
		push @ziplist, $fname;
	}
	else {
		# everything else except for sample quality files
		push @uploadlist, $fname unless $keeper == 4;
	}
	
}


sub write_file {
	my ($file, $data) = @_;
	
	# open file
	my $fh = IO::File->new($file, 'w') or 
		die "can't write to $file! $!\n";
	
	# write manifest header
	if ($file =~ /MANIFEST/) {
		$fh->print("MD5\tSize\tModification_Time\tType\tName\n");
	}
	
	# print out the data
	while (my $d = shift @$data) {
		$fh->print("$d\n");
	}
	$fh->close;
	
	# change permissions to rw-rw-rw- 
	chmod 0666, $file;
}


sub load_manifest {
	my $file = shift;
	
	# open file
	my $fh = IO::File->new($file, 'r') or 
		die "can't read $file! $!\n";
	
	# process
	while (my $line = $fh->getline) {
		chomp($line);
		my @bits = split('\t', $line);
		next if $bits[4] eq $manifest_file; # this may change
		next if $bits[4] eq $zip_file; # this may change
		$file2manifest{$bits[4]} = \@bits;
	}
	$fh->close;
}

sub get_file_stats {
	my ($file, $fname) = @_;
	
	# stats on the file
	my @st = stat($file);
	my $age = $st[9];
	my $date = strftime("%B %d, %Y %H:%M:%S", localtime($age));
	my $size = $st[7];
	my $md5;
	
	# check for existing file information from previous manifest file
	# make sure the date matches
	if (exists $file2manifest{$fname} and $date eq $file2manifest{$fname}->[2]) {
		$md5 = $file2manifest{$fname}->[0];
	}
	else {
		# we will have to calculate the md5 checksum
		if (-e "$file\.md5") {
			# looks like we have pre-calculated md5 file, so please use it
			my $fh = IO::File->new("$file\.md5");
			my $line = $fh->getline;
			($md5, undef) = split(/\s+/, $line);
			$fh->close;
		}
		else {
			# calculate the md5 with external utility
			my $checksum = `md5sum \"$file\"`; # quote file, because we have files with ;
			($md5, undef) = split(/\s+/, $checksum);
		}
	}
	
	# finished
	return ($date, $size, $md5, $age);
}
