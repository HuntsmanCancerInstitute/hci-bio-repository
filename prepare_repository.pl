#!/usr/bin/perl


use strict;
use IO::File;
use File::Spec;
use File::Find;
use File::Copy;
use File::Path qw(make_path);
use POSIX qw(strftime);
use Getopt::Long;

# shortcut variable name to use in the find callback
use vars qw(*fname);
*fname = *File::Find::name;

# other predefined variables
use constant {
	SIXMOS   => 60 * 60 * 24 * 180,
	THREEMOS => 60 * 60 * 24 * 90,
	TYPE     => [qw(Other Analysis Alignment Fastq QC ArchiveZipped)],
};

my $VERSION = 6.1;

# Documentation
my $doc = <<DOC;
  A script to prepare Repository project directories for uploading to Amazon via 
  Seven Bridges.
  
  It will generate the following text files, where ID is project ID number.
    - ID_MANIFEST.txt
    - ID_ARCHIVE_LIST.txt
    - ID_REMOVE_LIST.txt
    - ID_ARCHIVE.zip
  By default, they are stored in the project directory, or "hidden" by moving 
  them into the parent directory (where GNomEx doesn't look).
  
  It will optionally zip files using the archive list as input. Zip compression is 
  set to fastest (1) setting. Zip is performed with the -FS file sync option, so 
  existing archives will be updated. 
  
  It will optionally move the zip files to a hidden directory if the zip 
  archive exists.
  
  It will optionally move the to-be-removed files into a hidden directory.
  
  Execute under GNU parallel to speed up the process. Only one path per execution.
    sudo nohup /usr/local/bin/parallel -j 4 -a list.txt \
    $0 {} > out.txt
  
  Intended usage:
    1.  Execute with --scan --hide long before Snowball transfer
    2.  Immediately before Snowball transfer, re-run with --scan --zip --mvzip
    3.  Perform Snowball or aws transfer.
    4.  Chmod directories as "read only".
    5.  Chmod directories as writeable. Re-run with --mvdel option to "remove" 
        files. Chmod back to read only.
    5.  Clean up repository by deleting the "hidden" _ZIPPED_FILES and _DELETED_FILES
        directories. Only thing left are MANIFEST, ARCHIVE_LIST, and REMOVE_LIST 
        files, as well as bigWig, tabixed VCF files.
  
  Usage: $0 [options] <path>
  
  Options:  
    --scan    Scan the project directory for new files
    --hide    Boolean 1 or 0 to hide or not hide the files in parent directory
    --zip     Create and/or update the zip archive
    --mvzip   Move the zipped files out of the project folder to hidden folder 
    --mvdel   Move the to-delete files out of the project folder to hidden folder
    <path>    Path of the project folder, example
               /Repository/MicroarrayData/2010/1234R
               /Repository/AnalysisData/2010/A5678
DOC


# print help if no arguments
unless (@ARGV) {
	print $doc;
	exit;
}



# Process command line options
my $scan;
my $hidden; 
my $to_zip;
my $given_dir;
my $move_zip_files;
my $move_del_files;

if (scalar(@ARGV) > 1) {
	GetOptions(
		'scan!'     => \$scan,
		'hide!'     => \$hidden,
		'zip!'      => \$to_zip,
		'mvzip!'    => \$move_zip_files,
		'mvdel!'    => \$move_del_files,
	) or die "please recheck your options!\n\n$doc\n";
}
$given_dir = shift @ARGV;

# check options
die "must scan if you zip!\n" if ($to_zip and not $scan);
die "can't move zipped files if hidden!\n" if ($hidden and $move_zip_files);
die "can't move deleted files if hidden!\n" if ($hidden and $move_del_files);



# global arrays for storing file names
# these are needed since the File::Find callback doesn't accept pass through variables
my $start_time = time;
my @manifest;
my %file2manifest; # hash for processing existing manifest
my @ziplist;
# my @uploadlist;
my @removelist;
my $youngest_age = 0;
my $project;


# extract the project ID
if ($given_dir =~ m/(A\d{1,5}|\d{3,5}R)\/?$/) {
	# look for A prefix or R suffix project identifiers
	# this ignores Request digit suffixes such as 1234R1, 
	# when clients submitted replacement samples
	$project = $1;
	print " >working on $project at $given_dir\n";
}
elsif ($given_dir =~ m/(\d{2,4})\/?$/) {
	# old style naming convention without an A prefix or R suffix
	$project = $1;
	print " >working on $project at $given_dir\n";
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
if ($given_dir =~ s/^(\/Repository\/(?:MicroarrayData|AnalysisData)\/\d{4})\/?//) {
	# this also removes the base
	$start_dir = $1;
	print " >changing to $start_dir\n";
	chdir $start_dir or die "can not change to $start_dir!\n";
}
else {
	print " keeping start directory $start_dir\n";
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

# zipped file hidden folder
my $zipped_folder = File::Spec->catfile($start_dir, $project . "_ZIPPED_FILES");
if (-e $zipped_folder) {
	print "zipped files hidden folder already exists! Will not zip\n" if $to_zip;
	$to_zip = 0; # do not want to rezip
	print "zipped files hidden folder already exists! Will not move zipped files\n" if $move_zip_files;
	$move_zip_files = 0; # do not want to move zipped files
	print "cannot re-scan if zipped files hidden folder exists!\n" if $scan;
	$scan = 0;
}

# removed file hidden folder
my $deleted_folder = File::Spec->catfile($start_dir, $project . "_DELETED_FILES");
if (-e $deleted_folder) {
	print "deleted files hidden folder already exists! Will not move deleted files\n" if $move_del_files;
	$move_del_files = 0; # do not want to move zipped files
	print "cannot re-scan if deleted files hidden folder exists!\n" if $scan;
	$scan = 0;
}



# scan the project directory for new files
if ($scan) {
	print " >scanning $given_dir\n";
	# first unhide or remove existing files
	if (-e $alt_zip) {
		move($alt_zip, $zip_file);
	}
	if (-e $alt_ziplist) {
		unlink($alt_ziplist);
	}
	if (-e $ziplist_file) {
		unlink($ziplist_file);
	}
	if (-e $alt_upload) {
		unlink($alt_upload);
	}
	if (-e $alt_remove) {
		unlink($alt_remove);
	}
	if ($remove_file) {
		unlink($remove_file);
	}
	if (-e $alt_manifest) {
		move($alt_manifest, $manifest_file);
	}
	if (-e $upload_file) {
		# we are no longer using or writing upload files
		unlink($upload_file);
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
			next if ($fields[3] eq 'Alignment' or $fields[3] eq 'Fastq' or 
				$fields[3] eq 'QC' or $fields[3] eq 'ArchiveZipped');
			push @removelist, $fields[4];
		}
	}
	else {
		# otherwise remove everything
		foreach my $m (@manifest) {
			my @fields = split("\t", $m);
			next if ($fields[3] eq 'Analysis' or $fields[3] eq 'QC' or 
				$fields[3] eq 'ArchiveZipped');
			push @removelist, $fields[4];
		}
	}

	
	# write files
	write_file($ziplist_file, \@ziplist) if @ziplist;
	if (-e $ziplist_file) {
		my ($date, $size, $md5, $age) = get_file_stats($ziplist_file, $ziplist_file);
		push @manifest, join("\t", $md5, $size, $date, 'meta', $ziplist_file);
		
		# now create zip archive if requested
		if ($to_zip) {
			# we will zip zip with fastest compression for speed
			# use file sync option to add, update, and/or delete members in zip archive
			# regardless if it's present or new
			print " >zipping\n";
			my $command = sprintf("cat %s | zip -1 -FS -@ %s", $ziplist_file, $zip_file);
			print "  executing: $command\n";
			system($command);
			if (-e $zip_file) {
				my ($date, $size, $md5, $age) = get_file_stats($zip_file, $zip_file);
				push @manifest, join("\t", $md5, $size, $date, 'meta', $zip_file);
				push @removelist, $zip_file;
			}
		}
	}
	write_file($remove_file, \@removelist) if @removelist;
	write_file($manifest_file, \@manifest);
}




# temporarily move files out of directory
if ($hidden) {
	print " >hiding files\n";
	move($manifest_file, $alt_manifest) if (-e $manifest_file);
	move($ziplist_file, $alt_ziplist) if (-e $ziplist_file);
	move($zip_file, $alt_zip) if (-e $zip_file);
}
# but we always hide the remove list unless we've already removed
move($remove_file, $alt_remove) if (-e $remove_file and not -e $deleted_folder);




# move the zipped files
if ($move_zip_files and -e $ziplist_file) {
	print " >moving zipped files to $zipped_folder\n";
	die "no zip archive! Best not move!" if not -e $zip_file;
	
	# move the zipped files
	mkdir $zipped_folder;
	
	# load the ziplist file contents
	# I can't trust that we have a ziplist array in memory, so read it from file
	@ziplist = get_file_list($ziplist_file);
	
	# process the ziplist
	foreach my $file (@ziplist) {
		my (undef, $dir, $basefile) = File::Spec->splitpath($file);
		my $targetdir = File::Spec->catdir($zipped_folder, $dir);
		make_path($targetdir); # this should safely skip existing directories
						# permissions and ownership inherit from user, not from source
		move($file, $targetdir);
	}
	
	# clean up empty directories
	my $command = sprintf("find %s -type d -empty -delete", $given_dir);
	print "  executing: $command\n";
	system($command);
}




# move the deleted files
if ($move_del_files and -e $alt_remove) {
	print " >moving files to $deleted_folder\n";
	
	# move the deleted files
	mkdir $deleted_folder;
	
	# load the delete file contents
	# I can't trust that we have a removelist array in memory, so read it from file
	@removelist = get_file_list($alt_remove);
	
	# process the removelist
	foreach my $file (@removelist) {
		my (undef, $dir, $basefile) = File::Spec->splitpath($file);
		my $targetdir = File::Spec->catdir($deleted_folder, $dir);
		make_path($targetdir); # this should safely skip existing directories
						# permissions and ownership inherit from user, not from source
		move($file, $targetdir);
	}
	
	# clean up empty directories
	my $command = sprintf("find %s -type d -empty -delete", $given_dir);
	print "  executing: $command\n";
	system($command);
	
	# move the deleted folder back into archive
	move($alt_remove, $remove_file);
}




# finished
printf " >finished with $project in %.1f minutes\n", (time - $start_time)/60;



# find callback
sub callback {
	my $file = $_;
	
	# ignore certain files
	return unless -f $file; # skip directories and symlinks!
	if ($file =~ /\.sra$/i) {
		# what the hell are SRA files doing in here!!!????
		unlink $file;
		return;
	}
	elsif ($file =~ /libsnappyjava|fdt\.jar/) {
		# devil java spawn, delete!!!!
		unlink $file;
		return;
	}
	elsif ($file eq '.DS_Store' or $file eq 'Thumbs.db') {
		# Windows and Mac file browser devil spawn, delete these immediately
		unlink $file;
		return;
	}
	return if ($fname eq $zip_file);
	return if ($fname eq $manifest_file);
	return if ($fname eq $ziplist_file);
	return if ($fname eq $upload_file);
	
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
		$file =~ m/^\d{4,5}[xX]\d+_.+\.(?:txt|fastq)\.gz(?:\.md5)?$/ or 
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
	
	
	
	# record in appropriate lists
	if ($file =~ /\.txt$/i and not $keeper) {
		# all plain text files get zipped regardless of size
		push @ziplist, $fname;
		$keeper = 5;
	}
	elsif (int($size) < 100_000_000 and $keeper == 0) {
		# all small files under 100 MB get zipped
		push @ziplist, $fname;
		$keeper = 5;
	}
	elsif (int($size) < 100_000_000 and $keeper == 4) {
		# all bioanalysis files under 100MB get zipped
		push @ziplist, $fname;
	}
	
	# record the manifest information
	push @manifest, join("\t", $md5, $size, $date, TYPE->[$keeper], $fname);
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
	
	# change permissions to rw-r-r- 
	chmod 0644, $file;
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

sub get_file_list {
	my $file = shift;
	my $fh = IO::File->new($file, 'r') or 
		die "can't read $file! $!\n";
	
	# process
	my @list;
	while (my $line = $fh->getline) {
		chomp($line);
		push @list, $line;
	}
	$fh->close;
	return @list;
}




