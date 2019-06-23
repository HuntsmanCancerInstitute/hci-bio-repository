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

my $VERSION = 7.1;

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
  
  It can optionally zip files using the archive list as input. Zip compression is 
  set to fastest (1) setting. Zip is performed with the -FS file sync option, so 
  existing archives will be updated. 
  
  It can optionally move the zipped files to a hidden directory if the zip 
  archive exists.
  
  It can optionally move the to-be-removed files into a hidden directory.
  
  It can optionally clean up the zipped and removed hidden directory, or the 
  project directory.
  
  File rules:
  	Fastq, Sam, Bam, bigWig, bigBed, USeq, and tabixed VCF files are never zip archived.
  	Files under 100 MB are zip archived.
  	BigWig and tabixed VCF files are not deleted unless --all is set
  	Sample and Library QC files are archived and not deleted.
  	SRA files, .DS_Store, .thumbsdb files are immediately deleted and never reported.
  
  
  Execute under GNU parallel to speed up the process. Only one path per execution.
    sudo nohup /usr/local/bin/parallel -j 4 -a list.txt \\
    $0 {} > out.txt
  
  Intended usage:
    1.  Execute with --scan --hide long before Snowball transfer
    2.  Immediately before Snowball transfer, re-run with --scan --zip --mvzip
    3.  Perform Snowball or aws transfer.
    4.  Chmod directories as "read only".
    5.  Chmod directories as writeable. Re-run with --mvdel option to "remove" 
        files. Alternatively, run with --delete to immediately remove. 
        Chmod back to read only.
    6.  Clean up repository by deleting the "hidden" _ZIPPED_FILES and _DELETED_FILES
        directories by running with --delzip and --delete. Only files left are 
        MANIFEST, ARCHIVE_LIST, and REMOVE_LIST files, and some indexed analysis files.
  
  
  Usage: $0 [options] <path>
  
  Options:  
    --scan    Scan the project directory for new files
    --noalist Do not write an archive list when scanning
    --nodlist Do not write a delete list when scanning
    --zip     Create and/or update the zip archive when scanning
    --hide    Hide the list files in parent directory
    --mvzip   Move the zipped files out of the project folder to hidden parent folder 
    --mvdel   Move the to-delete files out of the project folder to hidden parent folder
    --delzip  Delete the hidden zipped files
    --delete  Delete the to-delete files from the project or hidden folder
    --all     Delete everything including Analysis files
    --verbose Tell me everything!
    <path>    Path of the project folder from root, example
               /Repository/MicroarrayData/2010/1234R
               /Repository/AnalysisData/2010/A5678
DOC


# print help if no arguments
unless (@ARGV) {
	print $doc;
	exit;
}






######## Process command line options
my $scan;
my $write_delete_list;
my $write_zip_list;
my $hidden; 
my $to_zip;
my $given_dir;
my $move_zip_files;
my $move_del_files;
my $delete_zip_files;
my $delete_del_files;
my $everything;
my $verbose;

if (scalar(@ARGV) > 1) {
	GetOptions(
		'scan!'     => \$scan,
		'alist!'    => \$write_zip_list,
		'dlist!'    => \$write_delete_list,
		'hide!'     => \$hidden,
		'zip!'      => \$to_zip,
		'mvzip!'    => \$move_zip_files,
		'mvdel!'    => \$move_del_files,
		'delzip!'   => \$delete_zip_files,
		'delete!'   => \$delete_del_files,
		'all!'      => \$everything,
		'verbose!'  => \$verbose,
	) or die "please recheck your options!\n\n$doc\n";
}
$given_dir = shift @ARGV;

### check options
if ($scan) {
	$write_zip_list = 1 if not defined $write_zip_list;
	$write_delete_list = 1 if not defined $write_delete_list;
}
die "must scan if you zip!\n" if ($to_zip and not $scan);
die "can't move zipped files if hidden!\n" if ($hidden and $move_zip_files);
die "can't move deleted files if hidden!\n" if ($hidden and $move_del_files);
die "can't both move deleted files and delete them!\n" if ($move_del_files and $delete_del_files);







######## Global variables
# these are needed since the File::Find callback doesn't accept pass through variables
my $start_time = time;
my @manifest;
my %file2manifest; # hash for processing existing manifest
my @ziplist;
my @removelist;
my $youngest_age = 0;
my $project;






####### Check directories

# check directory
unless ($given_dir =~ /^\//) {
	die "given path does not begin with / Must use absolute paths!\n";
}
unless (-e $given_dir) {
	die "given path $given_dir does not exist!\n";
}

# extract the project ID
if ($given_dir =~ m/(A\d{1,5}|\d{3,5}R)\/?$/) {
	# look for A prefix or R suffix project identifiers
	# this ignores Request digit suffixes such as 1234R1, 
	# when clients submitted replacement samples
	$project = $1;
	print " > working on $project at $given_dir\n";
}
elsif ($given_dir =~ m/(\d{2,4})\/?$/) {
	# old style naming convention without an A prefix or R suffix
	$project = $1;
	print " > working on $project at $given_dir\n";
}
else {
	die "unable to identify project ID for $given_dir!\n";
}


# check directory and move into the parent directory if we're not there
my $parent_dir = './';
if ($given_dir =~ m/^(\/Repository\/(?:MicroarrayData|AnalysisData)\/\d{4})\/?/) {
	$parent_dir = $1;
}
elsif ($given_dir =~ m/^(.+)$project\/?$/) {
	$parent_dir = $1;
}
print " >> using parent directory $parent_dir\n" if $verbose;

# change to the given directory
print " > changing to $given_dir\n";
chdir $given_dir or die "cannot change to $given_dir!\n";






####### Prepare file names

# file names in project directory
my $manifest_file = $project . "_MANIFEST.txt";
my $ziplist_file  = $project . "_ARCHIVE_LIST.txt";
my $upload_file   = $project . "_UPLOAD_LIST.txt";
my $remove_file   = $project . "_REMOVE_LIST.txt";
my $zip_file      = $project . "_ARCHIVE.zip";
my $notice_file   = "where_are_my_files.txt";

# hidden file names in parent directory
my $alt_manifest  = File::Spec->catfile($parent_dir, $project . "_MANIFEST.txt");
my $alt_zip       = File::Spec->catfile($parent_dir, $project . "_ARCHIVE.zip");
my $alt_ziplist   = File::Spec->catfile($parent_dir, $project . "_ARCHIVE_LIST.txt");
my $alt_upload    = File::Spec->catfile($parent_dir, $project . "_UPLOAD_LIST.txt");
my $alt_remove    = File::Spec->catfile($parent_dir, $project . "_REMOVE_LIST.txt");

if ($verbose) {
	print " =>  manifest file: $manifest_file or $alt_manifest\n";
	print " =>  zip list file: $ziplist_file or $alt_ziplist\n";
	print " =>       zip file: $zip_file or $alt_zip\n";
	print " =>    remove file: $remove_file or $alt_remove\n";
}

# zipped file hidden folder
my $zipped_folder = File::Spec->catfile($parent_dir, $project . "_ZIPPED_FILES");
print " =>  zipped folder: $zipped_folder\n" if $verbose;
if (-e $zipped_folder) {
	print "zipped files hidden folder already exists! Will not zip\n" if $to_zip;
	$to_zip = 0; # do not want to rezip
	print "zipped files hidden folder already exists! Will not move zipped files\n" if $move_zip_files;
	$move_zip_files = 0; # do not want to move zipped files
	print "cannot re-scan if zipped files hidden folder exists!\n" if $scan;
	$scan = 0;
}

# removed file hidden folder
my $deleted_folder = File::Spec->catfile($parent_dir, $project . "_DELETED_FILES");
print " => deleted folder: $deleted_folder\n" if $verbose;
if (-e $deleted_folder) {
	print "deleted files hidden folder already exists! Will not move deleted files\n" if $move_del_files;
	$move_del_files = 0; # do not want to move zipped files
	print "cannot re-scan if deleted files hidden folder exists!\n" if $scan;
	$scan = 0;
}

# notification file
my $notice_source_file;
if ($given_dir =~ /MicroarrayData/) {
	$notice_source_file = "/Repository/MicroarrayData/missing_file_notice.txt";
}
elsif ($given_dir =~ /AnalysisyData/) {
	$notice_source_file = "/Repository/AnalysisyData/missing_file_notice.txt";
}
else {
	# primarily for testing purposes
	$notice_source_file = "~/missing_file_notice.txt";
}






######## Main processing ###############

# scan the directory
if ($scan) {
	# this will also run the zip function
	print " > scanning $project in directory $given_dir\n";
	scan_directory();
}


# move the zipped files
if ($move_zip_files and -e $ziplist_file) {
	print " > moving zipped files to $zipped_folder\n";
	move_the_zip_files();
}
elsif ($move_zip_files and not -e $ziplist_file) {
	print " > no zipped files to move\n";
}


# delete zipped files
if ($delete_zip_files and -e $zipped_folder) {
	print " > deleting files in $zipped_folder\n";
	delete_zipped_file_folder();
}
elsif ($delete_zip_files and not -e $zipped_folder) {
	print " > zipped files not put into hidden zip folder, cannot delete!\n";
}


# move the deleted files
if ($move_del_files and -e $alt_remove) {
	print " > moving files to $deleted_folder\n";
	hide_deleted_files();
}
elsif ($move_del_files and not -e $alt_remove) {
	print " > No deleted files to hide\n";
}


# actually delete the files
if ($delete_del_files and -e $deleted_folder and -e $remove_file) {
	print " > deleting files in hidden delete folder $deleted_folder\n";
	delete_hidden_deleted_files();
}
elsif ($delete_del_files and -e $alt_remove and not -e $deleted_folder) {
	print " > deleting files in $given_dir\n";
	delete_project_files();
}
elsif ($delete_del_files) {
	print " > nothing found to delete\n";
}






######## Finished
printf " > finished with $project in %.1f minutes\n", (time - $start_time)/60;















####### Functions ##################

sub scan_directory {
	### first unhide or remove existing files
	if (-e $alt_zip) {
		print " >> moving $zip_file to $alt_zip\n" if $verbose;
		move($alt_zip, $zip_file);
	}
	if (-e $alt_ziplist) {
		print " >> removing $alt_ziplist\n" if $verbose;
		unlink($alt_ziplist);
	}
	if (-e $ziplist_file) {
		print " >> removing $ziplist_file\n" if $verbose;
		unlink($ziplist_file);
	}
	if (-e $alt_upload) {
		print " >> removing $alt_upload\n" if $verbose;
		unlink($alt_upload);
	}
	if (-e $alt_remove) {
		print " >> removing $alt_remove\n" if $verbose;
		unlink($alt_remove);
	}
	if ($remove_file) {
		print " >> removing $remove_file\n" if $verbose;
		unlink($remove_file);
	}
	if (-e $alt_manifest) {
		print " >> moving $alt_manifest to $manifest_file\n" if $verbose;
		move($alt_manifest, $manifest_file);
	}
	if (-e $upload_file) {
		# we are no longer using or writing upload files
		unlink($upload_file);
	}

	
	
	### Load existing manifest file
	if (-e $manifest_file) {
		# we have a manifest file from a previous run, so reuse the data
		load_manifest($manifest_file);
	}

	
	
	### search directory recursively using File::Find 
	# remember that we are in the project directory, so we search current directory ./
	# results from the recursive search are processed with callback() function and 
	# written to global variables - the callback doesn't support passed data 
	find(\&callback, '.');
	
	
	
	### fill out the removelist based on age
	if (time - $youngest_age < SIXMOS) {
		# keep everything here if date is less than six months
		print " >> youngest file is less than 6 months\n" if $verbose;
	}
	else {
		# otherwise remove everything
		print " >> youngest file is more than 6 months\n" if $verbose;
		foreach my $m (@manifest) {
			my @fields = split("\t", $m);
			next if ($fields[3] eq 'ArchiveZipped');
			next if ($fields[3] eq 'Analysis' and not $everything);
			push @removelist, $fields[4];
		}
	}
	printf " >> identified %d files to remove\n", scalar(@removelist) if $verbose;

	
	
	### Generate zip file list and zip archive
	if (@ziplist and $write_zip_list) {
		write_file($ziplist_file, \@ziplist);
		if (-e $ziplist_file) {
			my ($date, $size, $md5, $age) = get_file_stats($ziplist_file, $ziplist_file);
			push @manifest, join("\t", $md5, $size, $date, 'meta', $ziplist_file);
		
			# now create zip archive if requested
			if ($to_zip) {
				# we will zip zip with fastest compression for speed
				# use file sync option to add, update, and/or delete members in zip archive
				# regardless if it's present or new
				print " > zipping files\n";
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
	}
	
	
	
	### Write remaining files
	write_file($remove_file, \@removelist) if (scalar(@removelist) and $write_delete_list);
	write_file($manifest_file, \@manifest);

	
	
	### Hide files if requested
	if ($hidden) {
		print " > hiding files\n";
		move($manifest_file, $alt_manifest) if (-e $manifest_file);
		move($ziplist_file, $alt_ziplist) if (-e $ziplist_file);
		move($zip_file, $alt_zip) if (-e $zip_file);
	}

	# always hide the remove list
	if ($scan and -e $remove_file and not -e $deleted_folder) {
		move($remove_file, $alt_remove);
	}
}


# find callback
sub callback {
	my $file = $_;
	print " >> find callback on $file for $fname\n" if $verbose;

	
	### Ignore certain files
	return unless -f $file; # skip directories and symlinks!
	if ($file =~ /\.sra$/i) {
		# what the hell are SRA files doing in here!!!????
		print " >>   deleting SRA file\n" if $verbose;
		unlink $file;
		return;
	}
	elsif ($file =~ /libsnappyjava|fdt\.jar/) {
		# devil java spawn, delete!!!!
		print " >>   deleting java file\n" if $verbose;
		unlink $file;
		return;
	}
	elsif ($file eq '.DS_Store' or $file eq 'Thumbs.db') {
		# Windows and Mac file browser devil spawn, delete these immediately
		print " >>   deleting file browser metadata file\n" if $verbose;
		unlink $file;
		return;
	}
	return if ($file eq $zip_file);
	return if ($file eq $manifest_file);
	return if ($file eq $ziplist_file);
	return if ($file eq $upload_file);
	return if ($file eq $notice_file);
	print " >>   file doesn't match one of my repository preparation files\n" if $verbose;
	
	
	
	### Get stats on the file
	my ($date, $size, $md5, $age) = get_file_stats($file, $fname);
	# check age - we are comparing the time in seconds from epoch which is easier
	$youngest_age = $age if ($age > $youngest_age);
	
	
	
	### Check file type
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
	elsif ($fname =~ m/^.\/(?:bioanalysis|Sample QC|Library QC)\//) {
		# these are QC samples in a bioanalysis or Sample of Library QC folder
		# directly under the main project 
		# hope there aren't any of these folders in Analysis!!!!!
		$keeper = 4;
	}
	
	
	
	### Record file name in appropriate lists
	# generate a clean name for recording
	my $clean_name = $fname;
	$clean_name =~ s/^\.\///; # strip the beginning ./ from the name to clean it up
	
	if ($file =~ /\.txt$/i and not $keeper) {
		# all plain text files get zipped regardless of size
		push @ziplist, $clean_name;
		$keeper = 5;
	}
	elsif (int($size) < 100_000_000 and $keeper == 0) {
		# all small files under 100 MB get zipped
		push @ziplist, $clean_name;
		$keeper = 5;
	}
	elsif (int($size) < 100_000_000 and $keeper == 4) {
		# all bioanalysis files under 100MB get zipped
		push @ziplist, $clean_name;
	}
	
	# record the manifest information
	printf " >>   file is a %s file\n", TYPE->[$keeper] if $verbose;
	push @manifest, join("\t", $md5, $size, $date, TYPE->[$keeper], $clean_name);
}

sub move_the_zip_files {
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
	
	# put in notice
	if (not -e $notice_file and $notice_source_file) {
		$command = sprintf("ln -s %s %s", $notice_source_file, $notice_file);
		system($command);
	}
}

sub delete_zipped_file_folder {
	
	# load the ziplist file contents
	# I can't trust that we have a ziplist array in memory, so read it from file
	@ziplist = get_file_list($ziplist_file);
	
	# process the ziplist
	foreach my $file (@ziplist) {
		my (undef, $dir, $basefile) = File::Spec->splitpath($file);
		my $targetfile = File::Spec->catfile($zipped_folder, $dir, $basefile);
		print " >> DELETING $targetfile\n" if $verbose;
		unlink($targetfile);
	}
	
	# clean up empty directories
	my $command = sprintf("find %s -type d -empty -delete", $zipped_folder);
	print "  executing: $command\n";
	system($command);
	rmdir $zipped_folder;
}

sub hide_deleted_files {
	
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
	
	# put in notice
	if (not -e $notice_file and $notice_source_file) {
		$command = sprintf("ln -s %s %s", $notice_source_file, $notice_file);
		system($command);
	}
}

sub delete_hidden_deleted_files {
	
	# load the delete file contents
	# I can't trust that we have a removelist array in memory, so read it from file
	@removelist = get_file_list($remove_file);
	
	# process the removelist
	foreach my $file (@removelist) {
		my (undef, $dir, $basefile) = File::Spec->splitpath($file);
		my $targetfile = File::Spec->catfile($deleted_folder, $dir, $basefile);
		print " >> DELETING $targetfile\n" if $verbose;
		unlink($targetfile);
	}
	
	# clean up empty directories
	my $command = sprintf("find %s -type d -empty -delete", $deleted_folder);
	print "  executing: $command\n";
	system($command);
	rmdir $deleted_folder;
}

sub delete_project_files {
	
	# load the delete file contents
	# I can't trust that we have a removelist array in memory, so read it from file
	@removelist = get_file_list($alt_remove);
	
	# process the removelist
	foreach my $file (@removelist) {
		print " >> DELETING $file\n" if $verbose;
		unlink $file;
	}
	
	# clean up empty directories
	my $command = sprintf("find %s -type d -empty -delete", $given_dir);
	print "  executing: $command\n";
	system($command);
	
	# move the deleted folder back into archive
	move($alt_remove, $remove_file);
	
	# put in notice
	if (not -e $notice_file and $notice_source_file) {
		$command = sprintf("ln -s %s %s", $notice_source_file, $notice_file);
		system($command);
	}
}

sub write_file {
	my ($file, $data) = @_;
	print " >> writing file $file\n" if $verbose;
	
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
	print " >> loading manifest file $file\n" if $verbose;
	
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
	print " >> getting file stats on $file....\n" if $verbose;
	
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
	print " >>  $file: $size bytes, $age seconds, from $date, checksum $md5\n" if $verbose;
	return ($date, $size, $md5, $age);
}

sub get_file_list {
	my $file = shift;
	print " >> loading list file $file\n" if $verbose;
	
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




