#!/usr/bin/perl

use strict;
use IO::File;
use File::Spec;
use File::Copy;
use File::Path qw(make_path);
use Getopt::Long;

my $version = 1.1;



######## Documentation
my $doc = <<END;

A script to manage GNomEx Analysis project folders. Specifically, managing 
the zipped, hidden, and deleted files associated with a project after 
uploading it to Seven Bridges. 

This does not scan folders or upload files. See scripts 
process_analysis_project.pl and process_request_project.pl.

Version: $version

Usage:
    manage_project.pl [options] /Repository/AnalysisData/2019/A5678

Options:

 Mode
    --mvzip         Hide the zipped files by moving to hidden folder 
    --mvdel         Hide the to-delete files by moving to hidden folder
    --unhide        Unhide files, either _ZIPPED_FILES or _DELETED_FILES
    --delzip        Delete the hidden zipped files
    --delete        Delete the to-delete files from the project or hidden folder
    --notice        Symlink the notice file in the project folder
    --verbose       Tell me everything!

END



######## Process command line options
my $given_dir;
my $move_zip_files;
my $move_del_files;
my $unhide_files;
my $delete_zip_files;
my $delete_del_files;
my $add_notice;
my $verbose;

if (scalar(@ARGV) > 1) {
	GetOptions(
		'mvzip!'    => \$move_zip_files,
		'mvdel!'    => \$move_del_files,
		'unhide!'   => \$unhide_files,
		'delzip!'   => \$delete_zip_files,
		'delete!'   => \$delete_del_files,
		'notice!'   => \$add_notice,
		'verbose!'  => \$verbose,
	) or die "please recheck your options!\n\n$doc\n";
	$given_dir = shift @ARGV;
}
else {
	print $doc;
	exit;
}




### check options

if ($unhide_files) {
	die "can't do anything else if unhiding files!\n" if $move_del_files or 
		$move_zip_files or $delete_del_files or $delete_zip_files;
}

my $start_time = time;





####### Check directories

# check directory
unless ($given_dir =~ /^\//) {
	die "given path does not begin with / Must use absolute paths!\n";
}
unless (-e $given_dir) {
	die "given path $given_dir does not exist!\n";
}

# extract the project ID
my $project;
if ($given_dir =~ m/(A\d{1,5}|\d{3,5}R)\/?$/) {
	# look for A prefix or R suffix project identifiers
	# this ignores Request digit suffixes such as 1234R1, 
	# when clients submitted replacement samples
	$project = $1;
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
print " > working on $project at $given_dir\n";


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
print " > changing to $given_dir\n";
chdir $given_dir or die "cannot change to $given_dir!\n";






####### Prepare file names

# file names in project directory
my $ziplist_file  = $project . "_ARCHIVE_LIST.txt";
my $upload_file   = $project . "_UPLOAD_LIST.txt";
my $remove_file   = $project . "_REMOVE_LIST.txt";
my $zip_file      = $project . "_ARCHIVE.zip";
my $notice_file   = "where_are_my_files.txt";

# hidden file names in parent directory
my $alt_zip       = File::Spec->catfile($parent_dir, $project . "_ARCHIVE.zip");
my $alt_ziplist   = File::Spec->catfile($parent_dir, $project . "_ARCHIVE_LIST.txt");
my $alt_upload    = File::Spec->catfile($parent_dir, $project . "_UPLOAD_LIST.txt");
my $alt_remove    = File::Spec->catfile($parent_dir, $project . "_REMOVE_LIST.txt");

if ($verbose) {
	print " =>  zip list file: $ziplist_file or $alt_ziplist\n";
	print " =>       zip file: $zip_file or $alt_zip\n";
	print " =>    remove file: $remove_file or $alt_remove\n";
}

# zipped file hidden folder
my $zipped_folder = File::Spec->catfile($parent_dir, $project . "_ZIPPED_FILES");
print " =>  zipped folder: $zipped_folder\n" if $verbose;
if (-e $zipped_folder) {
	print " ! zipped files hidden folder already exists! Will not move zipped files\n" if $move_zip_files;
	$move_zip_files = 0; # do not want to move zipped files
}

# removed file hidden folder
my $deleted_folder = File::Spec->catfile($parent_dir, $project . "_DELETED_FILES");
print " => deleted folder: $deleted_folder\n" if $verbose;
if (-e $deleted_folder) {
	print " ! deleted files hidden folder already exists! Will not move deleted files\n" if $move_del_files;
	$move_del_files = 0; # do not want to move zipped files
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






# unhide files
if ($unhide_files) {
	if (-e $zipped_folder) {
		print " > unhiding files for $project from directory $zipped_folder to $given_dir\n";
		if (-e $ziplist_file) {
			unhide_directory_files($zipped_folder, $ziplist_file);
		}
		else {
			die " no zip list file $ziplist_file, can't unhide!\n";
		}
	}
	if (-e $deleted_folder) {
		print " > unhiding files for $project from directory $deleted_folder to $given_dir\n";
		if (-e $remove_file) {
			unhide_directory_files($deleted_folder, $remove_file);
			move($remove_file, $alt_remove) or print "   failed to hide $remove_file! $!\n";
		}
		else {
			if (-e $alt_remove) {
				print "   wierd! no delete list file, but have alternate remove file! trying with that\n";
				unhide_directory_files($deleted_folder, $alt_remove);
			}
			else {
				die " no delete list files found!\n";
			}
		}
		
	}
}


# move the zipped files
if ($move_zip_files and -e $ziplist_file) {
	print " > moving zipped files to $zipped_folder\n";
	move_the_zip_files();
}
elsif ($move_zip_files and not -e $ziplist_file) {
	print " ! no zipped files to move\n";
}


# delete zipped files
if ($delete_zip_files and -e $zipped_folder) {
	print " > deleting files in $zipped_folder\n";
	delete_zipped_file_folder();
}
elsif ($delete_zip_files and not -e $zipped_folder) {
	print " ! zipped files not put into hidden zip folder, cannot delete!\n";
}


# move the deleted files
if ($move_del_files and -e $alt_remove) {
	print " > moving files to $deleted_folder\n";
	hide_deleted_files();
}
elsif ($move_del_files and not -e $alt_remove) {
	print " ! No deleted files to hide\n";
}


# actually delete the files
if ($delete_del_files and -e $deleted_folder and -e $remove_file) {
	print " > deleting files in hidden delete folder $deleted_folder\n";
	delete_hidden_deleted_files();
}
elsif ($delete_del_files and -e $alt_remove and not -e $deleted_folder) {
	print " > deleting files in $given_dir\n";
	delete_project_files($alt_remove);
}
elsif ($delete_del_files and -e $remove_file) {
	# this is unusual, it may be a processed folder, or a folder processed with 
	# a very early version of this script
	print " > folder appears processed, but attempting to delete files in $given_dir\n";
	delete_project_files($remove_file);
}
elsif ($delete_del_files) {
	print " ! no remove file list found to delete files\n";
}


# add notice file
if ($add_notice) {
	if (not -e $notice_file) {
		print " > Linking notice\n";
		my $command = sprintf("ln -s %s %s", $notice_source_file, $notice_file);
		print "    failed to link notice file! $!\n" if (system($command));
	}
}



######## Finished
printf " > finished with $project in %.1f minutes\n\n", (time - $start_time)/60;















####### Functions ##################



sub move_the_zip_files {
	die "no zip archive! Best not move!" if not -e $zip_file;
	
	# move the zipped files
	mkdir $zipped_folder;
	
	# load the ziplist file contents
	my @ziplist = get_file_list($ziplist_file);
	
	# process the ziplist
	foreach my $file (@ziplist) {
		unless (-e $file or -l $file) {
			# handle both files and possibly broken symlinks
			# older versions may record the project folder in the name, so let's 
			# try removing that
			$file =~ s/^$project\///;
			unless (-e $file or -l $file) {
				# give up if it's still not there
				print "   $file not found!\n";
				next;
			}
		}
		my (undef, $dir, $basefile) = File::Spec->splitpath($file);
		my $targetdir = File::Spec->catdir($zipped_folder, $dir);
		make_path($targetdir); 
			# this should safely skip existing directories
			# permissions and ownership inherit from user, not from source
			# return value is number of directories made, which in some cases could be 0!
		print "   moving $file\n" if $verbose;
		move($file, $targetdir) or print "   failed to move $file! $!";
	}
	
	# clean up empty directories
	my $command = sprintf("find %s -type d -empty -delete", $given_dir);
	print "  > executing: $command\n";
	print "    failed! $!\n" if (system($command));
	
	# put in notice
	if (not -e $notice_file and $notice_source_file) {
		$command = sprintf("ln -s %s %s", $notice_source_file, $notice_file);
		print "    failed to link notice file! $!\n" if (system($command));
	}
}

sub delete_zipped_file_folder {
	
	# load the ziplist file contents
	# I can't trust that we have a ziplist array in memory, so read it from file
	my @ziplist = get_file_list($ziplist_file);
	
	# process the ziplist
	foreach my $file (@ziplist) {
		my (undef, $dir, $basefile) = File::Spec->splitpath($file);
		my $targetfile = File::Spec->catfile($zipped_folder, $dir, $basefile);
		unless (-e $targetfile or -l $targetfile) {
			# handle both files and possibly broken symlinks
			# older versions may record the project folder in the name, so let's 
			# try removing that
			$targetfile =~ s/$project\///;
			next unless (-e $targetfile or -l $targetfile); # give up if it's still not there
		}
		print "   DELETING $targetfile\n" if $verbose;
		unlink($targetfile) or print "   failed to delete $targetfile! $!\n";
	}
	
	# clean up empty directories
	my $command = sprintf("find %s -type d -empty -delete", $zipped_folder);
	print "  > executing: $command\n";
	print "    failed! $!\n" if (system($command));
	if (-e $zipped_folder) {
		# the empty trim command above should take care of this, if not try again
		# it may not be empty!
		rmdir $zipped_folder or print "   failed to remove directory $zipped_folder! $!\n";
	}
}

sub hide_deleted_files {
	
	# move the deleted files
	mkdir $deleted_folder;
	
	# load the delete file contents
	# I can't trust that we have a removelist array in memory, so read it from file
	my @removelist = get_file_list($alt_remove);
	
	# process the removelist
	foreach my $file (@removelist) {
		my (undef, $dir, $basefile) = File::Spec->splitpath($file);
		unless (-e $file or -l $file) {
			# handle both files and possibly broken symlinks
			# older versions may record the project folder in the name, so let's 
			# try removing that
			$file =~ s/^$project\///;
			next unless (-e $file or -l $file); # give up if it's still not there
			(undef, $dir, $basefile) = File::Spec->splitpath($file); # regenerate these
		}
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
		print "    failed to link notice file! $!\n" if (system($command));
	}
}

sub unhide_directory_files {
	my ($hidden_dir, $hidden_list) = @_; # could be either zipped or deleted
	
	my @filelist = get_file_list($hidden_list);
	
	# process the removelist
	foreach my $file (@filelist) {
		my (undef, $dir, $basefile) = File::Spec->splitpath($file);
		my $hiddenfile = File::Spec->catdir($hidden_dir, $file);
		unless (-e $hiddenfile or -l $hiddenfile) {
			# handle both files and possibly broken symlinks
			# older versions may record the project folder in the name, so let's 
			# try removing that
			$file =~ s/^$project\///;
			$hiddenfile = File::Spec->catdir($hidden_dir, $file);
			next unless (-e $hiddenfile or -l $hiddenfile); 
				# give up if it's still not there
			(undef, $dir, $basefile) = File::Spec->splitpath($file); # regenerate these
		}
		my $targetdir = File::Spec->catdir($given_dir, $dir);
		make_path($targetdir); 
			# this should safely skip existing directories
			# permissions and ownership inherit from user, not from source
			# return value is number of directories made, which in some cases could be 0!
		print "   moving $hiddenfile to $targetdir\n" if $verbose;
		move($hiddenfile, $targetdir) or print "   failed to move $hiddenfile! $!\n";
	}
	
	# clean up empty directories
	my $command = sprintf("find %s -type d -empty -delete", $hidden_dir);
	print "  > executing: $command\n";
	print "    failed! $!\n" if (system($command));
	if (-e $hidden_dir) {
		# the empty trim command above should take care of this, if not try again
		# it may not be empty!
		rmdir $hidden_dir or print "   failed to remove directory $hidden_dir! $!\n";
	}
	
	# remove notice file
	if (-e $notice_file) {
		print "  > deleting notice file\n" if $verbose;
		unlink $notice_file;
	}
}

sub delete_hidden_deleted_files {
	
	# load the delete file contents
	my @removelist = get_file_list($remove_file);
	
	# process the removelist
	foreach my $file (@removelist) {
		my (undef, $dir, $basefile) = File::Spec->splitpath($file);
		my $targetfile = File::Spec->catfile($deleted_folder, $dir, $basefile);
		unless (-e $targetfile or -l $targetfile) {
			# handle both files and possibly broken symlinks
			# older versions may record the project folder in the name, so let's 
			# try removing that
			$dir =~ s/^$project\///;
			$targetfile = File::Spec->catfile($deleted_folder, $dir, $basefile);
			next unless (-e $targetfile or -l $targetfile); # give up if it's still not there
		}
		print "   DELETING $targetfile\n" if $verbose;
		unlink($targetfile) or print "   failed to remove $targetfile! $!\n";
	}
	
	# clean up empty directories
	my $command = sprintf("find %s -type d -empty -delete", $deleted_folder);
	print "  > executing: $command\n";
	print "    failed! $!\n" if (system($command));
	if (-e $deleted_folder) {
		# the empty trim command above should take care of this, if not try again
		# it may not be empty!
		rmdir $deleted_folder or print "   failed to remove directory $deleted_folder! $!\n";
	}
}

sub delete_project_files {
	my $listfile = shift;
	
	# load the delete file contents
	my @removelist = get_file_list($listfile);
	
	# process the removelist
	foreach my $file (@removelist) {
		unless (-e $file or -l $file) {
			# handle both files and possibly broken symlinks
			# older versions may record the project folder in the name, so let's 
			# try removing that
			$file =~ s/^$project\///;
			next unless (-e $file or -l $file); # give up if it's still not there
		}
		print "   DELETING $file\n" if $verbose;
		unlink $file or print "   failed to remove $file! $!\n";
	}
	
	# clean up empty directories
	my $command = sprintf("find %s -type d -empty -delete", $given_dir);
	print "  > executing: $command\n";
	print "    failed! $!\n" if (system($command));
	
	# move the deleted folder back into archive
	move($alt_remove, $remove_file) or print "   failed to move file $alt_remove! $!\n";
	
	# put in notice
	if (not -e $notice_file and $notice_source_file) {
		$command = sprintf("ln -s %s %s", $notice_source_file, $notice_file);
		print "    failed to link notice file! $!\n" if (system($command));
	}
}

sub get_file_list {
	my $file = shift;
	print "  > loading list file $file\n" if $verbose;
	
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









