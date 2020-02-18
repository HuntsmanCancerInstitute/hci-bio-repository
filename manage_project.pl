#!/usr/bin/perl

use strict;
use Getopt::Long;
use FindBin qw($Bin);
use lib $Bin;
use RepoProject;

my $version = 4.0;



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
    --check         Check inventoried files in manifest and list files
    --mvzip         Hide the zipped files by moving to hidden folder 
    --mvdel         Hide the to-delete files by moving to hidden folder
    --unhide        Unhide files, either _ZIPPED_FILES or _DELETED_FILES
    --delzip        Delete the hidden zipped files
    --delete        Delete the to-delete files from the project or hidden folder
    --notice        Symlink the notice file in the project folder
    --verbose       Tell me everything!

END



######## Process command line options
my $path;
my $move_zip_files;
my $move_del_files;
my $unhide_files;
my $delete_zip_files;
my $delete_del_files;
my $add_notice;
my $verbose = 0;

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
	$path = shift @ARGV;
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





####### Initiate Project

my $Project = RepoProject->new($path, $verbose) or 
	die "unable to initiate Repository Project!\n";
printf " > working on %s at %s\n", $Project->project, $Project->given_dir;
printf "   using parent directory %s\n", $Project->parent_dir if $verbose;

# change to the given directory
printf " > changing to %s\n", $Project->given_dir;
chdir $Project->given_dir or die "cannot change to given directory! $!\n";





######## Main operations 

my $failure_count = 0;

# unhide files
if ($unhide_files) {
	if (-e $Project->zipped_folder) {
		printf " > unhiding files for %s from directory %s to %s\n",
			$Project->project, $Project->zipped_folder, $Project->given_dir;
		$failure_count += $Project->unhide_zip_files;
	}
	if (-e $Project->deleted_folder) {
		printf " > unhiding files for %s from directory %s to %s\n",
			$Project->project, $Project->deleted_folder, $Project->given_dir;
		$failure_count += $Project->unhide_deleted_files;
	}
}


# hide the zipped files
if ($move_zip_files) {
	if (-e $Project->ziplist_file) {
		printf " > hiding zipped files to %s\n", $Project->zipped_folder;
		$failure_count += $Project->hide_zipped_files;
	}
	else {
		print " ! no zipped files to move\n";
	}
} 


# delete zipped files
if ($delete_zip_files) {
	if (-e $Project->zipped_folder) {
		printf " > deleting files in %s\n", $Project->zipped_folder;
		$failure_count +=  delete_zipped_file_folder();
	}
	else {
		print " ! zipped files not put into hidden zip folder, cannot delete!\n";
	}
} 


# move the deleted files
if ($move_del_files) {
	if (-e $Project->alt_remove_file) {
		print " > moving files to %s\n", $Project->deleted_folder;
		$failure_count +=  $Project->hide_deleted_files;
	}
	else {
		print " ! No deleted files to hide\n";
	}
} 


# actually delete the files
if ($delete_del_files) {
	if (-e $Project->deleted_folder and -e $Project->remove_file) {
		printf " > deleting files in hidden delete folder %s\n", $Project->deleted_folder;
		$failure_count += $Project->delete_hidden_deleted_files();
	}
	else  {
		printf " > deleting files in %s\n", $Project->given_dir;
		$failure_count += $Project->delete_project_files();
	}
}


# add notice file
if ($add_notice) {
	print " > Linking notice\n";
	$failure_count += $Project->add_notice_file;
}



######## Finished
printf " > finished with %s with %d failures in %.1f minutes\n\n", $Project->project, 
	$failure_count, (time - $start_time)/60;













