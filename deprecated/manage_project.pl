#!/usr/bin/perl

use strict;
use Getopt::Long;
use IO::File;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use RepoProject;
use RepoCatalog;

my $version = 5;



######## Documentation
my $doc = <<END;

A script to manage GNomEx Analysis project folders. Specifically, managing 
the zipped, hidden, and deleted files associated with a project after 
uploading it to Seven Bridges. 

This does not scan folders or upload files. See scripts 
process_analysis_project.pl and process_request_project.pl.

This script handles both project paths as well as identifiers in a Repository 
Catalog file database. If a Catalog file is used, pertinent time stamps are 
updated as appropriate. Multiple projects can be entered following the command, 
or provided as a list in a file. The file is assumed to have a header, and the 
project is the first column.

More than one operation may be provided.

Version: $version

Usage:
    manage_project.pl [options] /Repository/AnalysisData/2019/A5678
    manage_project.pl --cat requests.db 1234R...

Options:
 
 Input
    --cat <file>    Catalog file of projects
    --list <file>   File list of project paths/identifiers, assumes header
 
 Mode
    --mvzip         Hide the zipped files by moving to hidden folder 
    --mvdel         Hide the to-delete files by moving to hidden folder
    --showzip       Unhide files from _ZIPPED_FILES folder
    --showdel       Unhide files from _DELETED_FILES folder
    --restorezip    Restore files from the zip archive file
    --delzip        Delete the hidden zipped files
    --delete        Delete the to-delete files from the project and/or hidden folder
    --notice        Symlink the notice file into the project folder
    --clean         Safely remove manifest and list files ONLY
    
 General
    --verbose       Tell me everything!

END



######## Process command line options
my @projects;
my $cat_file;
my $list_file;
my $move_zip_files;
my $move_del_files;
my $unhide_files;
my $unhide_zip_files;
my $unhide_del_files;
my $restore_zip_files;
my $delete_zip_files;
my $delete_del_files;
my $add_notice;
my $clean_project_files;
my $verbose = 0;

if (scalar(@ARGV) > 1) {
	GetOptions(
		'cat=s'       => \$cat_file,
		'list=s'      => \$list_file,
		'mvzip!'      => \$move_zip_files,
		'mvdel!'      => \$move_del_files,
		'unhide!'     => \$unhide_files,
		'showzip!'    => \$unhide_zip_files,
		'showdel!'    => \$unhide_del_files,
		'restorezip!' => \$restore_zip_files,
		'delzip!'     => \$delete_zip_files,
		'delete!'     => \$delete_del_files,
		'notice!'     => \$add_notice,
		'clean!'      => \$clean_project_files,
		'verbose!'    => \$verbose,
	) or die "please recheck your options!\n\n$doc\n";
}
else {
	print $doc;
	exit;
}


### Project list
if (@ARGV) {
	# left over projects
	@projects = @ARGV;
}
elsif ($list_file) {
	my $fh = IO::File->new($list_file) or 
		die "Cannot open import file '$list_file'! $!\n";
	my $header = $fh->getline;
	while (my $l = $fh->getline) {
		chomp $l;
		my @bits = split m/\s+/, $l;
		push @projects, $bits[0];
	}
	printf " loaded %d lines from $list_file\n", scalar(@projects);
	$fh->close;
}
else {
	die "No lists of project identifiers or paths provided!\n";
}


### check options
if ($unhide_files) {
	# legacy option
	$unhide_zip_files = 1;
	$unhide_del_files = 1;
}

if ($unhide_zip_files or $unhide_del_files) {
	die "can't do anything else if unhiding files!\n" if $move_del_files or 
		$move_zip_files or $delete_del_files or $delete_zip_files;
}

my $start_time = time;




####### Initiate Catalog if present
my $Catalog;
if ($cat_file) {
	$Catalog = RepoCatalog->new($cat_file) or 
		die "Cannot open catalog file '$cat_file'!\n";
}





####### Loop through projects
foreach my $identifier (@projects) {
	
	
	### Obtain the file path to the given project
	my $path;
	my $Entry; # catalog entry for the project for updating later
	if (-e $identifier and -d _) {
		# looks like a path
		$path = $identifier;
	}
	elsif ($Catalog) {
		# check the catalog
		$Entry = $Catalog->entry($identifier);
		if ($Entry) {
			$path = $Entry->path;
		}
		else {
			print " ! Identifier $identifier is not in the Catalog file! skipping\n";
			next;
		}
	}
	else {
		print " ! Identifier $identifier is not a legitimate project path! skipping\n";
	}
	

	### Initiate the project
	my $Project = RepoProject->new($path, $verbose);
	unless ($Project) { 
		print  " ! unable to initiate Repository Project for path $path!\n";
		next;
	}
	printf " > working on %s at %s\n", $Project->project, $Project->given_dir;
	printf "   using parent directory %s\n", $Project->parent_dir if $verbose;

	# change to the given directory
	printf " > changing to %s\n", $Project->given_dir;
	chdir $Project->given_dir or die "cannot change to given directory! $!\n";
	
	
	#### Main operations 
	
	my $failure_count = 0;
	
	# unhide files
	if ($unhide_zip_files) {
		if (-e $Project->zip_folder) {
			printf " > unhiding %s zipped files from directory %s to %s\n",
				$Project->project, $Project->zip_folder, $Project->given_dir;
			$failure_count += $Project->unhide_zip_files;
		}
		else {
			printf " ! Zip folder %s doesn't exist!\n", $Project->zip_folder;
		}
	}
	
	if ($unhide_del_files) {
		if (-e $Project->delete_folder) {
			printf " > unhiding %s deleted files from directory %s to %s\n",
				$Project->project, $Project->delete_folder, $Project->given_dir;
			my $failure = $Project->unhide_deleted_files;
			if ($Entry and not $failure) {
				if ($Entry->hidden_datestamp) {
					$Entry->hidden_datestamp(0);
					print "    updated catalog hidden timestamp\n";
				}
			}
			$failure_count += $failure;
		}
		else {
			printf " ! Deleted folder %s doesn't exist!\n", $Project->delete_folder;
		}
	}
	
	
	# hide the zipped files
	if ($move_zip_files) {
		if (-e $Project->ziplist_file) {
			printf " > hiding %s zipped files to %s\n", $Project->project, 
				$Project->zip_folder;
			$failure_count += $Project->hide_zipped_files;
		}
		else {
			printf " ! %s has no zipped files to move\n", $Project->project;
		}
	} 
	
	
	# delete zipped files
	if ($delete_zip_files) {
		if (-e $Project->zip_folder) {
			printf " > deleting %s zipped files in %s\n", $Project->project, 
				$Project->zip_folder;
			$failure_count += $Project->delete_zipped_files_folder;
		}
		else {
			printf " ! %s zipped files not put into hidden zip folder, cannot delete!\n", 
				$Project->project;
		}
	} 
	
	
	# restore the zip archive
	if ($restore_zip_files) {
		if (-e $Project->zip_file) {
			if (-e $Project->zip_folder) {
				printf " ! %s hidden zip folder %s exists!\n", $Project->project, 
					$Project->zip_folder;
				print  "    Restore from the zip folder\n";
				$failure_count++;
			}
			else {
				printf " > Restoring %s files from zip archive %s\n", $Project->project,
					$Project->zip_file;
				chdir $Project->given_dir;
				my $command = sprintf "unzip -n %s", $Project->zip_file;
				print  "  > executing: $command\n";
				my $result = system($command);
				if ($result) {
					print "     failed!\n";
					$failure_count++;
				}
			}
		}
		else {
			printf " ! %s Zip archive not present!\n", $Project->project;
		}
	}
	
	
	# move the deleted files
	if ($move_del_files) {
		if (-e $Project->alt_remove_file) {
			printf " > hiding %s deleted files to %s\n", $Project->project, 
				$Project->delete_folder;
			my $failure = $Project->hide_deleted_files;
			if ($Entry and not $failure) {
				$Entry->hidden_datestamp(time);
				print "    updated catalog hidden timestamp\n";
			}
			$failure_count += $failure;
		}
		else {
			printf " ! %s has no deleted files to hide!\n", $Project->project;
		}
	} 
	
	
	# actually delete the files
	if ($delete_del_files) {
		if (-e $Project->delete_folder and -e $Project->remove_file) {
			printf " > deleting %s files in hidden delete folder %s\n", $Project->project, 
				$Project->delete_folder;
			my $failure = $Project->delete_hidden_deleted_files();
			if ($Entry and not $failure) {
				$Entry->deleted_datestamp(time);
				print "    updated catalog deleted timestamp\n";
			}
			$failure_count += $failure;
		}
		else  {
			printf " > deleting %s files in %s\n", $Project->project, $Project->given_dir;
			my $failure += $Project->delete_project_files();
			if ($Entry and not $failure) {
				$Entry->deleted_datestamp(time);
				print "    updated catalog deleted timestamp\n";
			}
			$failure_count += $failure;
		}
	}
	
	
	# add notice file
	if ($add_notice) {
		printf " > Linking notice in %s\n", $Project->project;
		$failure_count += $Project->add_notice_file;
	}
	
	
	# clean project files
	if ($clean_project_files) {
		printf " > Cleaning %s project files\n", $Project->project;
	
		# zip archive
		if (-e $Project->zip_file) {
			printf "  ! Archive file can not be cleaned automatically!\n";
			$failure_count++;
		}
	
		# zip list
		if (-e $Project->zip_folder) {
			printf "  ! Zip folder still exists! Not removing %s\n", $Project->ziplist_file;
			$failure_count++;
		}
		elsif (-e $Project->ziplist_file) {
			unlink $Project->ziplist_file;
			printf "  Deleted %s\n", $Project->ziplist_file;
		}
	
		# remove lists
		if (-e $Project->delete_folder) {
			printf "  ! Hidden delete folder still exists! Not removing %s\n", $Project->remove_file;
			$failure_count++;
		}
		elsif (-e $Project->remove_file) {
			unlink $Project->remove_file;
			printf "  Deleted %s\n", $Project->remove_file;
		}
		if (-e $Project->alt_remove_file) {
			unlink $Project->alt_remove_file;
			printf "  Deleted %s\n", $Project->alt_remove_file;
		}
	
		# manifest
		if (-e $Project->manifest_file) {
			unlink $Project->manifest_file;
			printf "  Deleted %s\n", $Project->manifest_file;
		}
	}
	
	
	######## Finished
	printf " > finished with %s with %d failures\n\n", $Project->project, $failure_count;
	
}

printf "\n\nFinished in %.1f minutes\n", (time - $start_time)/60;




__END__

=head1 AUTHOR

 Timothy J. Parnell, PhD
 Dept of Oncological Sciences
 Huntsman Cancer Institute
 University of Utah
 Salt Lake City, UT, 84112

This package is free software; you can redistribute it and/or modify
it under the terms of the Artistic License 2.0.  







