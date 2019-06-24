#!/usr/bin/perl


use strict;
use warnings;
use File::Find;

my $VERSION = 2;

# useful shortcut variable names to use in the find callback
use vars qw(*fullname *curdir);
*fullname = *File::Find::name;
*curdir = *File::Find::dir;

# variables
my $verbose = 0;
my $do_copy = 1;
my $check_prefix = qr|^/Repository/AnalysisData/\d{4}/|; # up to year
my $restore_prefix = '/Repository/MicroarrayData/AnalysisRestore/'; 

unless (@ARGV) {
	print <<END;

Script to find corrupted files by comparing current with 
restored copies by checksum. Restores corrupted files from 
the backup copy if enabled; otherwise prints equivalent command.
Verbose mode prints status and skipped files.

The check prefix is replaced with the backup prefix so that the 
two file copies can be found and compared.

Hardcoded values:
   check prefix regex: $check_prefix    
   restore prefix:     $restore_prefix
   verbose:            $verbose         
   copy:               $do_copy         

Usage:
  $0 <check_directory> 

Example:
  $0 /Repository/AnalysisData/2019/A1234 

END
	exit;
}

# directories
my $check_directory = shift @ARGV;
print "\n ==== Processing $check_directory\n\n";

if (not -d $check_directory) {
	die "given directory to check '$check_directory' doesn't exist!\n";
}
if (not -d $restore_prefix) {
	die "restore prefix directory '$restore_prefix' doesn't exist!\n";
}


find( {
		follow => 0, # do not follow symlinks
		wanted => \&callback,
	  }, $check_directory
);



# find callback
sub callback {
	my $file = $_;
	print "checking $file\n" if $verbose;
	
	# skip directories and symlinks
	if (-d $file or -l $file) {
		print " > not a file, skipping\n" if $verbose;
		return;
	}
	my $date = (stat($file))[9];
	
	# calculate and check for a corresponding restoration file
	my $restore_file = $fullname;
	$restore_file =~ s|$check_prefix|$restore_prefix|;
	if (-e $restore_file) {
		print " > restored file present\n" if $verbose;
	}
	else {
		if ($restore_file =~ /_REMOVE_LIST\.txt$/) {
			# special case, may be hidden, try looking up one directory
			$restore_file =~ s|A\d{4}/||;
			if (-e $restore_file) {
				print " > restored file present\n" if $verbose;
			}
			else {
				# still not present
				print " > restored file '$restore_file' NOT present, skipping\n" if $verbose;
				return;
			}
		}
		elsif ($restore_file =~ /_DELETED_FILES/) {
			# special case, may be hidden, look in the parent directory
			$restore_file =~ s|_DELETED_FILES||;
			if (-e $restore_file) {
				print " > restored file present\n" if $verbose;
			}
			else {
				# still not present
				print " > restored file '$restore_file' NOT present, skipping\n" if $verbose;
				return;
			}
		}
		else {
			print " > restored file '$restore_file' NOT present, skipping\n" if $verbose;
			return;
		}
	}
	
	# check dates - they should be the same
	my $restore_date = (stat($restore_file))[9];
	if ($date > $restore_date) {
		if ($verbose) {
			print " > current file is newer than restored file, skipping\n";
		}
		else {
			print " Newer: $fullname\n";
		}
		return;
	}
	elsif ($date < $restore_date) {
		if ($verbose) {
			print " >  WARNING! current file is older than restored file! skipping\n";
		}
		else {
			print " OLDER! $fullname\n";
		}
		return;
	}
	
	# current file checksum
	my $checksum = `md5sum \"$file\"`;
	my ($check1, undef) = split /\s+/, $checksum;
	
	# restored file checksum
	$checksum = `md5sum \"$restore_file\"`;
	my ($check2, undef) = split /\s+/, $checksum;
	
	# compare
	if ($check1 eq $check2) {
		if ($verbose) {
			print " > file OK\n";
		}
		else {
			print " OK $fullname\n";
		}
		return;
	}
	else {
		if ($verbose) {
			print " > file CORRUPT\n";
		}
		else {
			print " CORRUPT $fullname\n"
		}
		if ($do_copy) {
			print "  > copying $restore_file to $file\n";
			system('/usr/bin/cp', '-p', $restore_file, $file) == 0 or 
				die " > WARNING! external cp failed!\n";
			# if this fails something is wrong and I need to fix it!!!!
		}
		else {
			print "  > should execute: cd $curdir && cp -p $restore_file $file\n";
		}
	}
}



