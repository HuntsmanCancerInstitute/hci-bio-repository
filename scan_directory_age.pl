#!/usr/bin/perl


use strict;
use File::Find;

our $VERSION = 1.1;

######## Documentation
my $doc = <<END;

A script to report the ages in days for a give directory.
It will scan all files in a given directory and sub-directories, 
and report the age in days from now for the youngest file found.

Version: $version

Usage:
    scan_directory_age.pl [--verbose] /path/to/directory1 ...

Options:
    --verbose       Report youngest and oldest file
    
END



### Options
my $verbose = 0;


if (scalar @ARGV == 0) {
	print $doc;
	exit;
}
if (scalar $ARGV[0] =~ /^(?:\-v|\-\-verbose)$/i) {
	$verbose = 1;
	shift @ARGV;
}



### Search the directory using File::Find to find oldest and youngest file
# the intent here is to find project directories that haven't been touched 
# for some while and are eligible for archiving in Seven Bridges

# set up
my $youngest      = 0;
my $oldest        = 0;
my $youngest_file = '';
my $oldest_file   = '';


foreach my $given_dir (@ARGV) {
	
	next unless -d $given_dir;
	
	# (re-)initialize
	$youngest      = 0;
	$oldest        = 0;
	$youngest_file = '';
	$oldest_file   = '';
	
	# search
	find( {
			follow => 0, # do not follow symlinks
			wanted => \&age_callback,
		  }, $given_dir
	);

	# report results
	if ($youngest == 0) {
		# no files found, can't give reliable time difference
		printf "-\t%s\n", $given_dir;
		next;
	}
	my $day = 60 * 60 * 24;
	my $youngest_age = (time - $youngest) / $day;
	my $oldest_age = (time - $oldest) / $day;
	printf("%0.f\t%s\n", $youngest_age, $given_dir);
	if ($verbose) {
		printf("  oldest file at %.0f days is %s\n", $oldest_age, $oldest_file);
		printf("  youngest file at %.0f days is %s\n", $youngest_age, $youngest_file);
	}
}

exit;

sub age_callback {
	my $file = $_;
	
	# skip specific files, including SB preparation files
	return if -d $file;
	return if -l $file;
	return if substr($file,0,1) eq '.'; # dot files are often hidden, backup, or OS stuff
	return if $file =~ /_MANIFEST\.csv$/; 
	return if $file =~ /_MANIFEST\.txt$/; 
	return if $file =~ /_ARCHIVE\.zip$/;
	return if $file =~ /_ARCHIVE_LIST\.txt$/;
	return if $file =~ /_REMOVE_LIST\.txt$/;
	
	# get age
	my $age = (stat($file))[9];
	
	# initialize
	if ($youngest == 0) {
		# first file! seed with current data
		$youngest      = $age;
		$oldest        = $age;
		$youngest_file = $file;
		$oldest_file   = $file;
	}
	
	# check for younger
	elsif ($age > $youngest) {
		$youngest = $age;
		$youngest_file = $file;
	}
	
	# check for older
	elsif ($age < $oldest) {
		$oldest = $age;
		$oldest_file = $file;
	}
}



