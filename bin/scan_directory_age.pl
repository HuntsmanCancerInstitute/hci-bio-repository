#!/usr/bin/env perl


use strict;
use File::Find;
use Getopt::Long;

our $VERSION = 1.4;

######## Documentation
my $doc = <<END;

A script to report the ages in days for a give directory.
It will scan all files in a given directory and sub-directories, 
and report the age in days from now for the youngest file found.
It will also generate the total size of the directory. Sizes
are given in human-readable (K,M,G,T suffix) binary sizes.

Version: $VERSION

Usage:
    scan_directory_age.pl [--verbose] /path/to/directory1 ...

Options:
    -i --hidden        Include "hidden" files beginning with dot
    -v --verbose       Report youngest, oldest, and biggest file
    -h --help          Show this help

END



### Options
my $hidden = 0;
my $verbose = 0;
my $help = 0;

if (scalar @ARGV == 0) {
	print $doc;
	exit;
}

GetOptions(
	'i|hidden!'       => \$hidden,
	'v|verbose!'      => \$verbose,
	'h|help!'         => \$help
) or die "please recheck your options!\n\n$doc\n";

if ($help) {
	print $doc;
	exit;
}


### Search the directory using File::Find to find oldest and youngest file
# the intent here is to find project directories that haven't been touched 
# for some while and are eligible for archiving in Seven Bridges

# set up
my $youngest      = 0;
my $oldest        = 0;
my $running_size  = 0;
my $biggest_size  = 0;
my $youngest_file = q();
my $oldest_file   = q();
my $biggest_file  = q();
my $day = 60 * 60 * 24; # in seconds

foreach my $given_dir (@ARGV) {
	
	next unless -d $given_dir;
	
	# (re-)initialize
	$youngest      = 0;
	$oldest        = 0;
	$running_size  = 0;
	$biggest_size  = 0;
	$youngest_file = q();
	$oldest_file   = q();
	$biggest_file  = q();
	
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
	my $youngest_age = (time - $youngest) / $day;
	my $oldest_age = (time - $oldest) / $day;
	printf("%.0f\t%s\t%s\n", $youngest_age, _format_size($running_size), $given_dir);
	if ($verbose) {
		printf("  oldest file at %.0f days is %s\n", $oldest_age, $oldest_file);
		printf("  youngest file at %.0f days is %s\n", $youngest_age, $youngest_file);
		printf("  biggest file at %s is %s\n", _format_size($biggest_size), 
			$biggest_file);
	}
}

exit;

sub age_callback {
	my $file = $_;
	
	# skip specific files, including SB preparation files
	return if -d $file;
	return if -l $file;
	return if $file =~ /_MANIFEST \.csv $/x; 
	return if $file =~ /_MANIFEST \.txt $/x; 
	return if $file =~ /_ARCHIVE \.zip $/x;
	return if $file =~ /_ARCHIVE_LIST \.txt $/x;
	return if $file =~ /_REMOVE_LIST \.txt $/x;
	
	# get file stats
	my ($size, $age) = (stat($file))[7,9];
	$running_size += $size;
	
	# dot files are often hidden, backup, or OS stuff - but we still count the size
	return if (substr($file,0,1) eq '.' and not $hidden);
	
	# initialize
	if ($youngest == 0) {
		# first file! seed with current data
		$youngest      = $age;
		$oldest        = $age;
		$youngest_file = $file;
		$oldest_file   = $file;
		$biggest_file  = $file;
		$biggest_size  = $size;
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
	
	# check file size
	if ($size > $biggest_size) {
		$biggest_size = $size;
		$biggest_file = $file;
	}
}

sub _format_size {
	my $size = shift;
	# using binary sizes here
	if ($size > 1099511627776) {
		return sprintf("%.1fT", $size / 1099511627776);
	}
	elsif ($size > 1073741824) {
		return sprintf("%.1fG", $size / 1073741824);
	}
	elsif ($size > 1048576) {
		return sprintf("%.1fM", $size / 1048576);
	}
	elsif ($size > 1000) {
		# avoid weird formatting situations of >1000 and < 1024 bytes
		return sprintf("%.1fK", $size / 1024);
	}
	else {
		return sprintf("%dB", $size);
	}
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


