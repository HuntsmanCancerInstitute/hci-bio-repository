#!/usr/bin/perl


use strict;
use warnings;
use IO::File;
use File::Find;
use File::Copy;
use Getopt::Long;

# shortcut variable name to use in the find callback
use vars qw(*fname);
*fname = *File::Find::name;

# variables
my $verbose = 1;
my $do_copy = 0;

# basename for temporary md5 checksum files
my $basename = '/tmp/temp.' . $$;

find(
	{
		follow => 0,
		wanted => \&callback,
	}, 
# 	'/Repository/AnalysisData/2018',
	'/Repository/AnalysisData/2019'
);



# find callback
sub callback {
	my $file = $_;
	print "checking $fname\n" if $verbose;
	
	# skip directories and symlinks
	if (not -f $file) {
		print " > not a file, skipping\n" if $verbose;
		return;
	}
	
	# skip small files, about 4 kb in size or smaller
	my $size = -s _;
	if ($size < 4000) {
		print " > too small, skipping\n" if $verbose;
		return;
	}
	
	# calculate and check for a corresponding restoration file
	my $restore_file = $fname;
	$restore_file =~ s|^/Repository/AnalysisData/\d{4}/|/Repository/MicroarrayData/AnalysisRestore/|;
	if (not -f $file) {
		print " > restored file not present, skipping\n" if $verbose;
		return;
	}
	else {
		print "  restored file: $restore_file\n";
	}
	
	# calculate checksums 
	my ($check1, $check2);
	# current file
	my $checksum = `md5sum \"$file\"`;
	($check1, undef) = split /\s+/, $checksum;
	
	# restored file
	$checksum = `md5sum \"$restore_file\"`;
	($check2, undef) = split /\s+/, $checksum;
	
# 	if ($size < 100000000) {
# 		# file is under 100 MB, calculate serially, directly
# 		
# 	}
# 	else {
# 		# files are big, fork this 
# 		
# 		# THIS ISN'T WORKING!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# 		
# 		# first file
# 		my $child1 = fork();
# 		die "couldn't fork!" unless defined $child1;
# 		if (not $child1) {
# 			# child process
# 			system("md5sum $file > $basename.1");
# 			exit;
# 		}
# 
# 		# check file2
# 		my $child2 = fork();
# 		die "couldn't fork!" unless defined $child2;
# 		if (not $child2) {
# 			# child process
# 			system("md5sum $restore_file > $basename.2");
# 			exit;
# 		}
# 
# 		# collect checksums
# 		wait;
# 		sleep 5; # seems like I have to wait.... why?
# 		if (-s "$basename.1" and -s "$basename.2") {
# 			# both files are present and non-zero length, success
# 		
# 			# first file
# 			my $fh = IO::File->new("$basename.1") or 
# 				die "unable to read $basename.1! $!\n";
# 			($check1, undef) = split(/\s+/, $fh->getline);
# 			$fh->close;
# 			unlink "$basename.1";
# 		
# 			# second file
# 			$fh = IO::File->new("$basename.2") or 
# 				die "unable to read $basename.2! $!\n";
# 			($check2, undef) = split(/\s+/, $fh->getline);
# 			$fh->close;
# 			unlink "$basename.2";
# 		
# 		}
# 		else {
# 			die "can't find md5 output files $basename!\n";
# 		}
# 	}
	
	# compare
	if ($check1 eq $check2) {
		print " > file ok\n";
		return;
	}
	else {
		print " > file $fname corrupt!\n" if $verbose;
		if ($do_copy) {
			print " > copying $restore_file to $file\n" if $verbose;
			cp($restore_file, $file);
		}
	}
	
	
}



