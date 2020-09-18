#!/usr/bin/perl

use strict;
use File::Spec;
use IO::File;
use POSIX qw(strftime);

unless (@ARGV) {
	print " A script to collect manifest file spec data\n";
	print " Usage: $0 <list_file> <output_file>\n";
	exit;
}


my $list = get_file_list(shift @ARGV);

my $output_file = shift @ARGV;
my @output;

foreach my $given_dir (@$list) {
	
	my $project;
	my $start_dir;

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
		warn "given path $given_dir does not exist!\n";
		next;
	}

	# start directory
	if ($given_dir =~ m/^(\/Repository\/(?:MicroarrayData|AnalysisData)\/\d{4})\/?/) {
		$start_dir = $1;
	}
	
	# file paths
	my $manifest_file = File::Spec->catfile($given_dir, $project . "_MANIFEST.txt");
	my $alt_manifest  = File::Spec->catfile($start_dir, $project . "_MANIFEST.txt");
	my $real_manifest = File::Spec->catfile($project, $project . "_MANIFEST.txt");
	
	# stats on the file
	my ($date, $size, $md5, $age);
	if (-e $manifest_file) {
		($date, $size, $md5, $age) = get_file_stats($manifest_file);
	}
	elsif (-e $alt_manifest) {
		($date, $size, $md5, $age) = get_file_stats($alt_manifest);
	}
	else {
		warn " $manifest_file file does not exist for $project!\n";
		next;
	}
	
	# store
	push @output, sprintf("%s\t%d\t%s\tmeta\t%s\n", $md5, $size, $date, $real_manifest);
}

# finished
my $fh = IO::File->new($output_file, 'w') or 
	die "can't write to $output_file! $!\n";
$fh->print("MD5\tSize\tModification_Time\tType\tName\n");
foreach my $d (@output) {
	$fh->print($d);
}
$fh->close;
printf "wrote %d items to output file $output_file\n", scalar(@output);





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
	return \@list;
}



sub get_file_stats {
	my ($file) = @_;
	
	# stats on the file
	my @st = stat($file);
	my $age = $st[9];
	my $date = strftime("%B %d, %Y %H:%M:%S", localtime($age));
	my $size = $st[7];
	
	# calculate the md5 with external utility
	my $checksum = `md5sum \"$file\"`; # quote file, because we have files with ;
	my ($md5, undef) = split(/\s+/, $checksum);
	
	# finished
	return ($date, $size, $md5, $age);
}

