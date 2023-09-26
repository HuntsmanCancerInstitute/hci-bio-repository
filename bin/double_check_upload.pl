#!/usr/bin/env perl

use warnings;
use strict;
use English qw(-no_match_vars);
use IO::File;
use Time::Local;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use RepoCatalog;
use RepoProject;


unless (@ARGV) {
	print <<DOC;

A script to double-check that all the files have been uploaded to 
Seven Bridges, usually before a drastic action occurs, like deleting 
a project. 

It simply compares the age of the youngest file in the project with 
the upload date-time stamp recorded in the catalog database, and 
issues a status verdict. It does not actually check the online project.

It requires the catalog database file and a text file of a list of 
project identifiers. 

Example usage:
double_check_upload.pl <catalog_file> <list_file.txt>

DOC
}

my $cat_file = shift;
my $list_file = shift;

# open catalog
my $Catalog = RepoCatalog->new($cat_file) or 
	die "Cannot open catalog file '$cat_file'!\n";

# open list
my @list;
my $fh = IO::File->new($list_file) or 
	die "Cannot open import file '$list_file'! $OS_ERROR\n";
my $header = $fh->getline;
while (my $l = $fh->getline) {
	chomp $l;
	my @a = split /\s+/, $l;
	push @list, $a[0];
}
$fh->close;


foreach my $id (@list) {
	
	# Collect project path
	my $Entry = $Catalog->entry($id);
	unless ($Entry) {
		print " ! Identifier $id not in Catalog! skipping\n";
		next;
	}
	my $Project = RepoProject->new($Entry->path);
	unless ($Project) { 
		printf  " ! unable to initiate Repository Project for path %s! skipping\n", 
			$Entry->path;
		next;
	}
	
	my ($size, $age) = $Project->get_size_age;
		
	my $up_stamp = $Entry->upload_datestamp;
	if ($up_stamp) {
		if ($age > $up_stamp) {
			printf " ! $id has younger files than upload time by %0.1f days!\n", ($age - $up_stamp) / 86400;
		}
		else {
			print " > $id should have all files uploaded to repo\n";
		}
	}
	else {
		# no upload stamp
		if ($Entry->division) {
			printf " ! $id has no upload date but has division %s!\n", $Entry->division;
		}
		else {
			print " > $id has no division and was not uploaded\n";
		}
	}
}
