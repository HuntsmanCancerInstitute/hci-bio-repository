#!/usr/bin/env perl

use warnings;
use strict;
use English qw(-no_match_vars);
use IO::File;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use Net::SB;
use RepoCatalog;

our $VERSION = 1;

my $doc = <<END;

Script to update project names. 

This will change the name of the project in the SBG database, but not the project ID.
Hopefully this will make it a little easier for end users to identify projects, using 
the original name in the GNomEx database rather than simply the identifier.

Give the script a catalog database file and a list of projects to change name.

The list can be generated from the script 'check_divisions.pl'. Specifically, 
it has columns Division, ID, and Name. It should be filtered appropriately for 
those projects that need changing.

Usage: change_project_names.pl <catalog> <list_file>

END

unless (scalar @ARGV == 2) {
	print " missing parameters!\n";
	print $doc;
	exit;
}

my ($cat_file, $list_file) = @ARGV;


### Open list
my @list;
{
	my $fh = IO::File->new($list_file) or 
		die "Cannot open import file '$list_file'! $OS_ERROR\n";
	
	# check header
	my $header = $fh->getline;
	chomp $header;
	my @heads = split /\t/, $header;
	unless (
		$heads[0] eq 'Division' and
		$heads[1] eq 'ID' and
		$heads[2] eq 'Name'
	) {
		print " List file doesn't look good! Not the right headers!\n\n$doc\n ";
		exit 1;
	}
	
	# load remaining file
	while (my $l = $fh->getline) {
		chomp $l;
		push @list, $l;
	}
	
	printf " loaded %d lines from $list_file\n\n", scalar(@list);
	$fh->close;
}


### Open catalog
my $Cat = RepoCatalog->new($cat_file) or 
	die "Cannot open catalog file '$cat_file'!\n";



### Iterate
my %Divisions = ();
my $good_count = 0;
my $bad_count  = 0;
foreach my $line (@list) {
	
	# split the line, there may be more columns
	# example
	# big-shot-pi	big-shot-pi/123456r	123456R
	my @bits = split /\t/, $line;
	my $div = $bits[0];
	my ($div2, $id) = split m|/|, $bits[1];
	my $name = $bits[2];
	unless ($div eq $div2) {
		die "whoa! somethings weird with this line:\n$  line\n";
	}
	print " Checking $bits[1]\n";
	
	# pull up Catalog entry
	my $Entry = $Cat->entry($name) || undef;
	unless ($Entry) {
		print "   ! not found in Catalog file\n";
		$bad_count++;
		next;
	}
	
	# check SBG Division
	my $Division;
	if (exists $Divisions{$div}) {
		# great we have one!
		$Division = $Divisions{$div};
	}
	else {
		$Division = Net::SB->new(
			'div' => $div
		);
		unless ($Division) {
			print "   ! Cannot connect to division, skipping\n";
			$bad_count++;
			next;
		}
		# cache for next time
		$Divisions{$div} = $Division;
	}
	
	# Get Project
	my $Project = $Division->get_project($id);
	unless ($Project) {
		print "    ! Cannot get project $id, skipping\n";
		$bad_count++;
		next;
	}
	
	# Update the name
	my $success = $Project->update(
		'name'   => sprintf("%s: %s", $name, $Entry->name),
	);
	if ($success) {
		printf "   > updated name to %s\n", $Project->name;
		$good_count++;
	}
	else {
		print "   ! name update failed\n";
		$bad_count++;
	}
}

print "\n => Changed $good_count project names\n";
if ($bad_count) {
	print " => $bad_count projects had issues and could not be changed\n";
}



