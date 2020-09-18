#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use RepoCatalog;
use Emailer;

my $VERSION = 5;


######## Documentation
my $doc = <<END;
A script to send emails based on repository catalog entries.

Project identifiers (1234R or A5678) may be provided as items on 
the command line, or provided as a list file. A list file is 
assumed to have a header line. The file may have multiple columns, 
only the first column is considered.

USAGE:
manage_catalog.pl --cat <file.db> <options> <project1 project2 ...>

OPTIONS:
  Required:
    --cat <path>            Provide the path to a catalog file
  
  Email type: pick one
    --req_del               Email Request scheduled deletion
    --req_up                Email Request  upload to Seven Bridges
    --anal_del              Email Analysis scheduled deletion
    --anal_up               Email Analysis upload to Seven Bridges
  
  Options:
    --list <file>           List of project identifiers, assumes header
    --mock                  Print email to STDOUT instead of sending
  
END
 




####### Input
my $cat_file;
my $list_file;
my $mock     = 0;
my $req_del  = 0;
my $req_up   = 0;
my $anal_del = 0;
my $anal_up  = 0;

if (scalar(@ARGV) > 1) {
	GetOptions(
		'cat=s'             => \$cat_file,
		'list=s'            => \$list_file,
		'mock!'             => \$mock,
		'req_del!'          => \$req_del,
		'req_up!'           => \$req_up,
		'anal_del!'         => \$anal_del,
		'anal_up!'          => \$anal_up,
	) or die "please recheck your options!\n\n";
}
else {
	print $doc;
	exit;
}




### Input options

# Catalog file
unless ($cat_file) {
	die "No catalog file provided!\n";
}

# Email functions
my $check = $req_del + $req_up + $anal_del + $anal_up;
if ($check == 0) {
	die "Must provide at least one email function!\n";
}
elsif ($check > 1) {
	die "Warning! Pick only one email function!\n";
}

# Project list
my @projects;
if (@ARGV) {
	# left over items from command line
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

# Initialize Emailer
my $Email = Emailer->new();
unless ($Email) {
	die "Unable to initialize Emailer!\n";
}



### Open Project
my $Catalog = RepoCatalog->new($cat_file) or 
	die "Cannot open catalog file '$cat_file'!\n";




### Process
foreach my $id (@projects) {
	
	# get project entry
	my $Entry = $Catalog->entry($id);
	unless (defined $Entry) {
		print " ! Unable to locate project $id in Catalog! Skipping\n";
		next;
	}
	
	# Email
	if ($req_del) {
		my $result = $Email->send_request_deletion_email($Entry, 'mock' => $mock);
		if ($result) {
			printf " > Sent Request deletion email for $id: %s\n", 
				ref($result) ? $result->message : "\n$result";
			$Entry->emailed_datestamp(time) if not $mock;
		}
		else {
			print " ! Failed Request deletion email for $id\n";
		}
	}
	if ($req_up) {
		my $result = $Email->send_request_upload_email($Entry, 'mock' => $mock);
		if ($result) {
			printf " > Sent Request SB upload email for $id: %s\n", 
				ref($result) ? $result->message : "\n$result";
			$Entry->emailed_datestamp(time) if not $mock;
		}
		else {
			print " ! Failed Request upload email for $id\n";
		}
	}
	if ($anal_del) {
		my $result = $Email->send_analysis_deletion_email($Entry, 'mock' => $mock);
		if ($result) {
			printf " > Sent Analysis deletion email for $id: %s\n", 
				ref($result) ? $result->message : "\n$result";
			$Entry->emailed_datestamp(time) if not $mock;
		}
		else {
			print " ! Failed Analysis deletion email for $id\n";
		}
	}
	if ($anal_up) {
		my $result = $Email->send_analysis_upload_email($Entry, 'mock' => $mock);
		if ($result) {
			printf " > Sent Analysis SB upload email for $id: %s\n", 
				ref($result) ? $result->message : "\n$result";
			$Entry->emailed_datestamp(time) if not $mock;
		}
		else {
			print " ! Failed Analysis upload email for $id\n";
		}
	}
}

print "Finished\n";





