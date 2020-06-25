#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use FindBin qw($Bin);
use lib $Bin;
use RepoCatalog;
use Emailer;

my $VERSION = 5;


######## Documentation
my $doc = <<END;
A script to send emails based on repository catalog entries.

manage_catalog.pl --cat <file.db> <options> <project1 project2 ...>

  Required:
    --cat <path>            Provide the path to a catalog file
  
  Email type: pick one
    --req_del               Email Request scheduled deletion
    --req_up                Email Request  upload to Seven Bridges
    --anal_del              Email Analysis scheduled deletion
    --anal_up               Email Analysis upload to Seven Bridges
END
 




####### Input
my $cat_file;
my $req_del  = 0;
my $req_up   = 0;
my $anal_del = 0;
my $anal_up  = 0;

if (scalar(@ARGV) > 1) {
	GetOptions(
		'cat=s'             => \$cat_file,
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

unless ($cat_file) {
	die "No catalog file provided!\n";
}

my $check = $req_del + $req_up + $anal_del + $anal_up;
if ($check == 0) {
	die "Must provide at least one email function!\n";
}
elsif ($check > 1) {
	die "Warning! Pick only one email function!\n";
}

my @projects = @ARGV;
unless (@projects) {
	die "Must provide one or more projects to work on!\n";
}



### Open Project
my $Catalog = RepoCatalog->new($cat_file) or 
	die "Cannot open catalog file '$cat_file'!\n";


### Process
foreach my $id (@projects) {
	
	# get project entry
	my $Entry = $Catalog->entry($id);
	unless (defined $Entry) {
		print " Unable to locate project $id in Catalog! Skipping\n";
		next;
	}
	
	# Email
	if ($req_del) {
		my $result = send_request_deletion_email($Entry);
		if ($result) {
			printf " Sent Request deletion email for $id: %s\n", $result->message;
			$Entry->emailed_datestamp(time);
		}
		else {
			print " Failed Request deletion email for $id\n";
		}
	}
	if ($req_up) {
		my $result = send_request_upload_email($Entry);
		if ($result) {
			printf " Sent Request SB upload email for $id: %s\n", $result->message;
			$Entry->emailed_datestamp(time);
		}
		else {
			print " Failed Request upload email for $id\n";
		}
	}
	if ($anal_del) {
		my $result = send_analysis_deletion_email($Entry);
		if ($result) {
			printf " Sent Analysis deletion email for $id: %s\n", $result->message;
			$Entry->emailed_datestamp(time);
		}
		else {
			print " Failed Analysis deletion email for $id\n";
		}
	}
	if ($anal_up) {
		my $result = send_analysis_upload_email($Entry);
		if ($result) {
			printf " Sent Analysis SB upload email for $id: %s\n", $result->message;
			$Entry->emailed_datestamp(time);
		}
		else {
			print " Failed Analysis upload email for $id\n";
		}
	}
}

print "Finished\n";





