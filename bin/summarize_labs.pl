#!/usr/bin/env perl

use warnings;
use strict;
use English qw(-no_match_vars);
use Getopt::Long;
use IO::File;
use File::Spec;
use Date::Parse;
use Text::CSV;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use RepoCatalog;

our $VERSION = 0.1;

my $doc = <<END;

A script to inventory and summarize labs and their projects.

It will collate all of the scanned projects in the catalog database
and summarize the total number and sizes for each lab. Projects
are separated by Request and Analysis and further divided by
uploaded, not uploaded and deleted, or currently active. Projects
not scanned, such as Requests for sample QC, are skipped.

It will write out a tab-delimited text file, sorted by PI name.

VERSION: $VERSION

USAGE:

    summarize_labs.pl -c catalog.db -o lab_usage.tsv

OPTIONS:
  
  -c --cat <path>         Path to metadata catalog database
  -o --out <path>         Path to output file
  --year <int>            Minimum year to calculate, default 2020
  -h --help               Show this help


END



# Command line options
my $cat_file;
my $out_file;
my $year = 2020;
my $help;

if (@ARGV) {
	GetOptions(
		'c|catalog=s'           => \$cat_file,
		'o|out=s'               => \$out_file,
		'year=i'                => \$year,
		'h|help!'               => \$help,
	) or die " bad options! Please check\n $doc\n";
}
else {
	print $doc;
	exit 0;
}

# check input options
if ($help) {
	print $doc;
	exit 0;
}
unless ($cat_file) {
	print "ERROR! Catalog file is required!\n";
	exit 1;
}
unless ($out_file) {
	print "ERROR! Output file is required!\n";
	exit 1;
}
if ( $year < 2006 or $year > 2033 ) {
	print "ERROR! Year is out of scope!\n";
}

# initialize
my $Cat = RepoCatalog->new($cat_file)
	or die "Cannot open catalog file '$cat_file'!\n";
my %pi;

# iterate
foreach my $id ( $Cat->list_all( year => $year ) ) {
	my $Entry = $Cat->entry($id) or next;
	my $lab   = sprintf "%s %s", $Entry->lab_first, $Entry->lab_last;
	if ( $Entry->external eq 'Y' ) {
		next;
	}
	unless ( exists $pi{$lab} ) {
		$pi{$lab} = [
			$Entry->lab_last,
			$Entry->lab_first,
			$Entry->pi_email,
			$Cat->get_upload_account($lab) || 'none',
			0,  # 4   youngest age
			0,  # 5   number requests
			0,  # 6   number requests uploaded
			0,  # 7   size of requests uploaded
			0,  # 8   number of requests deleted
			0,  # 9   size of requests deleted
			0,  # 10  number of active requests
			0,  # 11  size of active requests
			0,  # 12  number analyses
			0,  # 13  number analyses uploaded
			0,  # 14  size of analyses uploaded
			0,  # 15  number of analyses deleted
			0,  # 16  size of analyses deleted
			0,  # 17  number active analyses
			0,  # 18  size active analyses
		];
	}
	if ( $Entry->is_request ) {
		process_request( $Entry, $pi{$lab} );
	}
	else {
		process_analysis( $Entry, $pi{$lab} );
	}
}

# finish
write_file();
printf " > wrote %d labs to %s\n", scalar(keys %pi), $out_file;
exit 0;


#### subroutines

sub process_request {
	my ($E, $lab) = @_;
	if ( $E->scan_datestamp ) {
		$lab->[5] += 1;
	}
	else {
		# do not count
		return;
	}
	my $date = str2time($E->date);
	if ( $date > $lab->[4] ) {
		$lab->[4] = $date;
	}
	my $size = $E->last_size > $E->size ? $E->last_size : $E->size;
	if ( $E->upload_datestamp ) {
		$lab->[6] += 1;
		$lab->[7] += $size;
	}
	elsif ( $E->hidden_datestamp ) {
		$lab->[8] += 1;
		$lab->[9] += $size;
	}
	else {
		$lab->[10] += 1;
		$lab->[11] += $size;
	}
}

sub process_analysis {
	my ($E, $lab) = @_;
	return unless $E->scan_datestamp;
	my $date = str2time($E->date);
	if ( $date > $lab->[4] ) {
		$lab->[4] = $date;
	}
	$lab->[12] += 1;
	my $size = $E->last_size > $E->size ? $E->last_size : $E->size;
	if ( $E->upload_datestamp ) {
		$lab->[13] += 1;
		$lab->[14] += $size;
	}
	elsif ( $E->hidden_datestamp ) {
		$lab->[15] += 1;
		$lab->[16] += $size;
	}
	else {
		$lab->[17] += 1;
		$lab->[18] += $size;
	}
}

sub write_file {
	my $fh = IO::File->new( $out_file, '>' ) or
		die "unable to write to $out_file: $OS_ERROR\n";
	$fh->printf( "%s\n", join("\t", qw(
		PI_Last
		PI_First
		PI_Email
		AWS_Acct
		Most_Recent_Date
		Req_Num
		Req_Up_Num
		Req_Up_Size
		Req_Del_Num
		Req_Del_Size
		Req_Active_Num
		Req_Active_Size
		Ana_Num
		Ana_Up_Num
		Ana_Up_Size
		Ana_Del_Num
		Ana_Del_Size
		Ana_Active_Num
		Ana_Active_Size
	) ) );
	
	# sort labs my last, first in descending order
	my @keys = map { $_->[0] }
		sort { $a->[1] cmp $b->[1] or $a->[2] cmp $b->[2] }
		map { [ $_, $pi{$_}->[0], $pi{$_}->[1] ] } keys %pi;
	
	# transform raw to formatted
	foreach my $k (@keys) {
		my @times = localtime( $pi{$k}->[4] );
		$pi{$k}->[4] = sprintf("%04d-%02d-%02d",
			$times[5] + 1900, $times[4] + 1, $times[3]);
		if ( $pi{$k}->[5] == 0 and $pi{$k}->[12] == 0 ) {
			# no projects, skip
			next;
		}
		for my $i (7, 9, 11, 14, 16, 18) {
			$pi{$k}->[$i] = _format_size( $pi{$k}->[$i] );
		}
	}
	
	# print each lab
	foreach my $k (@keys) {
		if ( $pi{$k}->[5] == 0 and $pi{$k}->[12] == 0 ) {
			# no projects, skip
			next;
		}
		$fh->printf( "%s\n", join("\t", @{ $pi{$k} } ) );
	}
	$fh->close;
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


