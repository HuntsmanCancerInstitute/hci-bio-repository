#!/usr/bin/env perl

use warnings;
use strict;
use English qw(-no_match_vars);
use IO::File;
use Time::Local qw( timelocal_posix );
use FindBin     qw($Bin);
use lib "$Bin/../lib";
use RepoCatalog;

our $VERSION = 0.1;

my $doc = <<DOC;

A script to summarize the number and size of GNomEx Request and Analysis
projects recorded in a Catalog database file. Projects are summarized
by week for each year, using the recorded date when the project was
generated in GNomEx, for lack of anything better, and not when work was
performed or when data files were generated.

Requests are broken down into those with sequencing data (Sequencing 
Requests) and those without (usually Sample QC requests).

After data collection from the Catalog, additional columns are generated
with cumulative sums across each year to facilitate graphing. No cumulative
numbers are calculated for QC requests.

Two files are written, one for Requsts and one for Analyses. The file
names are appended with today's date.

USAGE:

    summarize_catalog.pl <Catalog.db>

Generates two tab-delimited text files:

    <Catalog>.request.YYYYMMDD.tsv
    <Catalog>.analysis.YYYYMMDD.tsv

DOC

unless (@ARGV) {
	print $doc;
	exit 0;
}

# Open catalog file
my $cat_file = shift @ARGV;
my $Cat      = RepoCatalog->new($cat_file)
	or die "Cannot open catalog file '$cat_file'!\n";

## process projects
my %req;
my %anal;
foreach my $id ( $Cat->list_all ) {
	my $Entry = $Cat->entry($id) or next;
	if ( $Entry->is_request ) {
		process_request($Entry);
	}
	else {
		process_analysis($Entry);
	}
}

## write files
write_req();
write_anal();

exit 0;

#### Subroutines

sub process_date {
	my $Entry = shift;
	my ( $year, $m, $d ) = split /\-/, $Entry->date;

	# need to convert simple date back to posix time
	my $time = timelocal_posix( 0, 0, 12, $d, $m - 1, $year - 1900 );

	# then convert to week
	my $yday = ( localtime($time) )[7];            # get day of year
	my $week = int( ( $yday / 365 ) * 52 ) + 1;    # convert to weeks 1..52
	if ( $week == 53 ) {

		# this happens when the date is at the end of December, so just call it 52
		$week = 52;
	}
	return ( $year, $week );
}

sub process_request {
	my $Entry = shift;
	my ( $year, $week ) = process_date($Entry);
	unless ( exists $req{$year} ) {

	   # each array: qc_count, seq_count, seq_size, up_count, up_size, del_count, del_size
		$req{$year} = { map { $_ => [ 0, 0, 0, 0, 0, 0, 0 ] } ( 1 .. 52 ) };
	}
	if ( $Entry->scan_datestamp ) {
		my $size = $Entry->size;
		if ( $size eq q(.) ) {
			$size = 0;    # must be empty or lost?
		}
		if ( $Entry->hidden_datestamp and $Entry->last_size > $size ) {
			$size = $Entry->last_size;
		}
		$req{$year}{$week}->[1] += 1;            # sequence count
		$req{$year}{$week}->[2] += $size;        # sequence size
		if ( $Entry->upload_datestamp ) {
			$req{$year}{$week}->[3] += 1;        # upload count
			$req{$year}{$week}->[4] += $size;    # upload size
		}
		elsif ( $Entry->deleted_datestamp ) {
			$req{$year}{$week}->[5] += 1;        # deleted count
			$req{$year}{$week}->[6] += $size;    # deleted size
		}
	}
	else {
		$req{$year}{$week}->[0] += 1;            # qc count
	}
}

sub process_analysis {
	my $Entry = shift;
	my ( $year, $week ) = process_date($Entry);
	unless ( exists $anal{$year} ) {

		# each array: count, size, up_count, up_size, del_count, del_size
		$anal{$year} = { map { $_ => [ 0, 0, 0, 0, 0, 0 ] } ( 1 .. 52 ) };
	}
	my $size = $Entry->size;
	if ( $size eq q(.) ) {
		$size = 0;    # must be empty?
	}
	if ( $Entry->hidden_datestamp and $Entry->last_size > $size ) {
		$size = $Entry->last_size;
	}
	$anal{$year}{$week}->[0] += 1;            # count
	$anal{$year}{$week}->[1] += $size;        # size
	if ( $Entry->upload_datestamp ) {
		$anal{$year}{$week}->[2] += 1;        # upload count
		$anal{$year}{$week}->[3] += $size;    # upload size
	}
	elsif ( $Entry->deleted_datestamp ) {
		$anal{$year}{$week}->[4] += 1;        # deleted count
		$anal{$year}{$week}->[5] += $size;    # deleted size
	}
}

sub write_req {
	my @today = localtime(time);
	my $base  = $cat_file;
	$base =~ s/\.db$//;
	my $out = sprintf "%s.request.%d%02d%02d.tsv", $base, $today[5] + 1900,
		$today[4] + 1, $today[3];
	my $fh = IO::File->new( $out, '>' )
		or die "unable to write '$out'! $OS_ERROR\n";
	$fh->printf(
		"%s\n",
		join(
			"\t", qw(Year Week QC_Count SeqCount SeqSize
				SeqUploadCount SeqUploadSize SeqDeleteCount SeqDeleteSize
				CumulSeqCount CumulSeqSize CumulSeqUploadCount CumulSeqUploadSize
				CumulSeqDeleteCount CumulSeqDeleteSize)
		)
	);
	foreach my $year ( sort { $a <=> $b } keys %req ) {
		my $cumul_count    = 0;
		my $cumul_size     = 0;
		my $cumul_up       = 0;
		my $cumul_up_size  = 0;
		my $cumul_del      = 0;
		my $cumul_del_size = 0;
		foreach my $week ( 1 .. 52 ) {
			$cumul_count    += $req{$year}{$week}->[1];
			$cumul_size     += $req{$year}{$week}->[2];
			$cumul_up       += $req{$year}{$week}->[3];
			$cumul_up_size  += $req{$year}{$week}->[4];
			$cumul_del      += $req{$year}{$week}->[5];
			$cumul_del_size += $req{$year}{$week}->[6];
			$fh->printf(
				"%s\n",
				join(
					"\t",
					$year,
					$week,
					$req{$year}{$week}->[0],
					$req{$year}{$week}->[1],
					size_in_gb( $req{$year}{$week}->[2] ),
					$req{$year}{$week}->[3],
					size_in_gb( $req{$year}{$week}->[4] ),
					$req{$year}{$week}->[5],
					size_in_gb( $req{$year}{$week}->[6] ),
					$cumul_count,
					size_in_gb($cumul_size),
					$cumul_up,
					size_in_gb($cumul_up_size),
					$cumul_del,
					size_in_gb($cumul_del_size)
				)
			);
		}
	}
	printf " wrote %s\n", $out;
}

sub write_anal {
	my @today = localtime(time);
	my $base  = $cat_file;
	$base =~ s/\.db$//;
	my $out = sprintf "%s.analysis.%d%02d%02d.tsv", $base, $today[5] + 1900,
		$today[4] + 1, $today[3];
	my $fh = IO::File->new( $out, '>' )
		or die "unable to write '$out'! $OS_ERROR\n";
	$fh->printf(
		"%s\n",
		join(
			"\t", qw(Year Week Count Size
				UploadCount UploadSize DeleteCount DeleteSize
				CumulCount CumulSize CumulUploadCount CumulUploadSize
				CumulDeleteCount CumulDeleteSize)
		)
	);
	foreach my $year ( sort { $a <=> $b } keys %req ) {
		my $cumul_count    = 0;
		my $cumul_size     = 0;
		my $cumul_up       = 0;
		my $cumul_up_size  = 0;
		my $cumul_del      = 0;
		my $cumul_del_size = 0;
		foreach my $week ( 1 .. 52 ) {
			$cumul_count    += $anal{$year}{$week}->[0];
			$cumul_size     += $anal{$year}{$week}->[1];
			$cumul_up       += $anal{$year}{$week}->[2];
			$cumul_up_size  += $anal{$year}{$week}->[3];
			$cumul_del      += $anal{$year}{$week}->[4];
			$cumul_del_size += $anal{$year}{$week}->[5];
			$fh->printf(
				"%s\n",
				join(
					"\t",
					$year,
					$week,
					$anal{$year}{$week}->[0],
					size_in_gb( $anal{$year}{$week}->[1] ),
					$anal{$year}{$week}->[2],
					size_in_gb( $anal{$year}{$week}->[3] ),
					$anal{$year}{$week}->[4],
					size_in_gb( $anal{$year}{$week}->[5] ),
					$cumul_count,
					size_in_gb($cumul_size),
					$cumul_up,
					size_in_gb($cumul_up_size),
					$cumul_del,
					size_in_gb($cumul_del_size)
				)
			);
		}
	}
	printf " wrote %s\n", $out;
}

sub size_in_gb {
	my $s = shift;
	return sprintf( "%.1f", ( $s / 1073741824 ) );
}

