#!/usr/bin/env perl

use warnings;
use strict;
use English qw(-no_match_vars);
use Getopt::Long;
use IO::File;
use Time::Local qw( timelocal_posix );
use FindBin     qw($Bin);
use lib "$Bin/../lib";
use RepoCatalog;

our $VERSION = 0.2;

my $doc = <<DOC;

A script to summarize the number and size of GNomEx Request and Analysis
projects recorded in a Catalog database file. Projects are summarized
in bins for each year (weeks or months), using the recorded date when the
project was generated in GNomEx, for lack of anything better, and not when
work was performed or when data files were generated.

Requests are broken down into those with sequencing data (Sequencing 
Requests) and those without (usually Sample QC requests).

After data collection from the Catalog, additional columns are generated
with cumulative sums across each year to facilitate graphing. No cumulative
numbers are calculated for QC requests.

Two files are written, one for Requsts and one for Analyses. The file
names are appended with today's date.

USAGE:

    summarize_catalog.pl -c <Catalog.db>

OPTIONS:
	-c --catalog <file>         The Catalog file
	-b --bin [week|month]       Report summed data in weeks or months
	                               Default is weeks.
	-h --help                   Print help

Generates two tab-delimited text files appended with the current day's date:

    <Catalog>.request.YYYYMMDD.tsv
    <Catalog>.analysis.YYYYMMDD.tsv

DOC

my $cat_file;
my $bin_unit = 'week';
my $help;

if (@ARGV) {
	GetOptions(
		'c|catalog=s'           => \$cat_file,
		'b|bin=s'               => \$bin_unit,
		'h|help!'               => \$help,
	) or die " bad options! Please check\n $doc\n";
}
else {
	print $doc;
	exit 0;
}
if ($help) {
	print $doc;
	exit 0;
}
if ($bin_unit =~ /^ (?: week | month | year) $/x) {
	$bin_unit =~ s/^(\w)/\u$1/;
}
else {
	die " Bin unit must be one of week, month, or year!\n";
}


# Open catalog file
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
	$m = int($m); # remove leading zero
	
	# generate bin unit
	my $bin;
	if ($bin_unit eq 'Week') {
		# need to convert simple date back to posix time
		my $time = timelocal_posix( 0, 0, 12, $d, $m - 1, $year - 1900 );
	
		# then convert to week
		my $yday = ( localtime($time) )[7];            # get day of year
		my $week = int( ( $yday / 365 ) * 52 ) + 1;    # convert to weeks 1..52
		if ( $week == 53 ) {
	
			# this happens when the date is at the end of December, so just call it 52
			$week = 52;
		}
		$bin = $week;
	}
	elsif ($bin_unit eq 'Month') {
		$bin = $m;
	}
	elsif ($bin_unit eq 'Year') {
		$bin = 1;
	}

	return ( $year, $bin );
}

sub process_request {
	my $Entry = shift;
	my ( $year, $bin ) = process_date($Entry);
	unless ( exists $req{$year} ) {

		my $max;
		if ($bin_unit eq 'Week') {
			$max = 52;
		}
		elsif ($bin_unit eq 'Month') {
			$max = 12;
		}
		elsif ($bin_unit eq 'Year') {
			$max = 1;
		}
		# each array: qc_count, seq_count, seq_size, up_count, up_size, del_count, del_size
		$req{$year} = { map { $_ => [ 0, 0, 0, 0, 0, 0, 0 ] } ( 1 .. $max ) };
	}
	if ( $Entry->scan_datestamp ) {
		my $size = $Entry->size;
		if ( $size eq q(.) ) {
			$size = 0;    # must be empty or lost?
		}
		if ( $Entry->hidden_datestamp and $Entry->last_size > $size ) {
			$size = $Entry->last_size;
		}
		$req{$year}{$bin}->[1] += 1;            # sequence count
		$req{$year}{$bin}->[2] += $size;        # sequence size
		if ( $Entry->upload_datestamp ) {
			$req{$year}{$bin}->[3] += 1;        # upload count
			$req{$year}{$bin}->[4] += $size;    # upload size
		}
		elsif ( $Entry->deleted_datestamp ) {
			$req{$year}{$bin}->[5] += 1;        # deleted count
			$req{$year}{$bin}->[6] += $size;    # deleted size
		}
	}
	else {
		$req{$year}{$bin}->[0] += 1;            # qc count
	}
}

sub process_analysis {
	my $Entry = shift;
	my ( $year, $bin ) = process_date($Entry);
	unless ( exists $anal{$year} ) {

		my $max;
		if ($bin_unit eq 'Week') {
			$max = 52;
		}
		elsif ($bin_unit eq 'Month') {
			$max = 12;
		}
		elsif ($bin_unit eq 'Year') {
			$max = 1;
		}
		# each array: count, size, up_count, up_size, del_count, del_size
		$anal{$year} = { map { $_ => [ 0, 0, 0, 0, 0, 0 ] } ( 1 .. $max ) };
	}
	my $size = $Entry->size;
	if ( $size eq q(.) ) {
		$size = 0;    # must be empty?
	}
	if ( $Entry->hidden_datestamp and $Entry->last_size > $size ) {
		$size = $Entry->last_size;
	}
	$anal{$year}{$bin}->[0] += 1;            # count
	$anal{$year}{$bin}->[1] += $size;        # size
	if ( $Entry->upload_datestamp ) {
		$anal{$year}{$bin}->[2] += 1;        # upload count
		$anal{$year}{$bin}->[3] += $size;    # upload size
	}
	elsif ( $Entry->deleted_datestamp ) {
		$anal{$year}{$bin}->[4] += 1;        # deleted count
		$anal{$year}{$bin}->[5] += $size;    # deleted size
	}
}

sub write_req {
	my @today = localtime(time);
	my $cur_year = $today[5] + 1900;
	my $cur_week = int( ( $today[7] / 365 ) * 52 ) + 1;
	my $cur_month = $today[4] + 1;
	my $base  = $cat_file;
	$base =~ s/\.db$//;
	my $out = sprintf "%s.request.%s.%d%02d%02d.tsv", $base, $bin_unit, $cur_year,
		$today[4] + 1, $today[3];
	my $fh = IO::File->new( $out, '>' )
		or die "unable to write '$out'! $OS_ERROR\n";
	$fh->printf(
		"%s\n",
		join(
			"\t", 'Year', $bin_unit, qw(QC_Count SeqCount SeqSize
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
		foreach my $bin (sort {$a <=> $b} keys %{ $req{$year} } ) {
			next if ( $bin_unit eq 'Week' and $year == $cur_year and $bin > $cur_week );
			next if ( $bin_unit eq 'Month' and $year == $cur_year and $bin > $cur_month );
			$cumul_count    += $req{$year}{$bin}->[1];
			$cumul_size     += $req{$year}{$bin}->[2];
			$cumul_up       += $req{$year}{$bin}->[3];
			$cumul_up_size  += $req{$year}{$bin}->[4];
			$cumul_del      += $req{$year}{$bin}->[5];
			$cumul_del_size += $req{$year}{$bin}->[6];
			$fh->printf(
				"%s\n",
				join(
					"\t",
					$year,
					$bin,
					$req{$year}{$bin}->[0],
					$req{$year}{$bin}->[1],
					size_in_gb( $req{$year}{$bin}->[2] ),
					$req{$year}{$bin}->[3],
					size_in_gb( $req{$year}{$bin}->[4] ),
					$req{$year}{$bin}->[5],
					size_in_gb( $req{$year}{$bin}->[6] ),
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
	my $cur_year = $today[5] + 1900;
	my $cur_week = int( ( $today[7] / 365 ) * 52 ) + 1;
	my $cur_month = $today[4] + 1;
	my $base  = $cat_file;
	$base =~ s/\.db$//;
	my $out = sprintf "%s.analysis.%s.%d%02d%02d.tsv", $base, $bin_unit, $cur_year,
		$today[4] + 1, $today[3];
	my $fh = IO::File->new( $out, '>' )
		or die "unable to write '$out'! $OS_ERROR\n";
	$fh->printf(
		"%s\n",
		join(
			"\t", 'Year', $bin_unit, qw(Count Size
				UploadCount UploadSize DeleteCount DeleteSize
				CumulCount CumulSize CumulUploadCount CumulUploadSize
				CumulDeleteCount CumulDeleteSize)
		)
	);
	foreach my $year ( sort { $a <=> $b } keys %anal ) {
		my $cumul_count    = 0;
		my $cumul_size     = 0;
		my $cumul_up       = 0;
		my $cumul_up_size  = 0;
		my $cumul_del      = 0;
		my $cumul_del_size = 0;
		foreach my $bin (sort {$a <=> $b} keys %{ $anal{$year} } ) {
			next if ( $bin_unit eq 'Week' and $year == $cur_year and $bin > $cur_week );
			next if ( $bin_unit eq 'Month' and $year == $cur_year and $bin > $cur_month );
			$cumul_count    += $anal{$year}{$bin}->[0];
			$cumul_size     += $anal{$year}{$bin}->[1];
			$cumul_up       += $anal{$year}{$bin}->[2];
			$cumul_up_size  += $anal{$year}{$bin}->[3];
			$cumul_del      += $anal{$year}{$bin}->[4];
			$cumul_del_size += $anal{$year}{$bin}->[5];
			$fh->printf(
				"%s\n",
				join(
					"\t",
					$year,
					$bin,
					$anal{$year}{$bin}->[0],
					size_in_gb( $anal{$year}{$bin}->[1] ),
					$anal{$year}{$bin}->[2],
					size_in_gb( $anal{$year}{$bin}->[3] ),
					$anal{$year}{$bin}->[4],
					size_in_gb( $anal{$year}{$bin}->[5] ),
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

