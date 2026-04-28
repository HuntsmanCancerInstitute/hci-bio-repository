#!/usr/bin/env perl

use warnings;
use strict;
use English qw(-no_match_vars);
use IO::File;
use IO::Handle;
use Text::CSV;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use Gnomex;

our $VERSION = '1.1';

my $outfile;
my @ids;
if (@ARGV) {
	$outfile = shift @ARGV;
	@ids = @ARGV;
}
else {
	print <<~DOC;

	This script will write a table of the samples from the GNomEx database for
	one or more experiment Request identifers. Exported values include the
	Sample ID, Sample Name, Sample Type, Organism, Application, and boolean
	indicators whether the sample failed QC checks or library preparation.
	
	Custom sample attributes, if any, are not included, and should be obtained
	directly from GNomEx.
	
	By default, a CSV file is written, unless the output file has a ".tsv"
	extension and tab-delimited is written. To write to standard out, use
	"stdout" as the filename; tabs are used when writing to standard out.
	
	If more than one ID is supplied, the ID is prefixed to each row as an
	additional column.
	
	Version: $VERSION
	
	Usage:
	    get_gnomex_samples.pl <output> <ID1 ID2 ...>
	
	Examples:
	    get_gnomex_samples.pl out.csv 12345R
	    get_gnomex_samples.pl stdout  12345R
	    
	
	DOC
	exit 0;
}


# initalize
my $GNomEx = Gnomex->new() or die "can't instantiate Gnomex object!\n";


# prepare output file
my $csv;
my $out_fh;
if ($outfile eq 'stdout') {
	$out_fh = IO::Handle->new;
	$out_fh->fdopen(fileno(STDOUT), "w")
		or die "can't write to stdandard out!\n";
	$csv = Text::CSV->new( {
		eol => "\n",
		sep_char => "\t",
		quote_space => 0
	} );
}
elsif ( $outfile =~ /\.tsv$/i ) {
	$out_fh = IO::File->new($outfile, '>')
		or die "can't write to file $outfile! $OS_ERROR\n";
	$csv = Text::CSV->new( {
		eol         => "\n",
		sep_char    => "\t",
		quote_space => 0
	} );
}
else {
	unless ( $outfile =~ /\.csv$/i ) {
		$outfile .= '.csv';
		printf " > writing to %s\n", $outfile;
	}
	$out_fh = IO::File->new($outfile, '>')
		or die "can't write to file $outfile! $OS_ERROR\n";
	$csv    = Text::CSV->new( { eol => "\n" } );
}

# iterate through request ID list
my $multi = scalar(@ids) > 1 ? 1 : 0;
my $header_done = 0;
foreach my $id (@ids) {

	# fetch samples
	my $samples = $GNomEx->fetch_request_samples($id);
	next unless (scalar @{$samples} > 0);
	unless ($outfile eq 'stdout') {
		printf " > collected %d samples for %s\n", scalar( @{$samples} ) - 1, $id;
	}
	
	# print the header which is the first element in the array
	unless ($header_done) {
		my @h = @{ $samples->[0] };
		if ($multi) {
			unshift @h, 'RequestID';
		}
		$csv->print( $out_fh, \@h );
		$header_done = 1;
	}
	
	# print samples
	for my $i ( 1 .. $#{$samples} ) {
		my @data = @{ $samples->[$i] };
		if ($multi) {
			unshift @data, $id;
		}
		$csv->print( $out_fh, \@data );
	}
}
$out_fh->close;

