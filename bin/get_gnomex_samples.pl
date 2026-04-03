#!/usr/bin/env perl

use warnings;
use strict;
use English qw(-no_match_vars);
use IO::File;
use Text::CSV;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use RepoCatalog;
use Gnomex;

our $VERSION = '1.0.0';

my $outfile;
my @ids;
if (@ARGV) {
	$outfile = shift @ARGV;
	@ids = @ARGV;
}
else {
	print <<~DOC;

	This script will write a CSV file of the samples from the GNomEx database
	for one or more experiment Request identifers. Exported values include the
	Project ID, Sample ID, Sample Name, Sample Type, Organism, Application,
	and boolean indicators whether the sample failed QC checks or library
	preparation.
	
	Custom sample attributes, if any, are not included, and should be obtained
	directly from GNomEx.
	
	Version: $VERSION
	
	Usage:
	    get_gnomex_samples.pl <output.csv> <ID1 ID2 ...>
	
	Example:
	    get_gnomex_samples.pl out.csv 12345R
	
	DOC
	exit 0;
}


# initalize
my $GNomEx = Gnomex->new() or die "can't instantiate Gnomex object!\n";


# prepare output file
my $csv    = Text::CSV->new( { eol => "\n" } );
my $out_fh = IO::File->new($outfile, '>')
	or die "can't write to file $outfile! $OS_ERROR\n";

# iterate through request ID list
my $header_done = 0;
foreach my $id (@ids) {

	# fetch samples
	my $samples = $GNomEx->fetch_request_samples($id);
	next unless (scalar @{$samples} > 0);
	printf " > collected %d samples for %s\n", scalar( @{$samples} ) - 1, $id;
	
	# print the header which is the first element in the array
	unless ($header_done) {
		my @h = @{ $samples->[0] };
		unshift @h, 'RequestID';
		$csv->print( $out_fh, \@h );
		$header_done = 1;
	}
	
	# print samples
	for my $i ( 1 .. $#{$samples} ) {
		my @data = @{ $samples->[$i] };
		unshift @data, $id;
		$csv->print( $out_fh, \@data );
	}
}
$out_fh->close;

