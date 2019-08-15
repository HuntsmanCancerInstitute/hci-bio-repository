#!/usr/bin/perl

use strict;
use IO::File;
use File::Spec;
use List::MoreUtils qw(first_index);
use Getopt::Long;

my $VERSION = 1.0;


### Documentation
my $doc = <<END;

A simple script to look up the Seven Bridges division name based 
on the Lab First and Last name, and compose a Seven Bridges access URL 
for the project.

It assumes a tab-delimited text file of a list of projects.
It needs the columns 'RequestNumber' or 'AnalysisNumber' as an ID, as  
well as 'LabFirstName' and 'LabLastName' columns. 


It will overwrite the input file unless provided an output file name.

Usage:
    add_division_url.pl -i upload.20190709.togo.txt -o output.txt


Options:
    -i --input <file>       Input file, such as from fetch_genomex_data.pl
    -o --output <file>      Output file, default overwrite input
    -d --division <file>    Lab name to division lookup file

END





### Command line options
my $infile;
my $outfile;
my $divfile;

if (scalar(@ARGV) > 1) {
	GetOptions(
		'in=s'          => \$infile,
		'out=s'         => \$outfile,
		'division=s'    => \$divfile,
	) or die "please recheck your options!\n";
}
else {
	print $doc;
	exit;
}

# check inputs
unless ($infile) {
	if (@ARGV) {
		$infile = shift @ARGV
	}
	else {
		die " no input file!\n";
	}
}
unless ($outfile) {
	# overwrite
	$outfile = $infile;
}
unless ($divfile) {
	die " no division file!\n";
}


#### Load credentials
my %lab2division = load_lookup_file();



#### Load Input file
my $id_index;
my $fname_index;
my $lname_index;
my ($columns, $data) = load_input_file();
print " Project ID is at index $id_index, Lab Name at $fname_index and $lname_index\n";



#### Process file

# Open output file
my $fh = IO::File->new($outfile, 'w') or die "can't write to $outfile! $!\n";

# New colunns
push @$columns, 'Division','URL';
$fh->printf("%s\n", join("\t", @$columns));


# Generate URL and write to output
foreach my $d (@$data) {
	chomp $d;
	my @fields = split /\t/, $d;
	
	# check for division name
	my $lab = sprintf("%s %s", $fields[$fname_index], $fields[$lname_index]);
	my $sbname = $lab2division{$lab} || '';
	
	# generate URL
	my $url;
	if ($sbname and $fields[$id_index]) {
		$url = sprintf("https://igor.sbgenomics.com/u/%s/%s", $sbname, 
		lc($fields[$id_index]) );
	}
	
	# print
	push @fields, $sbname, $url;
	$fh->printf("%s\n", join("\t", @fields));
}
$fh->close;

print " Wrote $outfile\n";




sub load_lookup_file {
	my $fh = IO::File->new($divfile) or 
		die "unable to open lab to division lookup file '$divfile'!\n$!\n";
	my %name2div;
	my $header = $fh->getline;
		# assume this file is correct, I'm not checking it
	while (my $line = $fh->getline) {
		chomp $line;
		my ($lab, $div) = split /\t/, $line;
		$name2div{$lab} = $div;
	}
	$fh->close;
	printf " Loaded %d labs from lookup file\n", scalar(keys %name2div);
	return %name2div;
}


sub load_input_file {
	
	# Load input file 
	my $fh = IO::File->new($infile) or 
		die "unable to open file '$infile'! $!\n";
	my @data = $fh->getlines;
	$fh->close;

	# Identify indexes
	my $header = shift @data;
	chomp $header;
	my @columns = split /\t/, $header;
	$id_index = first_index {1 if ($_ eq 'RequestNumber' or $_ eq 'AnalysisNumber') } @columns;
	$fname_index = first_index {1 if $_ eq 'LabFirstName'} @columns;
	$lname_index = first_index {1 if $_ eq 'LabLastName'} @columns;
	if ($id_index == -1) {
		die "Could not find index for RequestNumber or AnalysisNumber column!\n";
	}
	if ($fname_index == -1) {
		die "Could not find index Lab First Name column!\n";
	}
	if ($lname_index == -1) {
		die "Could not find index Lab Last Name column!\n";
	}

	return (\@columns, \@data);
}

