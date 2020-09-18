#!/usr/bin/env perl

use strict;
use List::Util qw(sum0);
use Time::Piece;
use Bio::ToolBox;

unless (@ARGV) {
	print "script to combine lab sizes and dates\n$0 analfile reqfile outfile\n";
	exit;
}

my $analfile = shift @ARGV;
my $reqfile  = shift @ARGV;
my $outfile  = shift @ARGV;


my %lab2sizes;


# Analysis file
my $Data = Bio::ToolBox->load_file($analfile) or 
	die "can't open $analfile\n";
printf "loaded $analfile with %s rows\n", $Data->last_row;

my $labfirst = $Data->find_column('LabFirstName') or 
	die "can't find LabFirstName column!";
my $lablast  = $Data->find_column('LabLastName') or 
	die "can't find LabLastName column!";
my $size     = $Data->find_column('size') or 
	die "can't find size column!";
my $date     = $Data->find_column('date');
$Data->iterate( sub {
	my $row = shift;
	my $name = join('_', $row->value($labfirst), $row->value($lablast));
	my $size = $row->value($size);
	$size =~ s/[\s\-,]//g; # remove stray characters from Excel
	$lab2sizes{$name} ||= {
		anal     => [],
		analdate => [],
		req      => [],
		reqdate  => []
	};
	push @{ $lab2sizes{$name}{anal} }, $size;
	push @{ $lab2sizes{$name}{analdate}}, 
		Time::Piece->strptime($row->value($date), "%m/%d/%Y");
});
undef $Data;

# Request file
$Data = Bio::ToolBox->load_file($reqfile) or 
	die "can't open $analfile\n";
printf "loaded $reqfile with %s rows\n", $Data->last_row;

$labfirst = $Data->find_column('LabFirstName') or 
	die "can't find LabFirstName column!";
$lablast  = $Data->find_column('LabLastName') or 
	die "can't find LabLastName column!";
$size     = $Data->find_column('size') or 
	die "can't find size column!";
$date     = $Data->find_column('date');
$Data->iterate( sub {
	my $row = shift;
	my $name = join('_', $row->value($labfirst), $row->value($lablast));
	my $size = $row->value($size);
	$size =~ s/[\s\-,]//g; # remove stray characters from Excel
	$lab2sizes{$name} ||= {
		anal     => [],
		analdate => [],
		req      => [],
		reqdate  => []
	};
	push @{ $lab2sizes{$name}{req} }, $size;
	push @{ $lab2sizes{$name}{reqdate}}, 
		Time::Piece->strptime($row->value($date), "%m/%d/%Y");
});
undef $Data;

# Output
# $Data = Bio::ToolBox->new_data(qw(LabFirstName LabLastName Requests RequestSizes 
# 	RequestTotalSize Analyses AnalysisSizes AnalysisTotalSize));
$Data = Bio::ToolBox->new_data(qw(LabFirstName LabLastName RequestNumber 
	RequestTotalSize LastRequestDate AnalyseNumber AnalysisTotalSize LastAnalysisDate));
foreach my $name (sort {$a cmp $b} keys %lab2sizes) {
	my ($first, $last) = split('_', $name);
	# for now assuming that the original input files are sorted by increasing project 
	# number and hence date, so no need to sort the dates
	$Data->add_row( [
		$first,
		$last,
		scalar(@{ $lab2sizes{$name}{req} }),
# 		join(',', map {sprintf("%.1f",  $_/1048576)} @{$lab2sizes{$name}{req}} ),
		sprintf("%.1f", sum0(@{$lab2sizes{$name}{req}})),
		defined $lab2sizes{$name}{reqdate}->[-1] ? 
			$lab2sizes{$name}{reqdate}->[-1]->strftime("%m/%d/%Y") : '',
		scalar(@{ $lab2sizes{$name}{anal} }),
# 		join(',', map {sprintf("%.1f", $_/1048576)} @{$lab2sizes{$name}{anal}} ),
		sprintf("%.1f", sum0(@{$lab2sizes{$name}{anal}})),
		defined $lab2sizes{$name}{analdate}->[-1] ? 
			$lab2sizes{$name}{analdate}->[-1]->strftime("%m/%d/%Y") : ''
	] );
}
printf "final $outfile has %s rows\n", $Data->last_row;
$Data->save($outfile);

