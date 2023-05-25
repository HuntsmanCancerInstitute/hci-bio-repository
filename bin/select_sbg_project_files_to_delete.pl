#!/usr/bin/env perl

use warnings;
use strict;
use English qw(-no_match_vars);
use Getopt::Long qw(:config no_ignore_case);
use IO::File;

our $VERSION = 0.1;

my $listfile;
my $outfile;
my $md5;
my $index;
my $log;
my $filter;

my $doc = <<END;

A script to filter out the unwanted files to target for deletion.

Many files are either redundant or not necessary to be kept and 
can be easily regenerated. When looking to save on file counts, 
for example when manually exporting lots of files, this might help.

MD5 files in the Legacy project are redundant (they're in the manifest).

Index files (bai, tbi, crai) can easily be recreated.

Other files could be selected through regular expression.

This works on recursive file listing generated with sbg_project_manager.
It does not actually delete the files. Use the output file of selected 
files as input to sbg_project_manager to delete.


Options

--list    <file>    The list of all files in the project
                        generate with sbg_project_manager.pl
--out     <file>    The output file written
-m                  Select md5 files (.md5)
-i                  Select index files ( .bai .tbi .crai )
-l                  Select log files (.log)
-f        <regex>   Custom file filter as Perl Regular Expression

END

if (@ARGV) {
	GetOptions(
		'list=s'           => \$listfile,
		'out=s'            => \$outfile,
		'm!'               => \$md5,
		'i!'               => \$index,
		'l!'               => \$log,
		'f=s'              => \$filter,
	) or die " bad options! Please check\n $doc\n";
}
else {
	print $doc;
	exit 0;
}

my $infh = IO::File->new($listfile)
	or die " unable to read $listfile! $OS_ERROR";

my $outfh = IO::File->new($outfile, '>')
	or die " unable to write $outfile! $OS_ERROR";

# header
my $header = $infh->getline;
$outfh->print($header);

# files
my $count = 0;
while (my $line = $infh->getline) {
	chomp $line;
	my $keep = 0;
	$keep++ if ( $md5    and $line =~ /\.md5$/ );
	$keep++ if ( $index  and $line =~ /\. (?: bai | tbi | crai ) $/xi );
	$keep++ if ( $log    and $line =~ /\.log$/ );
	$keep++ if ( $filter and $line =~ /$filter/ );
	if ($keep) {
		$outfh->printf("%s\n", $line);
		$count++;
	}
}

# finish
$infh->close;
$outfh->close;
printf "\n Selected %d files written to %s\n", $count, $outfile;


