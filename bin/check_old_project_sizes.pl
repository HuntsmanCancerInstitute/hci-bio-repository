#!/usr/bin/env perl

use warnings;
use strict;
use English qw(-no_match_vars);
use Getopt::Long;
use IO::File;
use Text::CSV;
use List::Util qw(mesh);
use FindBin qw($Bin);
use lib "$Bin/../lib";
use RepoCatalog;
use RepoProject;

our $VERSION = 0.1;

my $doc = <<DOC;

 A script to check old project previous sizes.
 
 This sums the file sizes from the manifest file and compares it to the
 last_size value in the catalog. A lot of old projects got their size
 accidentally updated when re-scanned. The current size should reflect
 reality on disk.
 
 This will only check deleted projects, not current or hidden projects.
 
 If indicated, it will update the last_size value in the catalog if the
 calculated value is larger by at least two fold.
 
 A comparative size is printed for every project to standard out.
 
VERSION: $VERSION

USAGE:

    check_old_project_sizes.pl -c catalog.db

OPTIONS:
  
  -c --cat <path>         Path to metadata catalog database
  -p --project <id>       Check the indicated project. Repeat as necessary.
                            Or simply append to the end of the command.
                            Default checks all projects in the Catalog.
  --list <file>           Provide a list of project IDs.
  -u --update             Update the last_size value in the Catalog.
                             Default simply prints the sizes.
  -h --help               Show this help


DOC

my $cat_file;
my @project_ids;
my $list_file;
my $update;
my $help;

if (@ARGV) {
	GetOptions(
		'c|catalog=s'           => \$cat_file,
		'p|project=s'           => \@project_ids,
		'list=s'                => \$list_file,
		'u|update!'             => \$update,
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

# Open catalog file
unless ($cat_file) {
	print 'FATAL: No catalog provided!\n';
	exit 1;
}
my $Cat = RepoCatalog->new($cat_file)
	or die "Cannot open catalog file '$cat_file'!\n";


# Collect project IDs
if (@ARGV) {
	push @project_ids, @ARGV;
}
if ($list_file) {
	my $fh = IO::File->new($list_file);
	unless ($fh) {
		printf " Unable to open file '%s': %s\n", $list_file, $OS_ERROR;
		exit 1;
	}
	while ( my $line = $fh->getline ) {
		if ($line =~ /^ ( A\d+ | \d+R )\b /x) {
			push @project_ids, $1;
		}
	}
	$fh->close;
	printf " Loaded %d IDs from file '%s'\n", scalar(@project_ids), $list_file;
}
unless (@project_ids) {
	@project_ids = $Cat->list_all;
}


## process projects
foreach my $id ( @project_ids ) {
	my $Entry = $Cat->entry($id) or next;
	if ( $Entry->scan_datestamp > 0 and $Entry->deleted_datestamp > 0 ) {
		process_project($Entry);
	}
	else {
		next;
	}
}

sub process_project {
	local $OUTPUT_AUTOFLUSH = 1; # piping hot output to monitor in real time
	my $E    = shift;
	my $path = $E->path;
	my $Proj = RepoProject->new($path) or return;
	my $csv  = Text::CSV->new();
	my $mf   = sprintf "%s/%s", $path, $Proj->manifest_file;
	if ( not -e $mf ) {
		# this may be an old TSV file instead
		$mf =~ s/csv/txt/;
		if ( -e $mf ) {
			# we have an old style file
			undef $csv;
			$csv = Text::CSV->new( { sep_char => "\t" } );
		}
		else {
			printf " ! %s manifest %s does not exist\n", $E->id, $Proj->manifest_file;
			return;
		}
	}
	unless ($csv) {
		die sprintf(" FATAL: csv object not created for %s!\n", $E->id);
	}
	unless (-s $mf) {
		printf " ! %s has empty manifest\n", $E->id;
		return;
	}
	my $fh = IO::File->new( $mf );
	unless ($fh) {
		printf " ! cannot open %s\n", $mf;
		return;
	}
	my $header = $csv->getline($fh);
	my $size   = 0;
	while ( my $data = $csv->getline($fh) ) {
		my %file = mesh $header, $data;
		my $s    = $file{Size} || 0;
		if ( $s =~ /^\d+$/ ) {
			$size += int($s);
		}
	}
	$fh->close;
	my $pmf = sprintf "%s/%s", $path, $Proj->previous_manifest_file;
	if (-e $pmf) {
		$fh = IO::File->new( $mf );
		unless ($fh) {
			printf " ! cannot open %s\n", $pmf;
			return;
		}
		$header = $csv->getline($fh);
		while ( my $data = $csv->getline($fh) ) {
			my %file = mesh $header, $data;
			my $s    = $file{Size} || 0;
			if ( $s =~ /^\d+$/ ) {
				$size += int($s);
			}
		}
		$fh->close;
	}
	return unless $size;
	my $old = $E->last_size || 1;   # make it at least 1 byte
	if ( $size > $old and ( $size / $old ) > 2 ) {
		printf " ! %s compare manifest %s to catalog %s\n", $E->id, _format_size($size),
			_format_size($old);
		if ($update) {
			printf "    updating last_size to %d\n", $size;
			# last_size() was intended as a read-only method so we have to 
			# dig into object and change it
			$E->last_size($size);
		}
	}
	else {
		printf " > %s compare manifest %s to catalog %s\n", $E->id, _format_size($size),
			_format_size($old);
	}
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





