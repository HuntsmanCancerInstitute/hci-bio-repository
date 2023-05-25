#!/usr/bin/env perl

use warnings;
use strict;
use English qw(-no_match_vars);
use Getopt::Long;
use IO::File;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use RepoCatalog;
use Net::SB;

our $VERSION = 0.2;


######## Documentation
my $doc = <<END;

A quick script to generate new prefixes for export purposes of existing 
projects on the Seven Bridges platform.

This uses the information from the GNomEx database in preference to
generate the path prefix for the AWS bucket in Core browser based on a
combination of the group folder, identifier, and given name. Some logic is
employed to generate reasonable names in default situations. An abundance
of regular expressions are used to clean up the names into acceptable
paths.

This requires a catalog database file of all GNomEx projects; see 
manage_repository application. It will search for projects in the lab 
division on the SBG platform, look up the original information in the 
Catalog, and generate an appropriate prefix. Projects based on GNomEx 
identifiers not found in the Catalog are skipped with warnings. 

This will descend into Legacy GNomEx projects and look up legacy projects 
as well.

Project names are truncated to between 20-30 characters, preferrably
at word boundaries, when generating prefixes. Prefixes are checked for 
uniqueness. 

The output file is a tab-delimited file of SBG division ID and new prefix.

USAGE

generate_project_export_prefixes.pl <catalog> <division> <out_file.txt>

OPTIONS
   -c --catalog     <file>      The catalog db file of GNomEx experiments
   -d --division    <text>      The lab division name to check
   -o --out         <file>      The output file of ID and prefix mappings
   -e --exclude     <file>      An optional list of SBG IDs to exclude/skip
   -h --help                    This help


END


my $division;
my $cat_file;
my $out_file;
my $exclude_file;
my $help;
my $core_members = qr/^(?: nix | boyle | atkinson | parnell | lohman | li )/xi;

if (scalar(@ARGV) > 0) {
	GetOptions(
		'c|catalog=s'       => \$cat_file,
		'd|division=s'      => \$division,
		'o|out=s'           => \$out_file,
		'e|exclude=s'       => \$exclude_file,
		'h|help!'           => \$help,
	) or die "please recheck your options!! Run with --help to display options\n";
}
else {
	print $doc;
	exit 1;
}

if ($help) {
	print $doc;
	exit 0;
}


# check options
unless ($cat_file and $division and $out_file) {
	print STDERR " Options catalog, division and output file are required!\n";
	exit 1;
}

# Exceptions
my %exclude;
if ($exclude_file) {
	my $fh = IO::File->new($exclude_file) or
		die "Unable to open '$exclude_file' $OS_ERROR";
	while (my $line = $fh->getline) {
		chomp $line;
		$exclude{$line} = 1;
	}
	$fh->close;
	printf " collected %d exclusions\n", scalar keys %exclude;
}

# Open Objects
my $Sb = Net::SB->new(
	div        => $division,
) or die " unable to initialize SB object!\n";


# Open catalog
my $Cat = RepoCatalog->new($cat_file) or 
	die "Cannot open catalog file '$cat_file'!\n";


# Open output filehandle
my $outfh = IO::File->new($out_file, 'w') or
	die " Cannot open output fiile '$out_file' $OS_ERROR";


# Work through projects
my %prefixes;
my $projects = $Sb->list_projects;
printf " identifed %d projects in %s\n", scalar( @{ $projects } ), $division;
foreach my $Project ( @{$projects} ) {

	# Check for exclusion
	if ( exists $exclude{ $Project->id } ) {
		next;
	}

	# request
	if ( $Project->id =~ /^ $division \/ (\d+)r $/x ) {
		my $id = $1 . 'R';
		my $prefix = process_project($id);
		unless ($prefix) {
			# no name, fake it
			$prefix = make_prefix( $id, q(), 'Unknown' );
		}
		$outfh->printf( "%s\t%s\n", $Project->id, $prefix);
	}
	
	# analysis
	elsif ( $Project->id =~ /^ $division \/ a(\d+) $/x ) {
		my $id = 'A' . $1;
		my $prefix = process_project($id);
		unless ($prefix) {
			# no name, fake it
			$prefix = make_prefix( $id, q(), 'Unknown' );
		}
		$outfh->printf( "%s\t%s\n", $Project->id, $prefix);
	}
	
	# legacy
	elsif ( $Project->id eq "$division/$division" ) {

		# need to list the individual projects
		my $folders = $Project->recursive_list( q(), 2) ;
		foreach my $Folder ( @{ $folders } ) {
			if ( $Folder->type eq 'file' ) {
				# why are files in here?????
				printf STDERR " ! Wierd file %s in $division/$division\n", $Folder->name;
				next;
			} 
			my $gnomex_id = $Folder->name;
			next if ( $gnomex_id eq 'request' or $gnomex_id eq 'analysis' );
			if ( $gnomex_id !~ /^ A? \d+ R? $/x ) {
				print STDERR " ! Wierd directory $gnomex_id in $division/$division\n";
				next;
			}
			my $name = sprintf("%s/%s/%s", $division, $division, $Folder->pathname);
			next if ( exists $exclude{ $name } );
			my $prefix = process_project($gnomex_id, $Folder);
			unless ($prefix) {
				# no name, fake it
				$prefix = make_prefix( $gnomex_id, q(), 'Unknown' );
				next;
			}
			$outfh->printf( "%s\t%s\n", $name, $prefix);
		}
	}
	
	# user generated project
	else {
		my $name  = $Project->name;
		my $Owner = $Project->created_by;
		my $group;
		if ( $Owner and $Owner->last_name ) {
			if ( $Owner->last_name !~ $core_members ) {
				$group = sprintf "%s-%s", $Owner->first_name, $Owner->last_name;
			}
			else {
				$group = q();
			}
		}
		elsif ( $Owner and $Owner->username ) {
			# abandoned accounts lose the ability to retrieve the first and last name
			# but we should still have their username so use that
			$group = $Owner->username;
		}
		else {
			$group = q();
		}
		my $prefix = make_prefix( q(), $name, $group );
		$outfh->printf( "%s\t%s\n", $Project->id, $prefix);
	}
}

printf " processed %d projects from $division\n", scalar keys %prefixes;




#### Subroutines

sub process_project {
	my $id = shift;
	my $Folder = shift || undef;

	my $Entry = $Cat->entry($id);
	unless ($Entry) {
		print STDERR "cannot find Catalog entry for $id! skipping\n";
		return;
	}
	my $group = $Entry->group;
	my $name  = $Entry->name;
	if ( $group =~ /^ (?: experiment s? | project s? ) \s for \s .+/xi ) {
		$group = sprintf "%s-%s", $Entry->user_first, $Entry->user_last;
	}
	elsif ( $group =~ /^Request s? \s submitted \s by \s .+/xi ) {
		$group = sprintf "%s-%s", $Entry->user_first, $Entry->user_last;
	}
	elsif ( $group =~ /^submitted \s for \s .+/xi ) {
		$group = sprintf "%s-%s", $Entry->user_first, $Entry->user_last;
	}
	elsif ( $group eq 'Novoalignments' ) {
		$group = 'Autoaligner';
	}
	elsif ( $group eq 'tomato' ) {
		$group = 'Autoaligner';
	}
	if ( $name =~ /^Alignment \s Analysis \s on \s \w+ day, \s (.+)$/x ) {
		my $date = $1;
		if ($Folder) {
			# try to ascertain the original request project from the file names
			my $list = $Folder->list_contents;
			my $req_id;
			foreach my $f ( @{ $list } ) {
				if ( $f->name =~ /^ (\d+) X \d+/x ) {
					$req_id = $1;
					last;
				}
			}
			if ($req_id) {
				$name = sprintf "Alignment for %sR", $req_id;
			}
			else {
				$name = "Alignment $date";
			}
		}
		else {
			$name = "Alignment $date";
		}
	}
	elsif ( not $name ) {
		my $user = $Entry->user_first;
		if ($group =~ /^ $user \-/x) {
			# user name is the group, so make up a name based on date
			if ($Entry->is_request) {
				$name = sprintf "Request %s", $Entry->date;
			}
			else {
				$name = sprintf "Analysis %s", $Entry->date;
			}
		}
		else {
			# put user name in the name
			$name = sprintf "%s-%s_%s", $Entry->user_first, $Entry->user_last,
				$Entry->date;
		}
	}
	return make_prefix($id, $name, $group);
}


sub make_prefix {
	my ($id, $name, $group) = @_;

	# clean up the names of all weird characters
	foreach ($id, $name, $group) {
		next unless length;

		# word replacement
		s/#(\d)/No$1/g;
		s/&/-/g;
		s/and/-/g;
		s/\@/-at-/g;
		s/%/percent/g;
		s/\+\/\-/-/g;
		s/\+/plus-/g;

		## no critic - it doesn't understand xx modifiers?

		# just remove - most of these won't ever be seen but just in case
		s/[ ' " \# \! \^ \* \{ \} \[ \] \> \< ~ ]+ //gxx;

		# dash replacement
		s/[ \- : ; \| \( \) \/ ]+ /-/gxx;

		# underscore replacement
		s/[ \s _ , ]+ /_/gxx;

		# stray beginning and ending characters
		s/^[ \s \- _ ]+//xx;
		s/[ \s \- _ ]+ $//xx;

		## use critic

		# weird combinations
		s/_\-/-/g;
		s/\-_/-/g;
		s/\-{2,}/-/g;
		s/_{2,}/_/g;
		s/\._/_/g;
		
		# check length
		if (length > 30) {
			# split naturally on a word after 20 characters
			my $i = index $_, q(_), 20;
			if ($i < 30) {
				$_ = substr $_, 0, $i;
			}
			else {
				# no word after 30 characters? try a dash delimiter
				$i = index $_, q(-), 20;
				if ($i < 30) {
					$_ = substr $_, 0, $i;
				}
				else {
					# hard cutoff after 30 characters
					$_ = substr $_, 0, 30;
				}
			}
		}
	}

	# compose prefix
	my $prefix;
	if (length $group and length $id and length $name) {
		$prefix = sprintf "%s/%s--%s", $group, $id, $name;
	}
	elsif (length $group and length $id) {
		$prefix = sprintf "%s/%s", $group, $id;
	}
	elsif (length $group and length $name) {
		$prefix = sprintf "%s/%s", $group, $name;
	}
	else {
		$prefix = $name;
	}
	
	# check uniqueness
	if ( exists $prefixes{ $prefix } ) {
		print STDERR " ! non-unique prefix $prefix\n";
		$prefixes{ $prefix } += 1;
		$prefix .= '-' . $prefixes{ $prefix };
	}
	else {
		$prefixes{ $prefix } = 0;
	}
	
	return $prefix;
}


