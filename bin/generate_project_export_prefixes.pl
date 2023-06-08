#!/usr/bin/env perl

use warnings;
use strict;
use English qw(-no_match_vars);
use Carp;
use Getopt::Long;
use IO::File;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use RepoCatalog;
use Net::SB;

our $VERSION = 0.3;


######## Documentation
my $doc = <<END;

A quick script to generate bucket names and prefixes for purposes of
exporting existing projects from the Seven Bridges platform to new 
AWS buckets.

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

The output file is a tab-delimited file of SBG division ID bucket, and 
new prefix.

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
my $verbose;
my $help;
my $core_members = qr/^(?: nix | boyle | atkinson | parnell | lohman | li )/xi;

if (scalar(@ARGV) > 0) {
	GetOptions(
		'c|catalog=s'       => \$cat_file,
		'd|division=s'      => \$division,
		'o|out=s'           => \$out_file,
		'e|exclude=s'       => \$exclude_file,
		'v|verbose!'        => \$verbose,
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
	printf " > collected %d exclusions\n", scalar keys %exclude;
}

# Open SBG Division
my $Sb = Net::SB->new(
	div        => $division,
	verbose    => $verbose,
) or die " unable to initialize SB object!\n";


# Open catalog
my $Cat = RepoCatalog->new($cat_file) or 
	die "Cannot open catalog file '$cat_file'!\n";


# Identify investigator
# relying on the fact that virtually all SBG divisions are named after PI
my ($div_pi_first, $div_pi_last) = split /\-/, $division, 2;
$div_pi_last =~ s/\-lab$//;
if ( $div_pi_last =~ /\-/ ) {
	my @bits = split /\-/, $div_pi_last;
	if ( $bits[0] !~ /^(?: hu | tristani | al | judson | t )$/xi ) {
		$div_pi_last =~ s/\-//; # exceptions
	}
	else {
		$div_pi_last = $bits[1]; # take the second name;
	}
}


# Main
my %bucket2count;
my %bucket2data;
my $generated_count = 0;
process_sbg_projects();
consolidate_buckets();
print_output();








#### Subroutines

sub process_sbg_projects {
	my $projects = $Sb->list_projects;
	printf " > identifed %d projects in %s\n", scalar( @{ $projects } ), $division;
	foreach my $Project ( @{$projects} ) {

		# Check for exclusion
		if ( exists $exclude{ $Project->id } ) {
			next;
		}

		# legacy
		if ( $Project->id eq "$division/$division" ) {

			# need to loop through the individual legacy projects
			# no filtering, limit recursive list to a depth of 2, which is just enough
			my $folders = $Project->recursive_list( q(), 2) ;
			foreach my $Folder ( @{ $folders } ) {

				# sanity checks to skip unwanted items
				if ( $Folder->type eq 'file' ) {
					# why are files in here?????
					printf STDERR " ! Weird file %s in $division/$division\n", $Folder->name;
					next;
				} 
				my $gnomex_id = $Folder->name;
				next if ( $gnomex_id eq 'request' or $gnomex_id eq 'analysis' );
				if ( $gnomex_id !~ /^ A? \d+ R? $/x ) {
					print STDERR " ! Weird directory $gnomex_id in $division/$division\n";
					next;
				}
				my $sbgid = sprintf("%s/%s/%s", $division, $division, $Folder->pathname);
				next if ( exists $exclude{ $sbgid } );

				# process the folder project
				my ($bucket, $prefix, $Entry) = 
					process_gnomex_project($gnomex_id, $Folder);
				unless ( $bucket and $prefix ) {
					print STDERR " ! Could not process $gnomex_id at $sbgid\n";
					next;
				}
				$bucket2count{$bucket} += 1;
				$bucket2data{$bucket} ||= [];
				push @{ $bucket2data{$bucket} }, [ $prefix, $sbgid, 1, $Entry ];
				$generated_count++;
			}

			next;
		}

		# process project
		my $bucket;
		my $prefix;
		my $Entry;
		if ( $Project->id =~ /^ $division \/ (a? \d+ r? )$/x ) {
			# uploaded request or analysis project
			my $gnomex_id = uc $1;
			($bucket, $prefix, $Entry) = process_gnomex_project($gnomex_id);
			unless ( $bucket and $prefix ) {
				printf STDERR " ! Could not process %s at %s\n", $gnomex_id, $Project->id;
				next;
			}
		}
		else {
			# novel project created on SBG platform
			# we need to get the user that created the project, but because we only 
			# get essentially an id, we then have to lookup that member from all 
			# members in the project
			# if the user account has lapsed, we won't have a full member object
			my $Owner = $Project->created_by;
			my @members = $Project->list_members;
			foreach my $member (@members) {
				if ( $member->username eq $Owner->username ) {
					$Owner = $member;
					last;
				}
			}
			
			my $group;
			if ( $Owner and $Owner->last_name ) {
				if ( $Owner->last_name !~ $core_members ) {
					$group = sprintf "%s-%s", $Owner->first_name, $Owner->last_name;
				}
				else {
					$group = 'sevenbridges';
				}
			}
			elsif ($Owner) {
				$group = $Owner->username;
			}
			else {
				$group = 'sevenbridges';
			}
			$bucket = make_bucket($div_pi_first, $div_pi_last, $group, 0);
			$prefix = cleanup( $Project->name );
		}
		$bucket2count{$bucket} += 1;
		$bucket2data{$bucket} ||= [];
		push @{ $bucket2data{$bucket} }, [ $prefix, $Project->id, 0, $Entry ];
		$generated_count++;
	}
	printf " > Generated %d project prefixes\n", $generated_count;
}


sub process_gnomex_project {
	my $id = shift;
	my $Folder = shift || undef;
	unless ($id) {
		confess "no id provided to process a gnomex project!";
	}

	# get details from Catalog entry
	my $Entry = $Cat->entry($id);
	unless ($Entry) {
		print STDERR " ! cannot find Catalog entry for $id! skipping\n";
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
	

	# Finish
	my $bucket = make_bucket($Entry->lab_first, $Entry->lab_last, $group,
		$Entry->is_request ? 1 : 2);
	my $prefix = make_prefix($id, $name);
	return ($bucket, $prefix, $Entry);
}


sub make_prefix {
	my ($id, $name) = @_;
	unless ($id or $name) {
		confess "no id or name provided to make prefix!";
	}

	# clean up the names of all weird characters
	if ($id) {
		$id = cleanup($id);
	}
	if ($name) {
		$name = cleanup($name);
	}

	# compose prefix
	my $prefix;
	if (length $id and length $name) {
		$prefix = sprintf "%s--%s", $id, $name;
	}
	elsif (length $id) {
		$prefix = $id;
	}
	elsif (length $name) {
		$prefix = $name;
	}
	else {
		print STDERR " ! error, no id or name provided!\n";
		return;
	}
	
	return $prefix;
}

sub make_bucket {
	my ($first, $last, $group, $type) = @_;
	$type ||= 0;   # 0=nothing, 1=request, 2=analysis
	unless ($group) {
		confess "no group provided to make bucket!";
	}

	# cleanup the group identifier
	$group = cleanup($group);
	# additional cleaning, no underscores allowed
	# periods can be used except with transfer acceleration, so skip? plus look funny?
	$group =~ s/[_\.]/-/g;

	# generate bucket
	my $bucket;
	if ($type) {
		$bucket = sprintf "%s%s-%s-%s", lc substr($first,0,1), lc substr($last,0,11), 
			$type == 1 ? 'exp' : 'ana', lc $group;
	}
	else {
		$bucket = sprintf "%s%s-%s", lc substr($first,0,1), lc substr($last,0,11), 
			lc $group;
	}
	return $bucket;
}

sub cleanup {
	my $name = shift;
	unless ($name) {
		confess "no name provided to cleanup!";
	}
	
	# word replacement
	$name =~ s/#(\d)/No$1/g;
	$name =~ s/&/-/g;
	$name =~ s/\s and \s/-/xg;
	$name =~ s/\@/-at-/g;
	$name =~ s/%/pct/g;
	$name =~ s/\+\/\-/-/g;
	$name =~ s/\+/plus-/g;

	## no critic - it doesn't understand xx modifiers?

	# just remove - most of these won't ever be seen but just in case
	$name =~ s/[ ' " \# \! \^ \* \{ \} \[ \] \> \< ~ ]+ //gxx;

	# dash replacement
	$name =~ s/[ \- : ; \| \( \) \/ ]+ /-/gxx;

	# underscore replacement
	$name =~ s/[ \s _ , ]+ /_/gxx;

	# stray beginning and ending characters
	$name =~ s/^[ \s \- _ ]+//xx;
	$name =~ s/[ \s \- _ ]+ $//xx;

	## use critic

	# weird combinations
	$name =~ s/_\-/-/g;
	$name =~ s/\-_/-/g;
	$name =~ s/\-{2,}/-/g;
	$name =~ s/_{2,}/_/g;
	$name =~ s/\._/_/g;
	
	# check length
	if (length($name) > 30) {
		# split naturally on a word after 20 characters
		my $i = index $name, q(_), 20;
		if ($i < 30) {
			$name = substr $name, 0, $i;
		}
		else {
			# no word after 30 characters? try a dash delimiter
			$i = index $name, q(-), 20;
			if ($i < 30) {
				$name = substr $name, 0, $i;
			}
			else {
				# hard cutoff after 30 characters
				$name = substr $name, 0, 30;
			}
		}
	}
	return $name;
}

sub consolidate_buckets {
	my $single_legacy_count = 0;
	my $singleton_count = 0;
	my %new_buckets;
	foreach my $bucket (keys %bucket2count) {
		next if $bucket2count{$bucket} > 1;
	
		# data is [ prefix, sbg_id, legacy_boolean, Entry ]
		# only consolidating legacy projects
		# only one bucket, so we know it is in position 0
		if ( $bucket2data{$bucket}->[0]->[2] ) {
			# make an alternate group
			my $alt_group;
			my $Entry = $bucket2data{$bucket}->[0]->[3];
			if ( $Entry->user_last !~ $core_members ) {
				$alt_group = sprintf "%s-%s", $Entry->user_first, $Entry->user_last;
			}
			else {
				$alt_group = 'legacy-gnomex';
			}
			my $newbucket = make_bucket($Entry->lab_first, $Entry->lab_last, $alt_group,
				$Entry->is_request ? 1 : 2);
		
			# copy into new hash key
			$bucket2count{$newbucket} += 1;
			$bucket2data{$newbucket} ||= [];
			push @{ $bucket2data{$newbucket} }, shift @{ $bucket2data{$bucket} };
		
			# delete old one
			delete $bucket2count{$bucket};
			delete $bucket2data{$bucket};
			$single_legacy_count++;
			$new_buckets{$newbucket} += 1;
		}
		else {
			# otherwise we leave it as such and hope for the best????
			$singleton_count++;
		}
	}
	
	printf " > consolidated %d legacy buckets into %d buckets\n", $single_legacy_count,
		scalar(keys %new_buckets);
	printf " > %d remaining buckets with single projects\n", $singleton_count;
}

sub print_output {
	# Open output filehandle
	my $outfh = IO::File->new($out_file, 'w') or
		die " Cannot open output fiile '$out_file' $OS_ERROR";

	# print the header
	$outfh->printf("%s\n", join( "\t", qw( SBGID Bucket Prefix OrigGroup OrigName ) ) );
	
	# print the data
	my $count = 0;
	foreach my $bucket ( sort {$a cmp $b} keys %bucket2data ) {
		foreach my $data ( sort { $a->[0] cmp $b->[0] } @{ $bucket2data{$bucket} } ) {
			if ( defined $data->[3] ) {
				# we have a GNomEx entry
				$outfh->printf("%s\n", join( "\t",
					$data->[1],
					$bucket,
					$data->[0],
					$data->[3]->group,
					$data->[3]->name
				) );
			}
			else {
				# no GNomEx entry
				$outfh->printf("%s\n", join( "\t",
					$data->[1],
					$bucket,
					$data->[0],
					q(),
					q()
				) );
				
			}
			$count++;
		}
	}
	$outfh->close;
	printf " > Wrote %d projects for %d buckets to %s\n", $count,
		scalar( keys %bucket2data ), $out_file;
}


