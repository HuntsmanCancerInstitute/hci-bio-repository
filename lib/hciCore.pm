package hciCore;


use strict;
use English qw(-no_match_vars);
use Carp;
use IO::File;

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw( generate_bucket generate_prefix cleanup );

our $VERSION = 6.0;
my  $net_loaded = 0;

sub generate_bucket {
	my $Entry = shift;
	my $first = $Entry->lab_first;
	my $last  = $Entry->lab_last;
	my $group = $Entry->group || q();

	# process common group names
	if ( $group =~ /^ (?: experiment s? | project s? ) \s for \s ([\w\-]+) \s ([\w\-\s]+)$/xi ) {
		$group = sprintf "%s-%s", $1, $2;
	}
	elsif ( $group =~ /^Request s? \s submitted \s by \s ([\w\-]+) \s ([\w\-\s]+)$/xi ) {
		$group = sprintf "%s-%s", $1, $2;
	}
	elsif ( $group =~ /^submitted \s for \s ([\w\-]+) \s ([\w\-\s]+)$/xi ) {
		$group = sprintf "%s-%s", $1, $2;
	}
	unless ($group) {
		$group = sprintf "%s-%s", $Entry->user_first, $Entry->user_last;
	}

	# cleanup the group identifier
	$group = cleanup($group);
	# additional cleaning, no underscores allowed
	# periods can be used except with transfer acceleration, so skip? plus look funny?
	$group =~ s/[_\.]/-/g;

	# generate bucket
	my $bucket = sprintf "cb-%s%s-%s", lc substr($first,0,1), lc substr($last,0,11), 
		lc $group;
	return $Entry->bucket($bucket);
}

sub generate_prefix {
	my $Entry = shift;
	my $id    = $Entry->id;
	my $name  = $Entry->name;

	# clean up the names of all weird characters
	if ($name) {
		$name = cleanup($name);
	}

	# compose prefix, we should always have both
	my $prefix;
	if ($name) {
		$prefix = sprintf "%s--%s", $id, $name;
	}
	else {
		$prefix = $id;
	}
	return $Entry->prefix($prefix);
}

sub cleanup {
	my $name = shift;
	unless ($name) {
		carp "no name provided to cleanup!";
		return;
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
		if ( $i > 19 and $i <= 31 ) {
			$name = substr $name, 0, $i;
		}
		else {
			# no word after 30 characters? try a dash delimiter
			$i = index $name, q(-), 20;
			if ( $i > 19 and $i <= 31 ) {
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


1;

__END__

=head1 NAME 

hciCore - Functions for working with HCI CORE AWS accounts

=head1 DESCRIPTION

These are exported subroutines for generating bucket and prefix names for uploading
to CORE AWS accounts. 

These are slightly modified versions of the subroutines from the 
C<generate_bulk_project_export_prefixes.pl> script.

No attempt is made to verify if the bucket or prefix exists.

=head1 FUNCTIONS

=over 4

=item generate_bucket

Pass a RepoEntry object from RepoCatalog. Generates a bucket name based on
the Principal Investigator name and  based on the format C<cb-flast-group>,
where "flast" is the first initial of the first name and the last name of
the Principal Investigator, and "group" is the the project's group folder
name in GNomEx. The RepoEntry bucket is automatically updated.

=item generate_prefix

Pass a RepoEntry object from RepoCatalog. Generates a prefix based on the Project's
ID and Name with the format C<ID--Name>.

=item cleanup

A simple function to remove and/or substitute non-permitted characters in a string, 
particularly for buckets and prefixes. The length is limited to no more than 30 
characters, breaking on word boundaries after 20 characters.
The RepoEntry prefix is automatically updated.

=back


=head1 AUTHOR

 Timothy J. Parnell, PhD
 Cancer Bioinformatics Shared Resource
 Huntsman Cancer Institute
 University of Utah
 Salt Lake City, UT, 84112

This package is free software; you can redistribute it and/or modify
it under the terms of the Artistic License 2.0.  




