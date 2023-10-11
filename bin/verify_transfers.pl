#!/usr/bin/env perl

# Timothy J. Parnell, PhD
# Huntsman Cancer Institute
# University of Utah
# Salt Lake City, UT 84112
#
# This package is free software; you can redistribute it and/or modify
# it under the terms of the Artistic License 2.0.
#

use warnings;
use strict;
use English qw(-no_match_vars);
use Getopt::Long qw(:config no_ignore_case);
use IO::File;
use Net::SB;
use Net::Amazon::S3::Client;


our $VERSION = 0.1;

my $doc = <<END;

A script to verify all files have been exported from a SBG project to a an
AWS bucket. It will recursively list all files from a source SBG project
and compare file names and sizes with those listed in a corresponding AWS
bucket. Files with mismatched file sizes or missing in either source or
target are reported.

Multiple sources and targets may be provided; repeat source and target 
options as necessary.

Usage

 verify_transfer.pl -d big-shot -p cb_bshot -s 12345r -t my-bucket/12345R--exp1

Options

-s --source   <text>   The Seven Bridges source project, folders allowed
-t --target   <text>   The AWS target bucket, folders allowed
-d --division <text>   The Seven Bridges division, required
-p --profile  <text>   The AWS profile, required
--sbcred      <file>   The Seven Bridges credential file
	                      default ~/.sevenbridges/credentials
--awscred     <file>   The AWS credential file
                          default ~/.aws/credentials
-h --help              Show this help

END



# Command line options
my @sources;
my @targets;
my $division;
my $profile;
my $sbg_cred_file = sprintf "%s/.sevenbridges/credentials", $ENV{HOME};
my $aws_cred_file = sprintf "%s/.aws/credentials", $ENV{HOME};
my $help;

if (@ARGV) {
	GetOptions(
		's|source=s'        => \@sources,
		't|target=s'        => \@targets,
		'd|division=s'      => \$division,
		'p|profile=s'       => \$profile,
		'sbcred=s'          => \$sbg_cred_file,
		'awscred=s'         => \$aws_cred_file,
		'h|help!'           => \$help,
	) or die " bad options! Please check\n $doc\n";
}
else {
	print $doc;
	exit 0;
}


# Check Options
if ($help) {
	print $doc;
	exit 0;
}
unless ($division) {
	print " Seven Bridges division name is required!\n";
	exit 1;
}
unless ($profile) {
	print " AWS IAM connection profile is required!\n";
	exit 1;
}
unless (@sources and @targets) {
	print " Both sources and targets must be specified!\n";
	exit 1;
}
unless (scalar @sources == scalar @targets) {
	print " Must provide equal numbers of sources and targets!\n";
	exit 1;
}

# Open AWS Client
my $Client = open_aws_client();


# Open SBG Division
my $Sb = Net::SB->new(
	div        => $division,
	cred       => $sbg_cred_file,
) or die " unable to initialize SB division object!\n";




# Iterate through projects
while (@sources) {
	my $source = shift @sources;
	my $target = shift @targets;
	compare($source, $target);
}
print "\n Completed\n";



sub open_aws_client {

	# First get AWS credentials
	my $infh = IO::File->new($aws_cred_file) or
		die "unable to open file '$aws_cred_file'! $OS_ERROR\n";
	my %options;
	my $line = $infh->getline;
	while ($line) {
		chomp $line;
		if ($line eq "[$profile]") {
			# found the profile header
			undef $line;
			while (my $line2 = $infh->getline) {
				if (substr($line2, 0, 1) eq '[') {
					$line = $line2;
					last;
				}
				elsif ($line2 =~ /^ (\w+) \s? = \s? (\S+) $/x ) {
					$options{$1} = $2;
				}		
			}
			$line ||= $infh->getline || undef;
		}
		else {
			$line = $infh->getline || undef;
		}
	}
	$infh->close;
	unless (
		exists $options{'aws_access_key_id'}
		and exists $options{'aws_secret_access_key'}
	) {
		printf " No AWS access keys found for profile %s in %s!\n", $profile,
			$aws_cred_file;
		exit 1;
	}
	
	# Open client
	my $aws = Net::Amazon::S3::Client->new(
		aws_access_key_id     => $options{'aws_access_key_id'},
		aws_secret_access_key => $options{'aws_secret_access_key'},
		retry                 => 1,
	) or die "unable to open AWS S3 Client!";
	return $aws;
}

sub compare {
	my ($source, $target) = @_;
	print "\n > Checking $source with $target\n";
	
	# Open AWS bucket
	my ($bucket_name, $prefix);
	if ($target =~ m|/|) {
		($bucket_name, $prefix) = split m|/|, $target, 2;
		$prefix .= '/';
	}
	else {
		$bucket_name = $target;
	}
	my $bucket = $Client->bucket( name => $bucket_name );
	unless ($bucket) {
		printf "  ! Failed to open bucket %s\n", $bucket_name;
		return;
	}

	# Open SB project
	my ($project_name, $folder);
	if ($source =~ m|/|) {
		($project_name, $folder) = split m|/|, $source, 2;
	}
	else {
		$project_name = $source;
	}
	my $SBProject = $Sb->get_project($project_name);
	unless ($SBProject) {
		printf "  ! Failed to open SB Project %s\n", $project_name;
		return;
	}

	# bucket object list
	my $stream;
	if ($prefix) {
		$stream = $bucket->list( { prefix => $prefix } );
	}
	else {
		$stream = $bucket->list();
	}
	unless ($stream) {
		print "  ! Failed to open AWS bucket stream\n";
		return;
	}
	my %aws;
	until ( $stream->is_done ) {
		foreach my $object ($stream->items) {
			my $k = $object->key;
			next if $k =~ m|/$|;  # skip folder
			if ($prefix) {
				$k =~ s/^ $prefix //x;
			}
			my $v = $object->size;
			$aws{$k} = $v;
		}
	}
	unless (%aws) {
		print "  ! Failed to get AWS bucket object listing\n";
		return;
	}
	
	# SB project list
	my $sb_list;
	if ($folder) {
		my $f = $SBProject->get_file_by_name($folder);
		if ($f) {
			$sb_list = $f->recursive_list;
		}
		else {
			printf "  ! Unable to find SB folder %s\n", $folder;
			return;
		}
	}
	else {
		$sb_list = $SBProject->recursive_list;
	}
	unless ( @{ $sb_list } ) {
		print "  ! Failed to get SB project recursive listing\n";
		return;
	}
	unless ( $Sb->bulk_get_file_details($sb_list) ) {
		print "  ! Failed to collect bulk details on SB file objects\n";
		return;
	}

	# compare
	my $sb_count  = scalar @{ $sb_list };
	my $aws_count = scalar keys %aws;
	my $match     = 0;
	my @incomplete;
	my @missing;
	foreach my $f ( @{ $sb_list } ) {
		next if $f->type eq 'folder';
		my $name = $f->pathname;
		if ($folder) {
			$name =~ s/^ $folder \/ //x;
		}
		if ( exists $aws{$name} ) {
			my $size = $f->size;
			if ( $size == $aws{$name} ) {
				$match++;
			}
			else {
				push @incomplete, [ $name, $size, $aws{$name} ];
			}
			delete $aws{$name};
		}
		else {
			push @missing, $name;
		}
	}
	my @extra;
	if (%aws) {
		@extra = sort {$a cmp $b} keys %aws;
	}
	
	# print results
	printf "    %d files matched\n", $match;
	if (@incomplete) {
		printf "  ! %d files have mismatched sizes\n", scalar @incomplete;
	}
	if (@missing) {
		printf "  ! %d files missing in bucket\n", scalar @missing;
	}
	if (@extra) {
		printf "  ! %d extra files in bucket\n", scalar @extra;
	}
	if (@incomplete) {
		foreach my $f ( @incomplete ) {
			printf "    incomplete: %s  %d => %d\n", @{$f};
		}
	}
	if (@missing) {
		foreach my $f ( @missing ) {
			printf "    missing: %s\n", $f;
		}
	}
	if (@extra) {
		foreach my $f ( @extra ) {
			printf "    extra: %s\n", $f;
		}
	}
}



