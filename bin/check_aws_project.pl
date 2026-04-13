#!/usr/bin/env perl

use warnings;
use strict;
use English qw(-no_match_vars);
use Getopt::Long;
use IO::File;
use Config::Tiny;
use Net::Amazon::S3::Client;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use RepoCatalog;

our $VERSION = 1.0;

my $doc = <<END;

A script to check whether a project's URI is still valid.

The metadata in the Catalog may become out-of-date or inaccurate over
time, and this script will double check that those projects that have
an AWS URI is still valid.

Legacy projects in particular may be inaccurate due to initial import
to SBG, then migration to AWS, and labs that have left or never signed
up with an account.

Provide a project list of identifiers to check, otherwise it will
go through every project in the Catalog.

Version: $VERSION

Usage:

  put_aws_project_readme.pl -c Catalog.db  12345R ...

Options:
    -c --cat <path>         Path to metadata catalog database. Required.
    --list <file>           List of project IDs
    --update                Update the Catalog metadata
    --cred <path>           Path to AWS credentials file. 
                               Default is ~/.aws/credentials. 
    -h --help               Show this help

END

# global variables
my $cat_file;
my @project_ids;
my $list_file;
my $update;
my $aws_cred_file = sprintf "%s/.aws/credentials", $ENV{HOME};
my $help;

# get options
if (@ARGV) {
	GetOptions(
		'c|catalog=s'           => \$cat_file,
		'list=s'                => \$list_file,
		'update!'               => \$update,
		'cred=s'                => \$aws_cred_file,
		'h|help!'               => \$help,
	) or die " bad options! Please check\n $doc\n";
}
else {
	print $doc;
	exit 0;
}

# Start
check_options();

# Initialize objects
my $Catalog = RepoCatalog->new($cat_file)
	or die "Cannot initialize Catalog file!\n";
my $Credentials = Config::Tiny->read($aws_cred_file) or
	die "Cannot load credentials file '$aws_cred_file'! $OS_ERROR";

# get project IDs, default everything!
unless (@project_ids) {
	@project_ids = $Catalog->list_all;
}

# check projects
foreach my $id (@project_ids) {
	check_project($id);
}



sub check_options {
	if ($help) {
		print $doc;
		exit 0;
	}
	# project IDs specified on the command line
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
	unless ( $aws_cred_file and -e $aws_cred_file ) {
		print " No AWS credential file provided! See help\n";
		exit 1;
	}
	unless ($cat_file) {
		print " ERROR: No catalog file provided! See help\n";
		exit 1;
	}
}

sub check_project {
	my $id = shift;
	my $Entry = $Catalog->entry($id);
	my $status;
	my $access_id;
	my $secret;
	if ( $Entry->external eq 'Y') {
		$status = 'external';
	}
	elsif ( not $Entry->core_lab ) {
		$status = 'no account';
	}
	elsif ( not $Entry->upload_datestamp ) {
		$status = 'not uploaded';
	}
	elsif ( not $Entry->bucket ) {
		$status = 'no bucket/prefix set';
	}
	else {
		# get credentials
		my $profile   = $Entry->profile;
		if ($profile) {
			$access_id = $Credentials->{$profile}{'aws_access_key_id'} || q();
			$secret    = $Credentials->{$profile}{'aws_secret_access_key'} || q();
		}
		unless ($access_id and $secret) {
			$status = 'no credentials';
		}
	}
	unless ($status) {
		my $aws = Net::Amazon::S3::Client->new(
			aws_access_key_id     => $access_id,
			aws_secret_access_key => $secret,
			# retry                 => 0,
		);

		# check bucket
		my $bucket_name = $Entry->bucket;
		my $bucket;
		my @buckets = $aws->buckets;
		foreach my $b (@buckets) {
			if ($b->name eq $bucket_name) {
				$bucket = $b;
				last;
			}
		}
		if ($bucket) {
			my $key    = sprintf "%s/%s_MANIFEST.csv", $Entry->prefix, $id;
			my $object = $bucket->object(
				key => $key
			);
			my $e;
			eval {$e = $object->exists};
			if ($e) {
				$status = 'ok';
			}
			else {
				# may be an old style file
				$key    = sprintf "%s/%s_MANIFEST.txt", $Entry->prefix, $id;
				$object = $bucket->object(
					key => $key
				);
				eval {$e = $object->exists};
				if ($e) {
					$status = 'ok';
				}
				else {
					$status = 'manifest not found!';
				}
			}
		}
		else {
			$status = 'bucket not found!'
		}
	}
	local $OUTPUT_AUTOFLUSH = 1; # piping hot output
	printf "%s\t%s\n", $id, $status;
}



 



