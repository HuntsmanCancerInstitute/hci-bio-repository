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

our $VERSION = 1.2;

my $doc = <<END;

A script to check whether a project's URI is still valid.

The metadata in the Catalog may become out-of-date or inaccurate over
time, and this script will double check that those projects that have
an AWS URI is still valid.

Legacy projects in particular may be inaccurate due to initial import
to SBG, then migration to AWS, and labs that have left or never signed
up with an account.

It will fix problems if indicated, including uploading a missing manifest
file and posting a missing readme file. For projects with no corresponding
bucket found or empty prefixes, it will clear the metadata from the
Catalog (caution!).

A status is printed for every project, and notifications for every
missing file uploaded and/or metadata update to the catalog.

Provide a project list of identifiers to check, otherwise it will
go through every project in the Catalog.

Version: $VERSION

Usage:

  check_aws_project.pl -c Catalog.db  12345R ...

Options:
    -c --cat <path>         Path to metadata catalog database. Required.
    --list <file>           File with list of project IDs

Fixes to problems:
    --update                Update Catalog to remove missing bucket/prefix
    --manifest              Upload a missing manifest file
    --readme                Post a missing readme file

Other:
    --cred <path>           Path to AWS credentials file. 
                               Default is ~/.aws/credentials. 
    -h --help               Show this help

END

# global variables
my $cat_file;
my @project_ids;
my $list_file;
my $update = 0;
my $post_manifest = 0;
my $post_readme = 0;
my $aws_cred_file = sprintf "%s/.aws/credentials", $ENV{HOME};
my $help;

# get options
if (@ARGV) {
	GetOptions(
		'c|catalog=s'           => \$cat_file,
		'list=s'                => \$list_file,
		'update!'               => \$update,
		'manifest!'             => \$post_manifest,
		'readme!'               => \$post_readme,
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
	my $bucket;

	# check metadata first
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

	# check remote if still good
	unless ($status) {
		my $aws = Net::Amazon::S3::Client->new(
			aws_access_key_id     => $access_id,
			aws_secret_access_key => $secret,
			# retry                 => 0,
		);

		# check bucket
		my $bucket_name = $Entry->bucket;
		my @buckets = $aws->buckets;
		foreach my $b (@buckets) {
			if ($b->name eq $bucket_name) {
				$bucket = $b;
				last;
			}
		}
		if ($bucket) {

			# check for manifest
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
					$status = 'manifest not found';
				}
			}
			
			# check for readme
			$key    = sprintf "%s/%s_README.txt", $Entry->prefix, $id;
			$object = $bucket->object(
				key => $key
			);
			undef $e;
			eval {$e = $object->exists};
			if ($e) {
				if ( $status ne 'ok' ) {
					$status .= '; readme found';
				}
				# otherwise it's ok
			}
			else {
				if ( $status eq 'ok' ) {
					$status = 'readme not found';
				}
				else {
					$status .= '; readme not found';
				}
			}
		}
		else {
			$status = 'bucket not found';
		}
	}
	local $OUTPUT_AUTOFLUSH = 1; # piping hot output
	printf "%s\t%s\n", $id, $status;
	
	# update
	if ( $update and $status eq 'bucket not found' ) {
		my $text = sprintf
			" ! Removed bucket/prefix metadata '%s/%s' from Catalog for %s\n",
			$Entry->bucket, $Entry->prefix, $id;
		$Entry->bucket( q() );
		$Entry->prefix( q() );
		print $text;
	}
	elsif ( $status =~ /not found/ and ( $post_manifest or $post_readme ) ) {
		# first check that there are contents in the bucket
		my $prefix = $Entry->prefix;
		my $stream = $bucket->list( { prefix => $prefix } );
		my $count = 0;
		foreach my $object ( $stream->items ) {
			next if $object->key =~ m|/$|; # skip folders
			$count++;
			last if $count > 2; # good enough
		}
		if ($count >= 2) {
			# folder is not empty
			
			# post a missing manifest
			if ( $post_manifest and $status =~ /manifest \s not \s found/x ) {
				my $destination;
				my $source = sprintf "%s/%s_MANIFEST.csv", $Entry->path, $id;
				if ( -e $source ) {
					$destination = sprintf "s3://%s/%s/%s_MANIFEST.csv", $Entry->bucket,
						$Entry->prefix, $id;
				}
				else {
					$source = sprintf "%s/%s_MANIFEST.txt", $Entry->path, $id;
					if ( -e $source ) {
						$destination = sprintf "s3://%s/%s/%s_MANIFEST.txt",
							$Entry->bucket, $Entry->prefix, $id;
					}
					else {
						printf " ! No source manifest file available in %s, not uploading\n",
							$Entry->path; 
					}
				}
	
				# we have a source and destination
				if ($destination) {
					my $cmd = sprintf "aws s3 cp %s %s --profile %s --no-progress", $source,
						$destination, $Entry->profile;
					# printf " > Executing %s\n", $cmd;
					my $result = qx($cmd);
					chomp $result;
					if ($result =~ m|\A upload: \s (?:[\.\/\w]+)? $source \s to \s $destination|x) {
						printf " > Successfully uploaded %s\n", $destination;
					}
					else {
						printf " ! Error: %s\n", $result;
					}
				}
			}
			
			# post a missing readme
			if ( $post_readme and $status =~ /readme \s not \s found/x ) {
				# for this we call another program
				my $cmd = sprintf "%s/put_aws_project_readme.pl -c %s -p %s", $Bin,
					$cat_file, $id;
				my $result = qx($cmd);
				print $result;
			}
		}
		else {
			printf " ! Prefix %s/%s appears empty, not uploading manifest or readme\n",
				$Entry->bucket, $Entry->prefix;
			if ($update) {
				my $text = sprintf
					" ! Removed bucket/prefix metadata '%s/%s' from Catalog for %s\n",
					$Entry->bucket, $Entry->prefix, $id;
				$Entry->bucket( q() );
				$Entry->prefix( q() );
				print $text;
			}
		}
	}
}



 



