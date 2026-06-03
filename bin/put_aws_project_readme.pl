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
use RepoProject;
use Gnomex;

our $VERSION = 1.1;

my $doc = <<END;

A script to place a readme file in the root of each uploaded GNomEx project
uploaded to an AWS account.

This is a small text file with basic information gleaned from GNomEx
database for each project, with basic information about the MANIFEST and
ARCHIVE files. It includes a URL back to the original GNomEx project.

For Request projects, it will also include at the end a tab-delimited
list of the sample identifiers and basic information.

Version: $VERSION

Usage:

  put_aws_project_readme.pl -c Catalog.db  12345R ...

Options:
    -c --cat <path>         Path to metadata catalog database. Required.
    -p --project <text>     Project identifier. Required. Repeat as necessary.
                              or simply append to the end of the command.
                              Multiple projects may be specified.
    --list <file>           List of project IDs
    --mock                  Do not upload, but leave the readme file in the
                              current directory.
    --cred <path>           Path to AWS credentials file. 
                               Default is ~/.aws/credentials. 
    -h --help               Show this help

END

# global variables
my $cat_file;
my @project_ids;
my $list_file;
my $mock;
my $aws_cred_file = sprintf "%s/.aws/credentials", $ENV{HOME};
my $help;
my @months = qw(Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec);

# get options
if (@ARGV) {
	GetOptions(
		'c|catalog=s'           => \$cat_file,
		'p|project=s'           => \@project_ids,
		'list=s'                => \$list_file,
		'mock!'                 => \$mock,
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
my $GNomEx = Gnomex->new()
	or die "Cannot initialize Gnomex object!\n";
my $Credentials = Config::Tiny->read($aws_cred_file) or
	die "Cannot load credentials file '$aws_cred_file'! $OS_ERROR";

# work through list
my $count = 0;
foreach my $id (@project_ids) {
	$count += post_project_readme($id);
}
if ($count > 1) {
	printf "\n Put %d readme files\n", $count;
}
exit 0;


######## Subroutines

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
	unless (@project_ids) {
		print " No Project IDs provided! See help\n";
		exit 1;
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

sub post_project_readme {
	my $id = shift;

	# get project objects
	my $Entry = $Catalog->entry($id);
	unless ($Entry) {
		printf " ! Project %s is not in the catalog file! skipping\n", $id;
		return 0;
	}
	my $Project = RepoProject->new($Entry->path);
	unless ($Project) {
		printf " ! unable to initiate Project object for %s path %s! skipping\n",
			$id, $Entry->path;
		return 0;
	}
	
	# check that project has been uploaded
	unless ( $Entry->core_lab ) {
		printf " ! Project %s does not have a CORE AWS account! skipping\n", $id;
		return 0;
	}
	unless ( $Entry->upload_datestamp ) {
		printf " ! Project %s has not been uploaded yet! skipping\n", $id;
		return 0;
	}
	
	# generate the readme text
	my $text;
	if ( $Entry->is_request ) {
		$text = generate_request_text($Entry, $Project);
	}
	else {
		$text = generate_analysis_text($Entry, $Project);
	}
	
	# write this as a temporary file
	my $file = sprintf "%s_README.txt", $id;
	my $fh = IO::File->new($file, '>')
		or die "Cannot write to file $file! $OS_ERROR";
	$fh->print($text);
	$fh->close;
	
	# if this was a mock attempt then we are done
	if ($mock) {
		printf " > wrote file %s\n", $file;
		return 1;
	}

	# get credentials
	my $profile   = $Entry->profile;
	my $access_id = $Credentials->{$profile}{'aws_access_key_id'} || q();
	my $secret    = $Credentials->{$profile}{'aws_secret_access_key'} || q();
	unless ($access_id and $secret) {
		print " ! No profile credentials for '%s' for %s! skipping\n", $profile, $id;
		return 0;
	}
	
	# initialize connection
	my $aws = Net::Amazon::S3::Client->new(
		aws_access_key_id     => $access_id,
		aws_secret_access_key => $secret,
		retry                 => 1,
	);
	unless ($aws) {
		print " ! Cannot connect to AWS account\n";
		return 0;
	}
	my $bucket = $aws->bucket( name => $Entry->bucket );
	my $key    = sprintf "%s/%s_README.txt", $Entry->prefix, $id;
	my $object = $bucket->object(
		key => $key,
		content_type => 'text/plain'
	);
	$object->put_filename($file);
	
	# no return value is provided from put so collect the metadata as alternative
	# and compare the sizes
	my $size = ( stat $file )[7];
	my $meta = $object->head;
	if ( $meta->{ContentLength} == $size ) {
		printf " > Successfully put file %s to %s/%s\n", $file, $Entry->bucket, $key;
		unlink $file;
		return 1;
	}
	else {
		printf " ! Issue with putting file %s: size mismatch: file %d, object %d\n",
			$file, $size, $meta->{ContentLength} || 0;
		printf "   Keeping temporary file %s\n", $file;
		return 0;
	}
}

sub generate_request_text {
	my ($Entry, $Project) = @_;
	
	# generate creation date string
	my ($y, $m, $d) = split /\-/, $Entry->date;
	my $date = sprintf "%s %d, %d", $months[ $m - 1 ], $d, $y;
	
	# Generate header text with basic information.
	my $text = <<~DOC;
	Project %s "%s"
	
	This Request was submitted to GNomex on %s by %s %s
	in the lab of %s %s. The original project folder was "%s".
	The original Project can be found on GNomEx at
	
	%s
	
	An inventory of the files from the original Project is found in the
	manifest file "%s".
	
	Large files by default may be archived in Deep Glacier and will need to
	be temporarily restored before downloading or accessing.
	DOC
	my $final = sprintf $text, $Entry->id, $Entry->name, $date, $Entry->user_first,
		$Entry->user_last, $Entry->lab_first, $Entry->lab_last, $Entry->group,
		$Entry->project_gnomex_url, $Project->manifest_file;
	
	# add archive stuff as necessary
	if ( $Entry->autoanal_folder and $Entry->autoanal_folder =~ /AutoAnalysis/
		and $Entry->autoanal_up_datestamp
	) {
		
		$text = <<~DOC;
		
		This project has included analysis files in the folder "%s". 
		Small files may be compressed into the Zip archive file "%s".
		A list of these files are in the file "%s".
		Unzipping this file in place will reconstitute the project.
		DOC
		$final .= sprintf( $text, $Entry->autoanal_folder, $Project->zip_file,
			$Project->ziplist_file );
	}
	
	# collect samples from the database
	# the first line returned are the column headers
	my $samples = $GNomEx->fetch_request_samples( $Entry->id );
	return unless $samples;
	$final .= <<~DOC;
	
	A list of the original samples is provided below as a tab-delimited list.
	
	DOC
	foreach my $line ( @{$samples} ) {
		$final .= sprintf "%s\n", join( "\t", map { $_ || q() } @{ $line } );
	}
	return $final;
}

sub generate_analysis_text {
	my ($Entry, $Project) = @_;
	
	# generate creation date string
	my ($y, $m, $d) = split /\-/, $Entry->date;
	my $date = sprintf "%s %d, %d", $months[ $m - 1 ], $d, $y;
	
	# Generate header text with basic information.
	my $text = <<~DOC;
	Project %s "%s"
	
	This Analysis was generated in GNomex on %s and owned by %s %s
	in the lab of %s %s. The original project folder was "%s".
	The original Project can be found on GNomEx at
	
	%s
	DOC
	my $final = sprintf $text, $Entry->id, $Entry->name, $date, $Entry->user_first,
		$Entry->user_last, $Entry->lab_first, $Entry->lab_last, $Entry->group,
		$Entry->project_gnomex_url;
	
	# add information about the linked Request project
	# this information is buried in the GNomEx database but not easy to retrieve
	# most people put the Request ID in the Analysis name, so use that if available
	if ( $Entry->name =~ /(\d+R)/ ) {
		my $Req = $Catalog->entry($1);
		if ($Req) {
			$text = <<~DOC;
			
			The linked Experiment Request Project is %s "%s".
			DOC
			$final .= sprintf $text, $Req->id, $Req->name;
			if ( $Req->upload_datestamp ) {
				$text = <<~DOC;
				It can found at %s/%s or by following this link:
				
				%s
				DOC
				$final .= sprintf $text, $Req->bucket, $Req->prefix, $Req->project_core_url;
			}
			else {
				$text = <<~DOC;
				This Project was not uploaded. The original GNomEx project can be found at:
				
				%s
				DOC
				$final .= sprintf $text, $Req->project_gnomex_url;
			}
		}
	}
	$text = <<~DOC;
	
	An inventory of the files from the original Project is found in the
	manifest file "%s".
	
	Large files by default may be archived in Deep Glacier and will need
	to be temporarily restored before downloading.
	
	Small files may be compressed into the Zip Archive file "%s".
	A list of these files are in the file "%s".
	Unzipping this file in place will reconstitute the project.
	
	If you are unable to find a file, try searching for it in either the
	MANIFEST or ARCHIVE_LIST files.
	
	The indicated genome build is %s "%s".
	
	DOC
	$final .= sprintf $text, $Project->manifest_file, $Project->zip_file,
		$Project->ziplist_file, $Entry->organism, $Entry->genome;
	
	return $final;
}



