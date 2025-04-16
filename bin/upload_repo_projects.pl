#!/usr/bin/env perl

use warnings;
use strict;
use English qw(-no_match_vars);
use Getopt::Long;
use File::Spec;
use IO::File;
use IO::Handle;
use Text::CSV;
use List::Util qw(mesh);
use Config::Tiny;
use Date::Parse;
use Parallel::ForkManager;
use Net::Amazon::S3::Client;
use Text::Levenshtein::Flexible;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use RepoProject;
use RepoCatalog;


our $VERSION = 1.0;

my $doc = <<END;

A script to prepare and upload a GNomEx Repository Project to its
corresponding CORE Lab account AWS bucket.

It will identify candidate files to upload from a given GNomEx
Repository project, either Request and Analysis. The Project Manifest
file is used to generate the list of candidate files, so the Project
must already be scanned; see corresponding process_project.pl
application. If the project was previously uploaded and the
bucket/prefix exists, then it will be recursively listed and compared
with the manifest file to determine which file(s) need to be uploaded.


The target AWS bucket and prefix are determined from a Project catalog
database. See the manage_repository.pl application. It is possible to
upload a standalone directory if given a manifest file, bucket URI,
prefix, and AWS profile; see usage below.

If the AWS bucket does not exist, then a warning will be issued and the 
program will exit; it will not create a bucket for you. Existing buckets
with similar names will be printed as possibilities, in which case the 
target bucket will need to be adjusted in the catalog database.

This uses the AWS command line utility to perform the actual uploads,
running them in parallel jobs for each file individually. This tool 
must be in your environment PATH. It uses the default AWS credential
files.

VERSION: $VERSION

USAGE:

  upload_repo_projects.pl -c <catalog> -p <project> --check
  
  upload_repo_projects.pl -c <catalog> -p <project>
  
  upload_repo_projects.pl --path <path> --bucket <name> --prefix <text> --profile <text>


OPTIONS:
  
  Project information from database:
    -c --cat <path>         Path to metadata catalog database
    -p --project <text>     Project identifier
  
  Or manually specify project information:
    --path <path>           Path of directory containing files and x_MANIFEST.csv
    --bucket <text>         Remote AWS bucket name
    --prefix <text>         Prefix for destination objects
    --profile <text>        AWS profile name in credentials file
  
  Options:
    --check                 Check and report upload counts only
    --aa                    Include the Request AutoAnalysis folder (default skip)
    -f --forks <int>        Number of parallel aws upload jobs, default 2
    --class <text>          Specify storage class (default STANDARD)
                              Examples include INTELLIGENT_TIERING, 
                              GLACIER, DEEP_ARCHIVE
    --dryrun                Do not upload
    --cred <path>           Path to AWS credentials file. 
                               Default is ~/.aws/credentials. 
    -v --verbose            Print additional details
    -h --help               Show this help


END



# Command line options
my $cat_file;
my $project_id;
my $project_path;
my $bucket_name;
my $prefix;
my $profile;
my $check_only;
my $include_autoanal;
my $storage_class;
my $dryrun;
my $cpu = 2;
my $aws_cred_file = sprintf "%s/.aws/credentials", $ENV{HOME};
my $aws_cli;
my $verbose;
my $help;

if (@ARGV) {
	GetOptions(
		'c|catalog=s'           => \$cat_file,
		'p|project=s'           => \$project_id,
		'path=s'                => \$project_path,
		'bucket=s'              => \$bucket_name,
		'prefix=s'              => \$prefix,
		'profile=s'             => \$profile,
		'check!'                => \$check_only,
		'aa!'                   => \$include_autoanal,
		'class=s'               => \$storage_class,
		'dryrun!'               => \$dryrun,
		'f|forks=i'             => \$cpu,
		'cred=s'                => \$aws_cred_file,
		'aws=s'                 => \$aws_cli,
		'v|verbose!'            => \$verbose,
		'h|help!'               => \$help,
	) or die " bad options! Please check\n $doc\n";
}
else {
	print $doc;
	exit 0;
}


# global variables
my $Entry;
my $upload_date;
my $Project;
my $aws;
my $using_flat_req;
my @upload_list;
my $count = 0;
my $size  = 0;

# levenshtein distance weights, emphasize insertion/deletion rather than substitution
my $max_dist = 12;
my $cost_ins = 1;
my $cost_del = 1;
my $cost_sub = 2;


#### Main functions
check_options();
initialize();
prepare_list();
upload_files_parallel() unless $check_only;

# finished


#### Subroutines

sub check_options {
	if ($help) {
		print $doc;
		exit 0;
	}
	if ( $project_id or $cat_file ) {
		unless ($project_id) {
			print " ERROR: No project identifer provided! See help\n";
			exit 1;
		}
		unless ($cat_file) {
			print " ERROR: No catalog file provided! See help\n";
			exit 1;
		}
	}
	elsif ( $project_path and $bucket_name and $prefix and $profile ) {
		print " Using provided project path, bucket, prefix, and profile\n";

		# clean bucket
		$bucket_name =~ s|^s3://||;
		if ($bucket_name =~ m|/|) {
			print " ERROR: Bucket name should not include / characters! Use prefix\n";
			exit 1;
		}

		# generate required information
		my @bits = split m|/|, $project_path;
		$project_id = $bits[-1];
	}
	else {
		print
" ERROR: Not enough source or destination specifications provided! See help\n";
		exit 1;
	}
	unless ( $aws_cred_file and -e $aws_cred_file ) {
		print " No AWS credential file provided! See help\n";
		exit 1;
	}
	if ($storage_class) {
		unless ($storage_class =~ 
/^ (?: STANDARD | REDUCED_REDUNDANCY | STANDARD_IA | ONEZONE_IA | INTELLIGENT_TIERING | GLACIER | DEEP_ARCHIVE | GLACIER_IR) $/x
		) {
			print " ERROR: Unrecognized storage class – see `aws s3 cp help`\n";
			exit 1;
		}
	}
	# check external aws command
	if ($aws_cli) {
		unless ( -e $aws_cli and -x _ ) {
			printf " ERROR: Provided AWS CLI program '%s' not available!\n", $aws_cli;
			exit 1;
		}
	}
	else {
		$aws_cli = qx(which aws);
		chomp $aws_cli;
		if ($aws_cli) {
			printf " Using %s to upload\n", $aws_cli;
		}
		else {
			print " ERROR: No AWS CLI program available in PATH!\n";
			exit;
		}
	}
}

sub initialize {

	printf " > Preparing Project %s for upload\n", $project_id;
	
	# Catalog entry
	if ( $cat_file and $project_id ) {
		my $Catalog = RepoCatalog->new($cat_file) or 
			die "Cannot open catalog file '$cat_file'!";
		$Entry = $Catalog->entry($project_id);
		unless ($Entry) {
			print " ! Identifier $project_id not in Catalog! failing\n";
			exit 1;
		}
		unless ($Entry->scan_datestamp > 1) {
			print " ! Identifier $project_id has not been scanned\n";
			exit 1;
		}
		$bucket_name  = $Entry->bucket;
		$profile      = $Entry->profile;
		$prefix       = $Entry->prefix;
		$project_path = $Entry->path;
		$upload_date  = $Entry->upload_datestamp;
	}

	# check if project is eligible
	unless ($profile) {
		print " ! $project_id does not have an AWS profile\n";
		exit 1;
	}
	unless ($bucket_name) {
		print " ! $project_id is not assigned a destination bucket\n";
		exit 1;
	}
	unless ($prefix) {
		print " ! $project_id is not assigned a destination prefix\n";
		exit 1;
	}
	# the manifest file is checked later once we initialize the project folder object

	# Collect AWS credentials
	my $Credentials = Config::Tiny->read($aws_cred_file) or
		die "Cannot load credentials file '$aws_cred_file'! $OS_ERROR";
	my $access_id = $Credentials->{$profile}->{'aws_access_key_id'} || q();
	my $secret    = $Credentials->{$profile}->{'aws_secret_access_key'} || q();
	unless ( $access_id and $secret ) {
		printf " ! No access keys for profile '%s' in aws credentials file\n", $profile;
		exit 1;
	}
	
	# open AWS connection
	$aws = Net::Amazon::S3::Client->new(
		aws_access_key_id     => $access_id,
		aws_secret_access_key => $secret,
		retry                 => 1,
	) or die "ERROR: unable to open AWS S3 Client!";
	
}

sub prepare_list {

	# check bucket
	my $bucket;
	my @buckets = $aws->buckets;
	foreach my $b (@buckets) {
		if ($b->name eq $bucket_name) {
			$bucket = $b;
			last;
		}
	}
	if ( $bucket ) {
		if ($verbose) {
			printf "  > bucket '%s' exists\n", $bucket_name;
		}
	}
	else {
		if ($Entry) {
			printf " ! Bucket '%s' in '%s' for %s does not exist\n", $bucket_name,
				$Entry->core_lab, $project_id;
		}
		else {
			printf " ! Bucket '%s' under profile '%s' for %s does not exist\n",
				$bucket_name, $profile, $project_id;
		}
		my $TL = Text::Levenshtein::Flexible->new($max_dist, $cost_ins, $cost_del,
			$cost_sub);
		my @names = grep { !/logs$/ } map { $_->name } @buckets;
		if ($verbose) {
			printf "    > all buckets: %s\n", join( q(, ), @names );
		}
		my @top   = map { $_->[0] }
		            sort { $a->[1] <=> $b->[1] }
		            $TL->distance_lc_all( $bucket_name, @names );
		printf "    Possibilities include: %s\n", join( q(, ), @top );
		exit 1;
	}

	# get the repository folder project
	$Project = RepoProject->new($project_path);
	unless ($Project) { 
		printf  " ! unable to initiate Repository Project for path '%s'!\n", 
			$project_path;
		exit 1;
	}

	# change to the given directory
	chdir $project_path or
		die sprintf("cannot change to %s! $OS_ERROR\n", $project_path);

	# check manifest file
	my $manifest_file = $Project->manifest_file;
	unless ( -e $manifest_file ) {
		printf " ! No project manifest file '%s' generated\n", $manifest_file;
		exit 1;
	}
	
	# files in a zip archive would be in the manifest but excluded from disk
	# therefore are skipped in the upload list
	my $ziplist = $Project->ziplist_file;
	my %zipped;
	if ( -e $ziplist ) {
		my $fh = IO::File->new($ziplist) or 
			die " Cannot read zip list file '$ziplist'! $OS_ERROR";
		while ( my $line = $fh->getline ) {
			chomp $line;
			$zipped{$line} = 1;
		}
		$fh->close;
		if ($verbose) {
			printf "  > loaded %d files from zip list file '%s'\n", scalar(keys %zipped),
				$ziplist;
		}
	}
	else {
		undef $ziplist;
	}

	# check existing bucket prefix contents
	my %existing;
	if ( $bucket ) {
		# perform a recursive search, but this may be empty
		my $stream = $bucket->list( { prefix => $prefix } );
		until ( $stream->is_done ) {
			foreach my $object ($stream->items) {
				my $k = $object->key;
				next if $k =~ m|/$|;  # skip folder
				$k =~ s/^ $prefix \/ //x;
				$existing{$k} = $object->last_modified_raw; 
			}
		}
		if ($verbose) {
			printf "  > found %d objects in %s/%s\n", scalar keys %existing,
				$bucket_name, $prefix;
		}
		if (%existing and not $upload_date) {
			printf " ! Project %s has contents in %s/%s but no upload date\n",
				$project_id, $bucket_name, $prefix;
		}
	}
	

	# manifest file itself
	my @man_stat = stat $manifest_file;
	if ( exists $existing{$manifest_file} ) {
		my $local_time  = $man_stat[9];
		my $remote_time = str2time( $existing{$manifest_file} );
		if ( $local_time > $remote_time ) {
			push @upload_list, $manifest_file;
			$count++;
			$size += $man_stat[7];
			if ($verbose) {
				print "   > including file $manifest_file\n";
			}
		}
		else {
			if ($verbose) {
				print "   > skipping uploaded file $manifest_file\n";
			}
		}
	}
	else {
		push @upload_list, $manifest_file;
		$count++;
		$size += $man_stat[7];		
		if ($verbose) {
			print "   > including file $manifest_file\n";
		}
	}

	# parse manifest
	my $csv = Text::CSV->new();
	my $fh  = IO::File->new($manifest_file) or
		die " Cannot read manifest file '$manifest_file'! $OS_ERROR";
	my $header = $csv->getline($fh);
	my $skip   = 0;
	my $zipcnt = 0;
	my $upcnt  = 0;
	my $aacnt  = 0;
	while ( my $data = $csv->getline($fh) ) {
		my %file = mesh $header, $data;
		my $fname = $file{File};
		
		# generate alternate name, old Request projects uploaded to Seven Bridges did
		# not maintain directories, so skip the Fastq directory to maintain consistency
		my $altname;
		if ( $fname =~ m|^Fastq/| ) {
			$altname = $fname;
			$altname =~ s|^Fastq/||;
		}
		
		# check if we need to skip this file
		if (exists $zipped{$fname}) {
			if ($verbose) {
				print "   > skipping zipped file $fname\n";
			}
			$skip++;
			$zipcnt++;
			next;
		}
		if ( not $include_autoanal and $fname =~ /^ AutoAnalysis_ \w+\d{4} \/ /x ) {
			$skip++;
			$aacnt++;
			next;
		}
		
		if ( exists $existing{$fname} or ( $altname and exists $existing{$altname} ) ) {
			if ( $altname and exists $existing{$altname} and not $using_flat_req ) {
				$using_flat_req = 1;
				print "   ! old-style flat folder structure detected\n";
			}
			my $local_time  = str2time( $file{Date} );
			my $remote_time;
			if ( $using_flat_req and $altname ) {
				$remote_time = str2time( $existing{$altname} );
			}
			else {
				$remote_time = str2time( $existing{$fname} );
			}
			if ($remote_time > $local_time) {
				if ($verbose) {
					print "   > skipping uploaded file $fname\n";
				}
				$skip++;
				$upcnt++;
				next;
			}
		}
		
		# process file
		push @upload_list, $fname;
		if ($verbose) {
			print "   > including file $fname\n";
		}
		$count++;
		$size += $file{Size};
	}
	$fh->close;
	
	printf " > Collected %d files (%s) out of %d to upload to %s/%s\n",
		$count, format_human_size($size), $count + $skip, $bucket_name, $prefix;
	if ($zipcnt) {
		printf "   > %d files were zipped\n", $zipcnt;
	}
	if ($upcnt) {
		printf "   > %d files were already uploaded\n", $upcnt;
	}
	if ($aacnt) {
		printf "   > %d files were in AutoAnalysis folder and not included\n", $aacnt;
	}

}

sub upload_files_parallel {
	return unless (@upload_list);
	printf " > Uploading in %d forks\n", $cpu;
	my $pm = Parallel::ForkManager->new($cpu)
		or die "unable to initiate Parallel::ForkManager!";

	# collect errors
	my $success = 0;
	my $failure = 0;
	my $start   = time;
	$pm->run_on_finish(
		sub {
			my ( $pid, $exit_code, $ident, $exit_signal, $core_dump, $data ) = @_;
			if ( $exit_code == 0 ) {
				printf "  > Uploaded '%s' successfully\n", $upload_list[$ident];
				$success++;
			}
			else {
				printf "  ! Failed to upload '%s'\n%s\n", $upload_list[$ident], ${$data};
				$failure++;
			}
		}
	);
	
	# iterate through list
	foreach my $i ( 0 .. $#upload_list ) {
		my $file = $upload_list[$i];
		$pm->start($i) and next;

		### in child
		my $remote_path = sprintf "s3://%s/%s/%s", $bucket_name, $prefix, $file;
		if ($using_flat_req) {
			my $alt = $file;
			$alt =~ s/^Fastq\///;
			$remote_path = sprintf "s3://%s/%s/%s", $bucket_name, $prefix, $alt;
		}
		my $command = sprintf qq(%s s3 cp '%s' '%s' --profile %s --no-progress),
			$aws_cli, $file, $remote_path, $profile;
		if ($storage_class) {
			$command .= sprintf(" --storage-class %s", $storage_class);
		}
		if ($dryrun) {
			$command .= ' --dryrun';
		}
		if ($verbose) {
			printf " => child $i executing: %s\n", $command;
		}
		$command .= ' 2>&1'; 
		my $result = qx($command);
		chomp $result;
		# if there is no local path then aws will stick a ./ to the file path
		if ( $result =~ /\A upload: \s (?:\.\/)? $file \s to/x ) {
			$pm->finish(0);
		}
		elsif ( $dryrun and $result =~ /\A \( dryrun \) \s upload: \s (?:\.\/)? $file/x) {
			$pm->finish(0);
		}
		else {
			# some sort of error
			$pm->finish(1, \$result);
		}
	}

	# wait till everything is completed
	$pm->wait_all_children;
	my $elapsed = time - $start;

	# completed
	printf "\n > Completed %d uploads in %.1f minutes, %.2f MB/sec\n", $success,
		$elapsed / 60, ($size / 1048576 ) / ($elapsed );
	if ($failure) {
		printf " ! There were %d upload failures\n", $failure;
		exit 1;
	}
	elsif ($success) {
		# update the upload time stamps as appropriate
		if ($Entry) {
			if ( $include_autoanal and $Entry->is_request ) {
				$Entry->autoanal_up_datestamp(time);

				# also update standard upload date too if it's not set
				unless ( $Entry->upload_datestamp ) {
					$Entry->upload_datestamp(time);
				}
			}
			else {
				$Entry->upload_datestamp(time);
			}
		}
	}
}




sub format_human_size {
	my $value = shift;
	if ($value > 1073741824) {
		return sprintf "%.1f GiB", ($value / 1073741824);
	}
	elsif ($value > 1048576) {
		return sprintf "%.1f MiB", ($value / 1048576);
	}
	elsif ($value > 1024) {
		return sprintf "%.1f KiB", ($value / 1024);
	}
	else {
		return sprintf "%d B", $value;
	}
}

