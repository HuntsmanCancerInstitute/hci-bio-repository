#!/usr/bin/env perl

use warnings;
use strict;
use English qw(-no_match_vars);
use Getopt::Long;
use IO::File;
use IO::Handle;
use Text::CSV;
use List::Util qw(mesh);
use Config::Tiny;
use Date::Parse;
use Forks::Super;
use Net::Amazon::S3::Client;
use Text::Levenshtein::Flexible;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use RepoProject;
use RepoCatalog;


our $VERSION = 0.5;

my $doc = <<END;

A script to prepare and upload a GNomEx Repository Project to its
corresponding CORE Lab account AWS bucket.

It will identify candidate files to upload from a given GNomEx Repository
project, either Request and Analysis. The Project Manifest file is used to
generate the list of candidate files, so the Project must already be
scanned; see corresponding process_analysis_project.pl and
process_request_project.pl applications. If the project was previously
uploaded and the bucket/prefix exists, then it will be recursively listed
and compared with the manifest file to determine which file(s) need to be
uploaded. 

The target AWS bucket and prefix are determined from a Project catalog
database. See the manage_repository.pl application.

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
  
  upload_repo_projects.pl --manifest <file> --bucket <name> --prefix <text> --profile <text>


OPTIONS:
  
  Project information from database:
    -c --cat <path>         Path to metadata catalog database
    -p --project <text>     Project identifier
  
  Or manually specify project information:
    --manifest <file>       CSV file of local relative file paths to upload
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
my $manifest_file;
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
my $verbose;
my $help;

if (@ARGV) {
	GetOptions(
		'c|catalog=s'           => \$cat_file,
		'p|project=s'           => \$project_id,
		'manifest=s'            => \$manifest_file,
		'bucket=s'              => \$bucket_name,
		'prefix=s'              => \$prefix,
		'profile=s'             => \$profile,
		'check!'                => \$check_only,
		'aa!'                   => \$include_autoanal,
		'class=s'               => \$storage_class,
		'dryrun!'               => \$dryrun,
		'f|forks=i'             => \$cpu,
		'cred=s'                => \$aws_cred_file,
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
upload_files_super() unless $check_only;

# finished


#### Subroutines

sub check_options {
	if ($help) {
		print $doc;
		exit 0;
	}
	if ( $project_id or $cat_file ) {
		unless ($project_id) {
			print " No project identifer provided! See help\n";
			exit 1;
		}
		unless ($cat_file) {
			print " No catalog file provided! See help\n";
			exit 1;
		}
	}
	elsif ( $manifest_file and $bucket_name and $prefix and $profile ) {
		print " Using provided manifest, bucket, prefix, and profile\n";
	}
	else {
		print " Not enough source or destination specifications provided! See help\n";
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
			print " Unrecognized storage class – see `aws s3 cp help`\n";
			exit 1;
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
		$bucket_name  = $Entry->bucket;
		$profile      = $Entry->profile;
		$prefix       = $Entry->prefix;
		$project_path = $Entry->path;
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
	unless ($manifest_file) {
		print " ! $project_id has not been scanned or no manifest file provided\n";
		exit 1;
	}

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
	) or die "unable to open AWS S3 Client!";
	
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
		printf " ! Bucket '%s' in '%s' for %s does not exist\n", $bucket_name,
			$Entry ? $Entry->core_lab : '???', $project_id;
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
	my $manifest  = $Project->manifest_file;
	unless ( -e $manifest ) {
		printf " ! no manifest file %s\n", $manifest;
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
		if (%existing and not $Entry->upload_datestamp) {
			printf " ! Project %s has contents in %s/%s but no upload date\n",
				$Entry->id, $bucket_name, $prefix;
		}
	}
	

	# manifest file itself
	my @man_stat = stat $manifest;
	if ( exists $existing{$manifest} ) {
		my $local_time  = $man_stat[9];
		my $remote_time = str2time( $existing{$manifest} );
		if ( $local_time > $remote_time ) {
			push @upload_list, $manifest;
			$count++;
			$size += $man_stat[7];
			if ($verbose) {
				print "   > including file $manifest\n";
			}
		}
		else {
			if ($verbose) {
				print "   > skipping uploaded file $manifest\n";
			}
		}
	}
	else {
		push @upload_list, $manifest;
		$count++;
		$size += $man_stat[7];		
		if ($verbose) {
			print "   > including file $manifest\n";
		}
	}

	# zip list file - this is not in the manifest, but the zip file is
	if ($ziplist) {
		my @zip_stat = stat $ziplist;
		if ( exists $existing{$ziplist} ) {
			my $local_time  = $zip_stat[9];
			my $remote_time = str2time( $existing{$ziplist} );
			if ( $local_time > $remote_time ) {
				push @upload_list, $ziplist;
				$count++;
				$size += $zip_stat[7];
				if ($verbose) {
					print "   > including file $ziplist\n";
				}
			}
			else {
				if ($verbose) {
					print "   > skipping uploaded file $ziplist\n";
				}
			}
		}
		else {
			push @upload_list, $ziplist;
			$count++;
			$size += $zip_stat[7];		
			if ($verbose) {
				print "   > including file $ziplist\n";
			}
		}
	}
	
	# parse manifest
	my $csv = Text::CSV->new();
	my $fh  = IO::File->new($manifest) or
		die " Cannot read manifest file '$manifest'! $OS_ERROR";
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
		printf "   > %d files were in AutoAnalysis folder and not included\n", $upcnt;
	}

}

sub upload_files_super {
	return unless (@upload_list);
	printf " > Uploading in %d forks\n", $cpu;

	# start each fork
	my $running = 0;
	my $success = 0;
	my $failure = 0;
	my $start   = time;
	my %pids;
	for my $i ( 1 .. $cpu ) {
		last unless @upload_list;
		if ($verbose) {
			print "   starting fork $i....\n";
		}
		my $pid = fork {
			sub => \&child_super_callback,
			child_fh => [ qw(in out) ],
		};
		sleep 1;
		$pids{$i} = $pid;
		# start first job for this child
		my $next = shift @upload_list;
		print { $pid->{child_stdin} } "$next\n";
# 		if ($verbose) {
# 			print "   child $i: uploading $next\n";
# 		}
		$running++;
	}
	
	# monitor each fork
	while ($running) {
		sleep 2;
		foreach my $i ( 1 .. $cpu ) {
			# check result from each fork
			my $pid = $pids{$i} or next;
			my $result = readline $pid->{child_stdout};
			if ( $result and $result =~ /\w+/ ) {
				if ($result =~ /^upload:/) {
					# normal success
					$success++;
					print "   child $i: $result";
				}
				elsif ($result =~ /^error/) {
					# real errors are printed directly to stderr by the child aws command
					$failure++;
					print "   ! child $i: $result";
				}
				elsif ($result =~ /^ \s => \s executing:/x) {
					# a verbose statement from the fork
					print "$result";
					next;
				}
				else {
					# something else
					print "   ? child $i: $result\n";
				}
				my $next = shift @upload_list || undef;
				if ($next) {
# 					if ($verbose) {
# 						print "   child $i: uploading $next\n";
# 					}
					print { $pid->{child_stdin} } "$next\n";
				}
				else {
					print { $pid->{child_stdin} } "YOU_ARE_DONE\n";
					$running--;
					$pid->dispose;
					undef $pids{$i};
					if ($verbose) {
						print "   child $i: shutting down\n";
					}
				}
			}
			# else continue waiting
		}
	}

	# completed
	my $elapsed = time - $start;
	printf "\n > Completed %d uploads in %.1f minutes, %.2f MB/sec\n", $success,
		$elapsed / 60, ($size / 1048576 ) / ($elapsed );
	if ($failure) {
		printf " ! There were %d upload failures\n", $failure;
	}
	elsif ($success) {
		if ($Entry) {
			$Entry->upload_datestamp(time);
			if ($include_autoanal) {
				$Entry->autoanal_datestamp(time);
			}
		}
	}
}

sub child_super_callback {
	# call back in a child

	# continuous loop
	while (1) {
		while ( defined (my $file = readline STDIN ) ) {
			chomp $file;
			exit 0 if $file eq 'YOU_ARE_DONE';
			my $remote_path = sprintf "s3://%s/%s/%s", $bucket_name, $prefix, $file;
			if ($using_flat_req) {
				my $alt = $file;
				$alt =~ s/^Fastq\///;
				$remote_path = sprintf "s3://%s/%s/%s", $bucket_name, $prefix, $alt;
			}
			my $command = sprintf qq(aws s3 cp '%s' '%s' --profile %s --no-progress),
				$file, $remote_path, $profile;
			if ($storage_class) {
				$command .= sprintf(" --storage-class %s", $storage_class);
			}
			if ($dryrun) {
				$command .= ' --dryrun';
			}
			if ($verbose) {
				printf " => executing: %s\n", $command;
			}
			my $result = qx($command);
			chomp $result;
			unless ($result) {
				# actual errors are probably not captured here
				$result = $dryrun ? "dryrun with $file" : "error with $file";
			}
			printf "%s\n", $result;
		}
		sleep 2;
	}

}

sub format_human_size {
	my $value = shift;
	if ($value > 1000000000) {
		return sprintf "%.1f GB", ($value / 1073741824);
	}
	elsif ($value > 1000000) {
		return sprintf "%.1f MB", ($value / 1048576);
	}
	elsif ($value > 1000) {
		return sprintf "%.1f KB", ($value / 1024);
	}
	else {
		return sprintf "%d B", $value;
	}
}

