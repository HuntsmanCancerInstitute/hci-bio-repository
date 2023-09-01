#!/usr/bin/env perl

use warnings;
use strict;
use English qw(-no_match_vars);
use Getopt::Long qw(:config no_ignore_case);
use IO::File;
use JSON::PP;
use Net::SB;

our $VERSION = 0.1;

my $doc = <<END;

A script to generate the bash commands for exporting a lab division from
Seven Bridges to AWS.

This takes as input the list of projects generated from the script 
'generate_bulk_project_export_prefixes.pl'. It also requires an AWS 
account lookup file, which is a TSV file containing the SB division ID, 
CORE Browser lab name, and AWS account number.

It will generate several custom, lab-specific, bash scripts with the 
commands necessary for exporting the projects and files. These include the
following script and files:

    - Script to create AWS buckets
    - Write the custom AWS IAM JSON policy for mounting SB buckets
    - Script to copy Legacy GNomEx SB projects into a new, consolidated SB project
      for purposes of manual file archive restoration ease
    - Script to mount the AWS buckets in the SB lab division
    - Script to bulk export active SB projcts to mounted bucket volumes
    - Script to batch download and copy restored project files on EC2 node
    - Script to remove the created projects of consolidated legacy GNomEx projects


Options

-i --input   <file>    The input list of project identifiers and prefixes
-o --out     <file>    The output directory to write the files
-a --account <file>    The AWS account lookup file
-h --help              Show this help

END



# Command line options
my $infile;
my $outdir;
my $account_file;
my $vol_connection_file = sprintf "%s/.aws/credentials", $ENV{HOME};
my $help;

if (@ARGV) {
	GetOptions(
		'i|input=s'         => \$infile,
		'o|out=s'           => \$outdir,
		'a|account=s'       => \$account_file,
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
unless ($infile) {
	die " must provide an input file!\n";
}
unless ($account_file) {
	die " must provide the AWS account lookup file!\n";
}
if ($outdir) {
	unless ( -e $outdir and -d _ and -w _ ) {
		die " output directory not exist or not writeable or something!\n";
	}
}
else {
	die " must provide an output directory!\n";
}


# Global Variables
my %accounts;      # hash of SBG id to array [lab name, account number]
my @buckets;       # array list of AWS buckets
my %leg2proj;      # hash of bucket to array of legacy SBG projects [project, prefix]
my %buck2proj;     # hash bucket to array of active SBG projects [project, prefix]
my %buck2vol;      # hash of bucket to mounted volume name    
my @rest_projects; # list of the SBG restoration projects [project, bucket, prefix]
my $division;
my $profile;

# custom private variables
my $region = 'us-east-1';
my $private = 'tjparnell'; # private aws profile
my $private_bucket = 's3://hcibioinfo-timp-test/AmazonLinuxExecutables';

# Main Functions
load_accounts();
load_table();
write_credential();
create_bucket_cmd();
create_sbg_policy();
create_restoration_project_cmd();
mount_bucket_cmd();
sbg_export_cmd();
aws_export_cmd();
cleanup_cmd();

print " > Finished with $division\n";
exit 0;



######## Subroutines ###############

sub load_accounts {
	my $infh = IO::File->new($account_file)
		or die " unable to read $account_file! $OS_ERROR";
	my $header = $infh->getline;
	while ( my $line = $infh->getline ) {
		next unless $line =~ /\w+/;
		chomp $line;
		my @data = split /\t/, $line;
		$accounts{ $data[0] } = [ $data[1], $data[2] ];
	}
	$infh->close;
}

sub load_table {
	my $infh = IO::File->new($infile)
		or die " unable to read $infile! $OS_ERROR";
	my $header = $infh->getline;
	if ( $header !~ /^SBGID \t Archived \t Bucket \t Prefix/x ) {
		die " header doesn't match expected contents: SBGID Archived Bucket Prefix\n";
	}
	my $n = 0;
	my %all_buckets;
	while ( my $line = $infh->getline ) {
		chomp $line;
		next unless $line =~ /\w+/;
		my @data    = split /\t/, $line;
		my $project = $data[0];
		my $bucket  = $data[2];
		my $prefix  = $data[3];
		$all_buckets{$bucket} += 1;

		# extract the SBG lab division - this should be unique within the file
		unless ($division) {
			my @bits  = split m|/|, $project, 2;
			$division = $bits[0];
		}
		unless ($profile) {
			($profile) = ($bucket =~ /^(cb \- [a-z]+ )/x);
		}

		# check the project
		if ($project =~ m|^$division / $division / (.+) $|x ) {
			# we have a legacy project
			$leg2proj{ $bucket } ||= [];
			push @{ $leg2proj{ $bucket } }, [$1, $prefix];
		}
		elsif ( $data[1] eq 'y' ) {
			# we have an otherwise archived project
			# this gets both direct export and aws download/copy
			# extract the short project name from the project id
			my ($shortname) = ( $project =~ m|^$division / (.+) $|x );
			push @rest_projects, [$division, $project, $shortname, $bucket, $prefix];
			$buck2proj{ $bucket } ||= [];
			push @{ $buck2proj{$bucket} }, [$project, $prefix];
		}
		else {
			# we have a regular active project
			$buck2proj{ $bucket } ||= [];
			push @{ $buck2proj{$bucket} }, [$project, $prefix];
		}
		$n++;
	}
	$infh->close;

	@buckets = sort {$a cmp $b} keys %all_buckets;
	printf " > loaded $n projects for %d buckets\n", scalar @buckets;
}

sub write_credential {

	# Seven Bridges credentials
	my $Sb = Net::SB->new(division => $division);
	my $token = $Sb->token;
	unless ($token) {
		print STDERR " ! no token for division '$division'\n";
		exit 1;
	}
	my $outfile = sprintf "%s/sbgcred.txt", $outdir;
	my $outfh   = IO::File->new($outfile, '>')
		or die " unable to write to '$outfile'! $OS_ERROR";
	$outfh->printf("[%s]\nauth_token = %s\napi_endpoint = %s\n", $division, $token,
		$Sb->endpoint);
	$outfh->close;
	printf " > wrote SB credential file '%s'\n", $outfile;
	undef $outfh;

	# AWS credentials
	my $infh = IO::File->new($vol_connection_file) or
		die "unable to open file '$vol_connection_file'! $OS_ERROR\n";
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
				elsif ($line2 =~ /^ (\w+) \s? = \s? ( [\/\-\w]+ )$/x ) {
					$options{$profile}{$1} = $2;
				}		
			}
			$line ||= $infh->getline || undef;
		}
		elsif ($line eq "[$private]") {
			undef $line;
			while (my $line2 = $infh->getline) {
				if (substr($line2, 0, 1) eq '[') {
					$line = $line2;
					last;
				}
				elsif ($line2 =~ /^ (\w+) \s? = \s? ( [\/\-\w]+ )$/x ) {
					$options{$private}{$1} = $2;
				}		
			}
			$line ||= $infh->getline || undef;
		}
		else {
			$line = $infh->getline || undef;
		}
	}
	$infh->close;
	$outfile = sprintf "%s/awscred.txt", $outdir;
	$outfh   = IO::File->new($outfile, '>')
		or die " unable to write to '$outfile'! $OS_ERROR";
	my $cred = <<CRED;
[$profile]
aws_access_key_id = $options{$profile}{aws_access_key_id}
aws_secret_access_key = $options{$profile}{aws_secret_access_key}
region = $region

[$private]
aws_access_key_id = $options{$private}{aws_access_key_id}
aws_secret_access_key = $options{$private}{aws_secret_access_key}
region = $region

CRED
	$outfh->print($cred);
	$outfh->close;
	printf " > wrote empty AWS credential file '%s'\n", $outfile;
}

sub create_bucket_cmd {
	
	# AWS account information
	unless ( exists $accounts{$division} ) {
		print STDERR " ! no AWS account info for '$division'\n";
		exit 1;
	}
	my $labname   = $accounts{$division}->[0];
	my $labnumber = $accounts{$division}->[1];
	unless ($labname =~ /\w+/) {
		print STDERR " ! no AWS account info for '$division'\n";
		exit 1;
	}

	# parameter file
	my $paramfile = sprintf "%s/account_parameters.txt", $outdir;
	my $paramfh   = IO::File->new($paramfile, '>')
		or die " unable to write to '$paramfile'! $OS_ERROR";
	foreach my $bucket ( @buckets ) {
		$paramfh->printf("%s\t%s\t%s\n", $bucket, $labname, $labnumber);
	}
	$paramfh->close;
	printf " > wrote parameter file '%s'\n", $paramfile;

	# script file
	my $outfile = sprintf "%s/create_buckets.sh", $outdir;
	my $outfh   = IO::File->new($outfile, '>')
		or die " unable to write to '$outfile'! $OS_ERROR";
	my $script = <<END;
#!/bin/bash

# a script to generate new buckets for a lab account
# must be run on hci-clingen1

# must explicitly set the Account Controller credentials here
# or pass as arguments
USER=\$1
PASS=\$2

rm -f FAILED.create_bucket
trap 'touch FAILED.create_bucket' ERR TERM

if [[ "\$HOSTNAME" -ne "hci-clingen1" ]]
then
	echo 'must be run on hci-clingen1'
	exit
fi

echo '==== creating CORE Browser AWS buckets for $labname ===='
echo 'this make take a while...'
date
echo

/usr/local/aws-account-creator/AccountCreationController \\
-u \$USER -p \$PASS \\
-m update -i account_parameters.txt -ou 'Core Browser' \\
-o $profile.out.txt

echo
echo '===== Done ===='
date

END
	$outfh->print($script);
	$outfh->close;
	print " > wrote script file '$outfile'\n";
}

sub create_sbg_policy {
	my @resource = map { sprintf("arn:aws:s3:::%s", $_) } @buckets;
	my @resource_root = map { sprintf("arn:aws:s3:::%s/*", $_) } @buckets;
	
	# the SBG read/write policy
	my $data = {
		'Version' => '2012-10-17',
		'Statement' => [
			{
				'Sid' => 'GrantReadOnBuckets',
				'Action' => [
					's3 =ListBucket',
					's3 =GetBucketCORS',
					's3 =GetBucketLocation'
				],
				'Effect' => 'Allow',
				'Resource' => \@resource,
			},
			{
				'Sid' => 'GrantReadOnObjects',
				'Action' => [
					's3 =GetObject'
				],
				'Effect' => 'Allow',
				'Resource' => \@resource_root,
			},
			{
				'Sid' => 'GrantWriteOnObjects',
				'Action' => [
					's3 =PutObject',
					's3 =GetObjectAcl',
					's3 =PutObjectAcl',
					's3 =AbortMultipartUpload',
					's3 =ListMultipartUploadParts'
				],
				'Effect' => 'Allow',
				'Resource' => \@resource_root,
			},
			{
				'Sid' => 'RequestReadOnCopySourceObjects',
				'Action' => [
					's3 =GetObject'
				],
				'Effect' => 'Allow',
				'Resource' => [
					'arn =aws =s3 = = =sbg-main/*',
					'arn =aws =s3 = = =sbg-main-us-west-2/*'
				]
			}
		]
	};

	# write a pretty json file
	my $outfile = sprintf "%s/sbg-rw-iam-policy.json", $outdir;
	my $outfh   = IO::File->new($outfile, '>')
		or die " unable to write to '$outfile'! $OS_ERROR";
	my $json = JSON::PP->new->ascii->pretty->allow_nonref;
	$outfh->print( $json->encode($data) );
	$outfh->close;
	print " > wrote policy file '$outfile'\n";
}

sub create_restoration_project_cmd {

	return unless keys %leg2proj;

	# prepare script
	my $outfile = sprintf "%s/create_restoration_projects.sh", $outdir;
	my $outfh   = IO::File->new($outfile, '>')
		or die " unable to write to '$outfile'! $OS_ERROR";
	my $header = <<END;
#!/bin/bash

# a script to consolidate GNomEx Legacy SB projects for restoration
# one or more projects will be copied to a new project 

rm -f FAILED.restoration_copy
trap 'touch FAILED.restoration_copy' ERR TERM
export PATH=\$PWD:\$PATH

echo '===== copying Legacy GNomEx projects into consolidated projects ====='
date
echo 


END
	$outfh->print($header);
	
	# walk through buckets
	foreach my $bucket ( sort {$a cmp $b} keys %leg2proj ) {

		# base command
		my $new_project = $bucket;
		$new_project =~ s/cb\-[a-z]+/export/; 
		my $new_project2 = $new_project;
		$new_project2 =~ s/\-/_/g;
		$outfh->printf( qq(echo '==== copying %d Legacy projects to project %s ===='\n\n), 
			scalar @{ $leg2proj{$bucket} }, $new_project );
		my $command = sprintf 
"sbg_async_folder_copy --cred sbgcred.txt --wait 10 --source %s/%s --destination %s/%s --new ",
			$division, $division, $division, $new_project2;
		
		# add each project
		foreach my $item ( @{ $leg2proj{$bucket} } ) {
			
			# extract the legacy GNomEx project name
			my (undef, $proj_name) = split m|/|, $item->[0];
			
			# add the legacy path to the command
			$command .= sprintf " -f %s ", $item->[0];
			
			# add the information to the restored project array
			push @rest_projects, [ 
				$division,
				sprintf("%s/%s", $new_project, $proj_name),
				$proj_name,
				$bucket,
				$item->[1], # prefix
			];
		}
		
		$outfh->printf("%s\n\n", $command);
	}

	# finish
	$outfh->print("\necho\necho '====== Done ====='\ndate\n\n");
	$outfh->close;
	printf " > wrote script file '$outfile'\n";
}

sub mount_bucket_cmd {

	return unless keys %buck2proj;
	
	# prepare script
	my $outfile = sprintf "%s/mount_buckets.sh", $outdir;
	my $outfh   = IO::File->new($outfile, '>')
		or die " unable to write to '$outfile'! $OS_ERROR";
	my $header = <<END;
#!/bin/bash

# a script to mount the AWS buckets within the SBG lab division

rm -f FAILED.mount_volumes
trap 'touch FAILED.mount_volumes' ERR TERM
export PATH=\$PWD:\$PATH

echo '===== mounting AWS buckets as volumes in $division ====='
date
echo 

END
	$outfh->print($header);

	# walk through buckets
	my @items;
	foreach my $bucket ( sort {$a cmp $b} keys %buck2proj ) {
		my $mount = $bucket;
		$mount =~ s/\-/_/;
		if (length $bucket > 32) {
			$mount = substr $bucket, 0, 32;
		}
		push @items, sprintf "-v %s -n %s ", $bucket, $mount;
		$buck2vol{$bucket} = $mount;
	}

	# write command
	$outfh->printf(
"sbg_vol_manager --cred sbgcred.txt --connection awscred.txt --division %s --add --mode RW %s\n",
		$division, join( q(), @items )
	);
	$outfh->printf("echo\necho '======= Done ======'\n\n");
	$outfh->close;
	printf " > wrote script file '$outfile'\n";	
}

sub sbg_export_cmd {

	return unless keys %buck2proj;

	# prepare script
	my $outfile = sprintf "%s/export_projects.sh", $outdir;
	my $outfh   = IO::File->new($outfile, '>')
		or die " unable to write to '$outfile'! $OS_ERROR";
	my $header = <<END;
#!/bin/bash

# a script to export active SB projects to mounted buckets

rm -f FAILED.active_export
trap 'touch FAILED.active_export' ERR TERM
export PATH=\$PWD:\$PATH

echo '===== exporting active projects in $division ====='
date
echo 

END
	$outfh->print($header);

	# walk through buckets
	foreach my $bucket ( sort {$a cmp $b} keys %buck2proj ) {

		foreach my $item ( @{ $buck2proj{ $bucket } } ) {
			$outfh->printf(qq(echo '===== exporting %s to volume %s/%s ====='\n\n), 
				$item->[0], $buck2vol{$bucket}, $item->[1] );
			$outfh->printf(
"sbg_project_manager --cred sbgcred.txt --export %s --volume %s --prefix %s\n\n",
				$item->[0], $buck2vol{$bucket}, $item->[1] );
		}
	}

	# finish
	$outfh->printf("echo\necho '======= Done ======'\ndate\n\n");
	$outfh->close;
	printf " > wrote script file '$outfile'\n";	
}

sub aws_export_cmd {

	return unless @rest_projects;

	# prepare script
	my $outfile = sprintf "%s/download_and_copy.sh", $outdir;
	my $outfh   = IO::File->new($outfile, '>')
		or die " unable to write to '$outfile'! $OS_ERROR";
	my $header = <<END;
#!/bin/bash

# a script to batch download restored SB projects and copy to AWS buckets
# this is intended to run on an EC2 node in $region
# recommend a t3.nano instance with AmazonLinux

# set the size of the EB2 volume in GB when generating 
VOLSIZE=25

rm -f FAILED.download_copy
trap 'touch FAILED.download_copy' ERR TERM
export PATH=\$PWD:\$PATH

# copy AWS credentials
mkdir -p .aws
if [ -e awscred.txt ]
then
	mv awscred.txt .aws/credentials
fi

# get executables
if [ -e aria2c ]
then
	echo "executables found"
else
	echo "retrieving executables"
	aws s3 cp --profile $private $private_bucket/aria2c ./ \\
	&& chmod +x aria2c
	aws s3 cp --profile $private $private_bucket/sbg_project_manager ./ \\
	&& chmod +x sbg_project_manager
fi


# reusable global parameters
PROFILE=$profile
DIVISION=""
SBGPROJECT=""
PROJECT=""
BUCKET=""
PREFIX=""

# main transfer function
function transfer()
{
	echo
	echo "=================================================================="
	echo "====== \$SBGPROJECT ======"
	echo "=================================================================="
	echo
	if [ -e \$PROJECT.finished ]
	then
		echo "==== \$PROJECT completed ===="
	else

		# generate batched download lists
		if [[ -n `ls \$PROJECT.list*.txt` ]]
		then
			echo "==== Using existing \$PROJECT download lists ===="
		else
			echo "==== Generating \$PROJECT download list ===="
			date
			sbg_project_manager --url --aria --batch \$VOLSIZE \\
			--cred sbgcred.txt \\
			--out \$PROJECT.list.txt \\
			\$DIVISION/\$SBGPROJECT
		fi

		# transfer batched download lists
		for listfile in \$PROJECT.list*.txt
		do
			echo
			echo "=================================================================="
			echo "==== Transferring \$listfile ===="
			aria2c --input-file \$listfile \\
			--max-concurrent-downloads=6 --max-connection-per-server=6 --split=6 \\
			--file-allocation=falloc --summary-interval=0 --show-console-readout=false \\
			&& aws s3 cp --profile \$PROFILE --recursive --storage-class GLACIER \\
			\$PROJECT s3://\$BUCKET/\$PREFIX/ \\
			&& rm -r \$PROJECT \$listfile
		done

		# completed with project
		touch \$PROJECT.finished
	fi
}

END
	$outfh->print($header);

	# walk through the restored items 
	foreach my $item ( @rest_projects ) {
		my $stanza = <<END;
DIVISION=$item->[0]
SBGPROJECT=$item->[1]
PROJECT=$item->[2]
BUCKET=$item->[3]
PREFIX=$item->[4]
transfer

END
		$outfh->print($stanza);	
	}
	
	# finish
	$outfh->printf("\necho\necho '======= Done ======'\ndate\n\n");
	$outfh->close;
	printf " > wrote script file '$outfile'\n";	

}

sub cleanup_cmd {

	return unless @rest_projects;

	# prepare script
	my $outfile = sprintf "%s/remove_restoration_projects.sh", $outdir;
	my $outfh   = IO::File->new($outfile, '>')
		or die " unable to write to '$outfile'! $OS_ERROR";
	my $header = <<END;
#!/bin/bash

# a script to remove the restoration SB projects for Legacy GNomEx projects
# ONLY after they have been copied to AWS

rm -f FAILED.cleanup
trap 'touch FAILED.cleanup' ERR TERM
export PATH=\$PWD:\$PATH

echo '===== Removing the Legacy GNomEx Restoration projects ====='
date
echo 


END
	$outfh->print($header);

	# walk through buckets
	foreach my $bucket ( sort {$a cmp $b} keys %leg2proj ) {

		# base command
		my $new_project = $bucket;
		$new_project =~ s/cb\-[a-z]+/export/; 
		$outfh->printf( "sbg_project_manager --cred sbgcred.txt --deleteproject %s/%s\n\n",
			$division, $new_project );
	}

	# finish
	$outfh->print("\necho\necho '====== Done ====='\ndate\n\n");
	$outfh->close;
	printf " > wrote script file '$outfile'\n";
}
