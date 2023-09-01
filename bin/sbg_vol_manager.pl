#!/usr/bin/env perl

use warnings;
use strict;
use English qw(-no_match_vars);
use Getopt::Long;
use IO::File;
use Net::SB;

our $VERSION = 0.2;

my $division_name;
my $list_volumes = 0,
my $add_volumes = 0,
my $activate_volumes = 0;
my $deactivate_volumes = 0;
my $delete_volumes = 0;
my @volume_ids;
my @volume_names;
my $vol_connection_file = sprintf "%s/.aws/credentials", $ENV{HOME};
my $profile = 'default';
my $access_mode;
my $credentials_file;
my $verbose;
my $help;

my $doc = <<DOC;

A script to manage external volumes on Seven Bridges.

This can mount one or more volumes, assuming the same connection credentials.
Mounted volumes can be listed, activated, deactivated, or deleted (removed).

Currently only supports AWS S3 storage.

The connection file must have the following IAM connection role contents:

  aws_access_key_id = AKIA3MNGGGFGVxxxxxxx
  aws_secret_access_key = e2PYXqFo9c8VbtHfBxxxxxxxxxxx

This is essentially identical to an AWS credentials file, except multiple profiles
are not supported.

If the name isn't provided, it's derived from the bucket name with munging.


See the documentation about mounting AWS S3 volumes for more information at 
https://docs.sevenbridges.com/docs/amazon-web-services-simple-storage-service-aws-s3-volumes


USAGE:
    sbg_vol_manager.pl -d big-shot-pi --list

    sbg_vol_manager.pl -d big-shot-pi --add -c secrets.txt -v my-data-bucket


Main Functions: (select one)
    --list                      list attached volumes with status and mode
    --add                       attach new volume(s)
    --activate                  activate the volume(s)
    --deactivate                deactivate the volume(s)
    --delete                    remove the volume(s)
    
Required:
    -d --division               name of the division
    -v --volume                 name of the bucket, repeat as necessary
    -n --name                   name of the attached volume, repeat as necessary
    
    Options for adding:
    -c --connection             text file with IAM secret keys
                                  default $vol_connection_file
    --profile                   the AWS profile name, default '$profile'
    --mode                      specify access mode, default RO
                                    RO for Read Only
                                    RW for Read Write
General:
    --cred          <file>      Path to SBG credentials file 
                                  default ~/.sevenbridges/credentials
    --verbose                   Print all https processing commands
    -h --help                   Print this help

DOC


#### Options

if (scalar(@ARGV) > 0) {
	GetOptions(
		'd|division=s'      => \$division_name,
		'list!'             => \$list_volumes,
		'add!'              => \$add_volumes,
		'activate!'         => \$activate_volumes,
		'deactivate!'       => \$deactivate_volumes,
		'delete!'           => \$delete_volumes,
		'v|volume=s'        => \@volume_ids,
		'n|name=s'          => \@volume_names,
		'c|connection=s'    => \$vol_connection_file,
		'profile=s'         => \$profile,
		'mode=s'            => \$access_mode,
		'cred=s'            => \$credentials_file,
		'verbose!'          => \$verbose,
		'h|help!'           => \$help,
	) or die "please recheck your options!! Run with --help to display options\n";
}
else {
	print $doc;
	exit 0;
}
if ($help) {
	print $doc;
	exit 0;
}


#### Main

check_options();

# Initialize SBG object
my $Sb = Net::SB->new(
	div        => $division_name,
	cred       => $credentials_file,
	verbose    => $verbose,
) or die " unable to initialize SB object!\n";


if ($list_volumes) {
	go_print_volume_list();
}
if ($add_volumes) {
	go_add_new_volumes();
}
if ($activate_volumes) {
	go_activate_volumes();
}
if ($deactivate_volumes) {
	go_deactivate_volumes();
}
if ($delete_volumes) {
	go_delete_volumes();
}
exit 0;




#### Subroutines

sub check_options {
	if (@ARGV) {
		# presume these are volumes
		push @volume_ids, @ARGV;
	}
	my $check = $list_volumes + $add_volumes + $activate_volumes + $deactivate_volumes +
		$delete_volumes;
	if ($check == 0) {
		print " Must specify an action! See help\n";
		exit 1;
	}
	elsif ($check > 1) {
		print " Must specify one action only! See help\n";
		exit 1;
	}
	unless ($division_name) {
		print " Division name is required! See help\n";
		exit 1;
	}
	if ( not $list_volumes and scalar(@volume_ids) == 0 ) {
		print " Volume names must be specified! See help\n";
		exit 1;
	}
	if ( $add_volumes and not $vol_connection_file ) {
		print " A connection file must be specified to add new volumes! See help\n";
		exit 1;
	}
	if (@volume_ids and @volume_names) {
		unless ( scalar(@volume_ids) == scalar(@volume_names) ) {
			print " Non-equal lists of volume IDs and Names provided! See help\n";
			exit 1;
		}
	}
}

sub go_print_volume_list {
	my $volumes = $Sb->list_volumes;
	if ($volumes and scalar @{$volumes}) {
		print "Active  Mode  Name\n";
		foreach my $p (@{$volumes}) {
			printf "%-6s  %-4s  %s\n", $p->active ? 'yes' : 'no ', $p->mode, $p->name;
		}
	}
	else {
		print STDERR " No volumes to list!\n";
	}
}

sub go_add_new_volumes {
	# read the connection file and setup options hash
	my $fh = IO::File->new($vol_connection_file) or
		die "unable to open file '$vol_connection_file'! $OS_ERROR\n";
	my %options;
	my $header = sprintf "[%s]", $profile;
	my $line = $fh->getline;
	while ($line) {
		chomp $line;
		if ($line eq $header) {
			# found the header
			undef $line;
			while (my $line2 = $fh->getline) {
				last if (substr($line2, 0, 1) eq '[');
				if ($line2 =~ /^ (\w+) \s? = \s? ( [\/\-\w]+ )$/x ) {
					$options{$1} = $2;
				}		
			}
		}
		else {
			$line = $fh->getline || undef;
		}
	}
	$fh->close;
	unless (keys %options) {
		die "no connection details found from '$vol_connection_file'!\n";
	}
	$options{access} = $access_mode;

	# iterate through the volumes
	while (@volume_ids) {
		my $id = shift @volume_ids;
		my $name;
		if (@volume_names) {
			$name = shift @volume_names;
		}
		else {
			$name = $id;
			$name =~ s/[\-]/_/g;
			if (length($name) > 32) {
				$name = substr $name, 0, 32;
			}
		}
		$options{bucket} = $id;
		$options{name}   = $name;
		my $volume = $Sb->attach_volume(%options);
		if ( $volume and ref($volume) eq 'Net::SB::Volume' ) {
			print "  > successfully attached volume $id as '$name'\n";
		}
		else {
			print "  ! failed to attach volume '$id' as '$name'\n";
		}
	}
}

sub go_activate_volumes {
	foreach my $id (@volume_ids) {
		my $volume = $Sb->get_volume($id);
		unless ($volume) {
			print STDERR " ! volume $id not found\n";
			next;
		}
		if ( $volume->activate ) {
			print "  > activated $id\n";
		}
		else {
			print "  ! unable to activate $id\n";
		}
	}
}

sub go_deactivate_volumes {
	foreach my $id (@volume_ids) {
		my $volume = $Sb->get_volume($id);
		unless ($volume) {
			print STDERR " ! volume $id not found\n";
			next;
		}
		if ( $volume->deactivate ) {
			print "  > deactivated $id\n";
		}
		else {
			print "  ! unable to deactivate $id\n";
		}
	}
}

sub go_delete_volumes {
	foreach my $id (@volume_ids) {
		my $volume = $Sb->get_volume($id);
		unless ($volume) {
			print STDERR " ! volume $id not found\n";
			next;
		}
		if ( $volume->delete ) {
			print "  > deleted $id\n";
		}
		else {
			print "  ! unable to delete $id\n";
		}
	}
}

