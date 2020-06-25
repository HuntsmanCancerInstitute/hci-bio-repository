#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use FindBin qw($Bin);
use lib $Bin;
use SB2;


my $VERSION = 5;


######## Documentation
my $doc = <<END;
A simple script to check a Seven Bridge division, including listing 
projects and members. Lists are tab-delimited tables with a header 
and can be imported into whatever.

Member lists print division id, member name, member role in division, 
and email address.

Project lists print division id, project id, project name, and the 
number of members.

When neither members or projects is specified, a list of division IDs, 
names, number of members, and number of projects is printed.

Version: $VERSION

Example Usage:
    check_division.pl [options] 
    check_division.pl 

Options:
    --division <text>           Indicate the division to check. May repeat as necessary
                                  or simply append names at the end of the command
    --all                       Print all divisions for which you have a token
    --members                   Print list of members
    --projects                  Print list projects
    --cred <file>               Path to credentials file 
                                  default ~/.sevenbridges/credentials
    --verbose                   Print processing commands

END





######## Process command line options
my @divisions;
my $all;
my $get_members;
my $get_projects;
my $credentials;
my $verbose = 0;

if (scalar(@ARGV) > 0) {
	GetOptions(
		'division=s'        => \@divisions,
		'all!'              => \$all,
		'members!'          => \$get_members,
		'projects!'         => \$get_projects,
		'cred=s'            => \$credentials,
		'verbose!'          => \$verbose,
	) or die "please recheck your options!\n$!\n";
}
else {
	print $doc;
	exit;
}

# any remaining divisions
if (@ARGV) {
	push @divisions, @ARGV;
}

if ($get_members and $get_projects) {
	die "Only one option allowed at a time! members or projects!\n";
}




#### Initialize


# check divisions
if ($all) {
	# initialize without a specific division - should default to "default" token
	my $sb = SB2->new(
		verbose    => $verbose,
		cred       => $credentials,
	) or die "unable to initialize SB object!\n";
	my $div_list = $sb->list_divisions;
	die "no divisions found!\n" unless @$div_list;
	# printf " > collected %d divisions\n", scalar @$div_list;
	foreach my $div (@$div_list) {
		my $token = $div->token;
		if ($token) {
			push @divisions, $div;
			# printf " > division %s has token $token\n", $div->id;
		}
		else {
			# printf " > division %s has no token\n", $div->id;
		}
	}
	# printf "collected %d divisions to check\n", scalar(@divisions);
}


# prepare output
if ($get_members) {
	print "Division\tName\tRole\tEmail\n";
}
elsif ($get_projects) {
	print "Division\tID\tName\t#Members\n";
}
else {
	print "Division\tName\t#Members\t#Projects\n";
}

# Loop through the divisions
foreach my $div (@divisions) {
	
	# get the SB project
	my $sbproject;
	if (ref($div) eq 'SB2::Division') {
		# we already have a project
		$sbproject = $div;
	}
	else {
		$sbproject = SB2->new(
			division   => $div,
			verbose    => $verbose,
			cred       => $credentials
		);
		unless ($sbproject) {
			print "unable to initialize SB project for $div! skipping\n";
			next;
		}
	}
	
	if ($get_members) {
		my $memberlist = $sbproject->list_members;
		foreach my $member (@$memberlist) {
			printf "%s\t%s\t%s\t%s\n", $sbproject->id, $member->name, $member->role, 
				$member->email;
		}
	}
	elsif ($get_projects) {
		my $projectlist = $sbproject->list_projects;
		foreach my $project (@$projectlist) {
			my $memberlist = $project->list_members;
			printf "%s\t%s\t%s\t%d\n", $sbproject->id, $project->id, $project->name, 
				scalar(@$memberlist);
		}
	}
	else {
		my $memberlist = $sbproject->list_members;
		my $projectlist = $sbproject->list_projects;
		printf "%s\t%s\t%d\t%d\n", $sbproject->id, $sbproject->name, 
			scalar(@$memberlist), scalar(@$projectlist);
	}
}




