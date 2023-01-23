#!/usr/bin/env perl

use warnings;
use strict;
use English qw(-no_match_vars);
use Getopt::Long;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use Net::SB;

our $VERSION = 6.1;


######## Documentation
my $doc = <<END;
A simple script to check a Seven Bridge division, including listing 
projects and members. Lists are tab-delimited tables with a header 
and can be imported into whatever.

Member lists print division id, member name, member role in division, 
and email address.

Project lists print division id, project id, project name, and the 
number of members.

Tasks lists division id, project name, and task name.

Billing lists division id, billing group name, and running total cost 
to the billing group (not current invoice).

Default function is to simply print a count of the projects, members, 
tasks, and current total cost for each division.

Version: $VERSION

Example Usage:
    check_division.pl [options] 
    check_division.pl 

Main function: Pick one only
    --members                   Print list of members
    --projects                  Print list projects
    --tasks                     Print list of tasks
    --billing                   Print current billing charge
    --counts                    Print counts of members, projects, tasks, current bill
                                  (default action if no other function)

Options:
    --division <text>           Indicate the division to check. May repeat as necessary
                                  or simply append names at the end of the command
                                  Default is "default".
    --all                       Print all divisions for which you have a token
    --cred <file>               Path to credentials file 
                                  default ~/.sevenbridges/credentials
    --verbose                   Print processing commands

END





######## Process command line options
my @divisions;
my $all;
my $get_members  = 0;
my $get_projects = 0;
my $get_tasks    = 0;
my $get_counts   = 0;
my $get_billing  = 0;
my $credentials;
my $verbose = 0;

if (scalar(@ARGV) > 0) {
	GetOptions(
		'division=s'        => \@divisions,
		'all!'              => \$all,
		'members!'          => \$get_members,
		'projects!'         => \$get_projects,
		'tasks!'            => \$get_tasks,
		'billing!'          => \$get_billing,
		'counts!'           => \$get_counts,
		'cred=s'            => \$credentials,
		'verbose!'          => \$verbose,
	) or die "please recheck your options!\n$OS_ERROR\n";
}
else {
	print $doc;
	exit;
}

# any remaining divisions
if (@ARGV) {
	push @divisions, @ARGV;
}
unless (@divisions) {
	push @divisions, "default"
}
my $sanity = $get_members + $get_projects + $get_tasks + $get_counts + $get_billing;
if ($sanity == 0) {
	# default function
	$get_counts = 1;
}
elsif ($sanity > 1) {
	die "Only one function allowed at a time! members, projects, tasks, or counts!\n";
}




#### Initialize


# check divisions
if ($all) {
	# initialize without a specific division - should default to "default" token
	my $sb = Net::SB->new(
		verbose    => $verbose,
		cred       => $credentials,
	) or die "unable to initialize SB object!\n";
	my $div_list = $sb->list_divisions;
	die "no divisions found!\n" unless @{$div_list};
	# printf " > collected %d divisions\n", scalar @{$div_list};
	foreach my $div (@{$div_list}) {
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
	print "Division\tID\tName\tMembers\n";
}
elsif ($get_tasks) {
	print "Division\tProject\tTaskName\n";
}
elsif ($get_billing) {
	print "Division\tBillingName\tTotalCost\n";
}
elsif ($get_counts) {
	print "Division\tName\tMembers\tProjects\tTasks\tTotalCost\n";
}

# Loop through the divisions
foreach my $div (@divisions) {
	
	# get the SB project
	my $sbproject;
	if (ref($div) eq 'Net::SB::Division') {
		# we already have a project
		$sbproject = $div;
	}
	else {
		$sbproject = Net::SB->new(
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
		foreach my $member (@{$memberlist}) {
			printf "%s\t%s\t%s\t%s\n", $sbproject->id, $member->name, $member->role, 
				$member->email;
		}
	}
	elsif ($get_projects) {
		my $projectlist = $sbproject->list_projects;
		foreach my $project (@{$projectlist}) {
			my $memberlist = $project->list_members;
			printf "%s\t%s\t%s\t%d\n", $sbproject->id, $project->id, $project->name, 
				scalar(@{$memberlist});
		}
	}
	elsif ($get_tasks) {
		# using custom API execution for now
		my $tasklist = $sbproject->execute('GET', sprintf("%s/tasks", $sbproject->endpoint));
		foreach my $task (@{$tasklist}) {
			printf "%s\t%s\t%s\n", $sbproject->id, $task->{project}, $task->{name};
		}
	}
	elsif ($get_billing) {
		# using custom API execution for now
		my $billgroup = $sbproject->billing_group;
		my $bill = $sbproject->execute('GET', sprintf("%s/billing/groups/%s", 
			$sbproject->endpoint, $billgroup));
		printf "%s\t%s\t%.2f\n", $sbproject->id, $bill->{name}, 
			$bill->{balance}{amount} || 0;
	}
	elsif ($get_counts) {
		my $memberlist = $sbproject->list_members;
		my $projectlist = $sbproject->list_projects;
		my $tasklist = $sbproject->execute('GET', sprintf("%s/tasks", $sbproject->endpoint));
		my $billgroup = $sbproject->billing_group;
		my $bill = $sbproject->execute('GET', sprintf("%s/billing/groups/%s", 
			$sbproject->endpoint, $billgroup));
		printf "%s\t%s\t%d\t%d\t%d\t%.2f\n", $sbproject->id, $sbproject->name, 
			scalar(@{$memberlist}), scalar(@{$projectlist}), scalar(@{$tasklist}), 
			$bill->{balance}{amount} || 0;
	}
	else {
		die "you've got a bug! No function defined\n";
	}
}




