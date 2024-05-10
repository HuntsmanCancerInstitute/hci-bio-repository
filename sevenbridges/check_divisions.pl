#!/usr/bin/env perl

use warnings;
use strict;
use English qw(-no_match_vars);
use Getopt::Long;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use Net::SB;

our $VERSION = 7;


######## Documentation
my $doc = <<END;
A simple script to check a Seven Bridge division, including listing 
projects and members. Lists are tab-delimited tables with a header 
and can be imported into whatever.

Member lists print division id, member name, member role in division, 
and email address.

Project lists print division id, project id, project name, the 
number of members, and the amount of Active and Archived storage 
in units of GB/Month.

Tasks lists division id, project name, and task name.

Invoice prints the storage, analysis, and total costs for the last 
completed invoice (not the current billing period).

Default function is to simply print a count of the projects, members, 
and tasks for each division.

Version: $VERSION

Example Usage:
    check_division.pl [options] 
    check_division.pl 

Main function: Pick one only
    --members                   Print list of members
    --projects                  Print list projects
    --tasks                     Print list of tasks
    --counts                    Print counts of members, projects, tasks
                                  (default action if no other function)
    --invoice                   Print totals from the last invoice

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
my $get_invoice  = 0;
my $credentials;
my $verbose = 0;
my $help;

if (scalar(@ARGV) > 0) {
	GetOptions(
		'division=s'        => \@divisions,
		'all!'              => \$all,
		'members!'          => \$get_members,
		'projects!'         => \$get_projects,
		'tasks!'            => \$get_tasks,
		'counts!'           => \$get_counts,
		'invoice!'          => \$get_invoice,
		'cred=s'            => \$credentials,
		'verbose!'          => \$verbose,
		'h|help!'           => \$help,
	) or die "please recheck your options!\n$OS_ERROR\n";
}
else {
	print $doc;
	exit;
}

if ($help) {
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
my $sanity = $get_members + $get_projects + $get_tasks + $get_counts + $get_invoice;
if ($sanity == 0) {
	# default function
	$get_counts = 1;
}
elsif ($sanity > 1) {
	die "Only one function allowed at a time! see help with --help!\n";
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
	printf STDERR " found %d divisions\n", scalar @{$div_list};
	my $default_token = $sb->token;
	foreach my $div (@{$div_list}) {
		my $token = $div->token;
		if ( $token and $token ne $default_token ) {
			push @divisions, $div;
		}
		# do not keep if it is using the default token
	}
	printf STDERR "collected %d divisions with unique tokens to check\n",
		scalar(@divisions);
}


# prepare output
if ($get_members) {
	print "Division\tName\tRole\tEmail\n";
}
elsif ($get_projects) {
	print "Division\tID\tMemberCount\tActive_GB\tArchive_GB\tName\n";
}
elsif ($get_tasks) {
	print "Division\tProject\tTaskName\n";
}
elsif ($get_counts) {
	print "Division\tName\tMembers\tProjects\tTasks\n";
}
elsif ($get_invoice) {
	print "Division\tEnding\tAnalysis\tStorage\tTotal\n";
}
else {
	print " no method defined!\n";
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

		# first collect the storage summary
		my $billgroup = $sbproject->billing_group;
		my $storage_list = $sbproject->execute('GET', 
			sprintf( "%s/billing/groups/%s/breakdown/storage", $sbproject->endpoint, 
			$billgroup ) );
		my %proj2cost;
		if ($storage_list and scalar @{ $storage_list } ) {
			foreach my $storage ( @{ $storage_list } ) {
				if ( exists $storage->{'project_name'} ) {
					my $name = $storage->{'project_name'};
					$proj2cost{ $name } = [
						$storage->{'active'}{'size'},
						$storage->{'archived'}{'size'},
					];
				}
				else {
					next;
				}
			}
		}

		# then get the project list itself
		my $projectlist = $sbproject->list_projects;
		foreach my $project (@{$projectlist}) {
			my $memberlist = $project->list_members;
			my $name = $project->name;
			printf "%s\t%s\t%d\t%.4f\t%.4f\t%s\n", 
				$sbproject->id,
				$project->id, 
				scalar( @{$memberlist} ),
				$proj2cost{ $name }->[0] || 0,
				$proj2cost{ $name }->[1] || 0,
				$name;
		}
	}
	elsif ($get_tasks) {
		# using custom API execution for now
		my $tasklist = $sbproject->execute('GET', sprintf("%s/tasks", $sbproject->endpoint));
		if ($tasklist and scalar @{ $tasklist } ) {
			foreach my $task (@{$tasklist}) {
				printf "%s\t%s\t%s\n", $sbproject->id, $task->{project}, $task->{name};
			}
		}
		else {
			printf STDERR " No task lists for %s\n", $sbproject->id;
		}
	}
	elsif ($get_counts) {
		my $memberlist = $sbproject->list_members;
		my $projectlist = $sbproject->list_projects;
		my $tasklist = $sbproject->execute('GET', sprintf("%s/tasks", $sbproject->endpoint));
		printf "%s\t%s\t%d\t%d\t%d\n", $sbproject->id, $sbproject->name, 
			scalar(@{$memberlist}), scalar(@{$projectlist}), scalar(@{$tasklist});
	}
	elsif ($get_invoice) {
		# using custom API execution for now
		my $billgroup = $sbproject->billing_group;
		my $invoice_list = $sbproject->execute('GET', 
			sprintf( "%s/billing/invoices?billing_group=%s", $sbproject->endpoint, 
			$billgroup ) );
		if ($invoice_list and scalar @{ $invoice_list } ) {
			my $last_id = $invoice_list->[-1]->{id};
			my $invoice = $sbproject->execute('GET', 
				sprintf( "%s/billing/invoices/%s", $sbproject->endpoint, $last_id) );
			if ($invoice and exists $invoice->{id}) {
				my $end = $invoice->{'invoice_period'}{'to'};
				$end =~ s/T.+$//;
				printf "%s\t%s\t\$%s\t\$%s\t\$%s\n",
					$sbproject->id, 
					$end,
					$invoice->{'analysis_costs'}{'amount'},
					$invoice->{'storage_costs'}{'amount'},
					$invoice->{'total'}{'amount'};
			}
			else {
				printf STDERR " cannot get last invoice for %s\n", $sbproject->id;
			}
		}
		else {
			printf STDERR " no invoices for %s\n", $sbproject->id;
		}
	}
	else {
		die "you've got a bug! No function defined\n";
	}
}




