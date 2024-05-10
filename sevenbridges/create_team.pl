#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use FindBin qw($Bin);
use lib $Bin;
use Net::SB;


my $VERSION = 5.1;


######## Documentation
my $doc = <<END;
A script to create a lab team for each division

END

my $division;
my $team_name = 'lab';
my $cred;

if (scalar(@ARGV) > 1) {
	GetOptions(
		'div=s'             => \$division,
		'team=s'            => \$team_name,
		'cred=s'            => \$cred,
	) or die "please recheck your options!\n\n";
}
else {
	print $doc;
	exit;
}
my @members = @ARGV;

my $sb = Net::SB->new(
	division    => $division,
	credentials => $cred,
) or die "unable to initialize!";

unless ($sb->token) {
	die "no token available for division '$division'! Check credentials file!\n";
}

# verify members
unless (scalar @members) {
	foreach my $member ($sb->list_members) {
		if ($member->username =~ /kclemens|tjparnell/) {
			printf " > rejecting super admin %s\n", $member->username;
			next;
		}
		if ($member->role ne 'MEMBER') {
			printf " > skipping %s %s <%s>\n", $member->role, $member->username, $member->email;
			next;
		}
		printf " + adding %s %s <%s>\n", $member->role, $member->username, $member->email;
		push @members, $member;
	}
}


# check with operator
printf "Ready to make team $team_name with %d members? y/n  ", scalar(@members);

my $response = <STDIN>;
if ($response =~ /n/i) {
	exit;
}


# create team
my $team = $sb->create_team($team_name);
if ($team) {
	printf " Created team %s\n", $team->id;
}
else {
	die "unable to create team!\n";
}

# add users
foreach my $memb (@members) {
	my $result = $team->add_member($memb);
	printf " added %s\n", $memb->username;
}


