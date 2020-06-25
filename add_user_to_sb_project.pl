#!/usr/bin/env perl

use strict;
use warnings;
use Getopt::Long;
use FindBin qw($Bin);
use lib $Bin;
use RepoCatalog;
use SB2;

my $VERSION = 5;


######## Documentation
my $doc = <<END;

A script to add a user to a SB project. By default, the owner 
(user) of the GNomEx project will be added with full rights (read,
copy, write, execute, and admin). 

More than one project may be specified to updated.

If a catalog file is not provided, then the division and email of 
the user to be added must be provided. 

User will be added with permissions:
	read    true
	copy    true
	write   true
	execute true
	admin   true
If the user already exists, then all but "admin" permissions will 
be updated to "true";


USAGE:
add_user_sb_project.pl --cat <file.db> <options> <project1 project2 ...>

add_user_sb_project.pl --id 1234R --division big-shot-pi --email grad.student\@utah.edu

OPTIONs:
    --cat <path>            Provide the path to a catalog file
    --division <text>       Provide the SB division
    --id <text>             Project identifier: 1234R or A5678
    --email <text>          Email of user to check and/or add
    --perm <text>           Comma-delimited list of true permissions:
                              read,copy,write,execute,admin
    --check                 Check only, do not add
    --alt                   Print list of other division members for 
                              visual crosscheck
    --cred <file>           Path to credentials file 
                              default ~/.sevenbridges/credentials
    --verbose               print processing commands
END
 




####### Input
my $cat_file;
my @projects;
my $division;
my $email;
my $perms;
my $print_alts;
my $credentials;
my $check_only  = 0;
my $verbose     = 0;

if (scalar(@ARGV) > 1) {
	GetOptions(
		'cat=s'             => \$cat_file,
		'division=s'        => \$division,
		'id=s'              => \@projects,
		'check!'            => \$check_only,
		'email=s'           => \$email,
		'perm=s'            => \$perms,
		'alt!'              => \$print_alts,
		'cred=s'            => \$credentials,
		'verbose!'          => \$verbose,
	) or die "please recheck your options!\n\n";
}
else {
	print $doc;
	exit;
}

if (not $cat_file) {
	unless ($division and $email) {
		die "A catalog file and/or division and email must be provided!\n";
	}
}
if (@ARGV) {
	push @projects, @ARGV;
}
unless (@projects) {
	die "One or more GNomEx project IDs (1234R or A5678) must be provided!\n";
}



### Initialize
my $Catalog;
if ($cat_file) {
	$Catalog = RepoCatalog->new($cat_file) or 
		die "Cannot open catalog file '$cat_file'!\n";
}



### Loop through given projects
foreach my $id (@projects) {
	next unless $id;
	print " Processing project $id\n";
	
	# Get division and project information
	my $curr_division = $division || undef;
	my $user_email = $email || undef;
	my ($first, $last);
	
	if (not $curr_division or not $user_email) {
		# get catalog entry
		my $Entry = $Catalog->entry($id);
		unless ($Entry) {
			print "  unable to locate project $id in Catalog! Skipping\n";
			next;
		}
		$curr_division = $Entry->division;
		unless ($curr_division) {
			print "  $id does not have a valid division!? Skipping\n";
			next;
		}
		$user_email = $Entry->user_email;
		unless ($user_email) {
			print "  Catalog entry does not have a user email!?\n Skipping\n";
			next;
		}
		$first = $Entry->user_first || undef;
		$last = $Entry->user_last || undef;
	}
	
	# get SB project
	my $Division = SB2->new(
		division   => $curr_division,
		verbose    => $verbose,
		cred       => $credentials,
	);
	unless ($Division) {
		printf "  unable to initialize SB division %s!\n", $curr_division;
		next;
	}
	my $Project = $Division->get_project( lc($id) );
	unless ($Project) {
		printf "  unable to locate SB division project %s!?\n", lc($id);
		next;
	}
	
	# find GNomEx project owner amongst division members
	my $divMember;
	my @possibles;
	foreach my $member ($Division->list_members) {
		if (lc($member->email) eq lc($user_email)) {
			# email matches, that was easy!
			$divMember = $member;
			last;
		}
		elsif (
			$last and 
			$first and 
			lc($member->last_name) eq lc($last) and 
			lc($member->first_name) eq lc($first)
		) {
			# first and last names match
			$divMember = $member;
			last;
		}
		else {
			if ($member->username !~ /kclemens|tjparnell/) {
				push @possibles, $member;
			}
		}
	}
	if ($divMember) {
		printf "  found user as %s\n", $divMember->id;
	}
	else {
		printf "  unable to match user %s %s <%s> out of %d candidates\n", 
			$first, $last, $user_email, scalar(@possibles);
		if ($print_alts) {
			printf "%s\n", join("\n", 
				map {sprintf("    %s %s <%s>", $_->first_name, $_->last_name, $_->email)} 
				@possibles);
		}
		next;
	}
	
	# check project members
	my $pMember;
	foreach my $member ($Project->list_members) {
		if ($member->username eq $divMember->username) {
			# user is already a member of this project!
			$pMember = $member;
			printf "  user %s already a member of %s\n", $divMember->username, 
				$Project->id;
			last;
		}
	}
	
	next if $check_only;
	
	# permissions
	my @permissions;
	if ($perms) {
		foreach (split ',', $perms) {
			push @permissions, $_, 'true';
		}
	}
	else {
		@permissions = (
			'read'      => 'true',
			'copy'      => 'true',
			'write'     => 'true',
			'execute'   => 'true',
			'admin'     => 'true'
		);
	}
	
	# update
	print "  updating...\n";
	if ($pMember) {
		my $result = $Project->modify_member_permission($pMember, @permissions);
		if ($result and ref($result) eq 'HASH') {
			printf "  updated permissions to %s\n", join(", ", 
				map {sprintf("$_ => %s", $result->{$_})} 
				sort {$a cmp $b}
				keys %$result);
		}
		else {
			printf "  updating permission resulted in %s\n", $result;
		}
	}
	else {
		$pMember = $Project->add_member($divMember, @permissions);
		if ($pMember) {
			printf "  added user to project as %s\n", $pMember->{username};
		}
	}
}





