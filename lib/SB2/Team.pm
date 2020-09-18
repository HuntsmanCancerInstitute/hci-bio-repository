package SB2::Team;

=head1 SB2::Team

Class object representing a Team on the Seven Bridges platform.

See SB2 documentation for details.

=cut


use strict;
use Carp;
use base 'SB2';
use SB2::Member;

1;

sub new {
	my ($class, $parent, $result, $name) = @_;
	
	# create object based on the given result
	unless (defined $result and ref($result) eq 'HASH') {
		confess "Must call new() with a parsed JSON team result HASH!";
	}
	my $self = $result;
	
	# add items from parent
	$self->{name}  = $name;
	$self->{div}   = $parent->division;
	$self->{token} = $parent->token;
	$self->{verb}  = $parent->verbose;
	$self->{end}   = $parent->endpoint;
	
	return bless $self, $class;
}

sub id {
	return shift->{id} || undef;
}

sub name {
	# I think this should always be present
	return shift->{name};
}

sub href {
	return shift->{href} || undef;
}

sub list_members {
	my $self = shift;
	
	# execute
	my $h = {'x-sbg-advance-access' => 'advance'};
	my $url = sprintf "%s/teams/%s/members", $self->endpoint, $self->{id};
	my $results = $self->execute('GET', $url, $h);
	my @members = map { SB2::Member->new($self, $_) } @$results;
	return wantarray ? @members : \@members;
}

sub add_member {
	my $self = shift;
	my $member = shift;
	unless ($member) {
		carp "Must pass a member object or ID to add a member!";
		return;
	}
	
	# get member id
	my $id;
	if (ref($member) eq 'SB2::Member') {
		$id = $member->id;
	}
	else {
		$id = $member;
	}
	if ($id !~ /^[a-z0-9\-]+\/[a-z0-9\-]+$/) {
		carp "ID '$id' doesn't match expected pattern of lab-division/user-name";
		return;
	}
	
	# execute
	my $data = { 'id' => $id };
	my $url = $self->href . '/members';
	my $result = $self->execute('POST', $url, undef, $data);
	return $result;
}

sub delete_member {
	my $self = shift;
	my $member = shift;
	unless ($member) {
		carp "Must pass a member object or ID to add a member!";
		return;
	}
	
	# get member id
	my $id;
	if (ref($member) eq 'SB2::Member') {
		$id = $member->id;
	}
	else {
		$id = $member;
	}
	if ($id !~ /^[a-z0-9\-]+\/[a-z0-9\-]+$/) {
		carp "ID '$id' doesn't match expected pattern of lab-division/user-name";
		return;
	}
	
	# execute
	my $url = sprintf "%s/members/%s", $self->href, $id;
	return $self->execute('DELETE', $url); # this may not necessarily be true
}

__END__

=head1 AUTHOR

 Timothy J. Parnell, PhD
 Bioinformatics Shared Resource
 Huntsman Cancer Institute
 University of Utah
 Salt Lake City, UT, 84112

This package is free software; you can redistribute it and/or modify
it under the terms of the Artistic License 2.0.  


