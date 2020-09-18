package SB2::Member;


=head1 SB2::Member

Class object representing a Member on the Seven Bridges platform.

See SB2 documentation for details.

=cut


use strict;
use Carp;
use base 'SB2';



1;

sub new {
	my ($class, $parent, $result) = @_;
	
	# create object based on the given result
	# this is tricky, because the results will vary with different keys depending 
	# on whether this is was called from a division or a project - sigh
	unless (defined $result and ref($result) eq 'HASH') {
		confess "Must call new() with a parsed JSON member result HASH!";
	}
	my $self = $result;
	
	# add items from parent
	$self->{div}   = $parent->division;
	$self->{token} = $parent->token;
	$self->{verb}  = $parent->verbose;
	$self->{end}   = $parent->endpoint;
	
	# clean up some stuff due to inconsistencies in the API and the source of the result
	if (exists $self->{username} and $self->{username} =~ /^([a-z0-9\-]+)\/([\w\-\.]+)$/) {
		my $div = $1;
		my $name = $2;
		$self->{id} = $self->{username}; # id is division/shortname
		$self->{username} = $name;       # username is shortname
	}
	
	return bless $self, $class;
}

sub id {
	my $self = shift;
	if (exists $self->{id} and defined $self->{id}) {
		return $self->{id};
	}
	elsif (exists $self->{username} and defined $self->{username}) {
		# generate what should be the ID
		return sprintf("%s/%s", $self->division, $self->name);
	}
	else {
		return undef;
	}
}

sub name {
	my $self = shift;
	if (exists $self->{first_name} and exists $self->{last_name}) {
		return sprintf("%s %s", $self->{first_name}, $self->{last_name});
	}
	else {
		return $self->username;
	}
}

sub username {
	# I think this should always be present
	my $self = shift;
	if (exists $self->{username} and defined $self->{username}) {
		return $self->{username};
	}
	elsif (exists $self->{id} and $self->{username} =~ /^[a-z0-9\-]+\/([\w\-]+)$/) {
		# extract it from the ID
		return $1;
	}
	else {
		return undef;
	}
}

sub email {
	my $self = shift;
	return $self->{email} || undef; # this should always be present
}

sub first_name {
	my $self = shift;
	return $self->{first_name} || undef;
}

sub last_name {
	my $self = shift;
	return $self->{last_name} || undef;
}

sub type {
	# project attribute
	my $self = shift;
	return $self->{type} || undef;
}

sub role {
	# division attribute
	my $self = shift;
	return $self->{role} || undef;
}

sub copy {
	# project attribute
	my $self = shift;
	return $self->{permissions}{copy} || undef;
}

sub write {
	# project attribute
	my $self = shift;
	return $self->{permissions}{write} || undef;
}

sub read {
	# project attribute
	my $self = shift;
	return $self->{permissions}{read} || undef;
}

sub exec {
	# project attribute
	my $self = shift;
	return $self->{permissions}{exec} || undef;
}

sub href {
	# attribute of both, but have different URLs
	my $self = shift;
	return $self->{href};
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


