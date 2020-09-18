package SB2::Project;


=head1 SB2::Project

Class object representing a Project on the Seven Bridges platform.

See SB2 documentation for details.

=cut


use strict;
use Carp;
use base 'SB2';
use SB2::Member;

1;

sub new {
	my ($class, $parent, $result) = @_;
	
	# create object based on the given result
	unless (defined $result and ref($result) eq 'HASH') {
		confess "Must call new() with a parsed JSON project result HASH!"
	}
	my $self = $result;
	
	# add items from parent
	$self->{div}   = $parent->division;
	$self->{token} = $parent->token;
	$self->{verb}  = $parent->verbose;
	$self->{end}   = $parent->endpoint;
	
	return bless $self, $class;
}

sub id {
	return shift->{id};
}

sub project {
	return shift->{id};
}

sub href {
	my $self = shift;
	return $self->{href};
}

sub name {
	my $self = shift;
	return $self->{name};
}

sub details {
	my $self = shift;
	return $self->{details};
}

sub description {
	my $self = shift;
	return $self->{description};
}

sub update {
	my $self = shift;
	my %data = @_;
	unless (%data) {
		carp "no data to update!?";
		return;
	}
	
	# set URL, using simple POST since I don't think client supports PATCH
	my $url = $self->href . '?_method=PATCH';
	
	# execute
	my $result = $self->execute('POST', $url, undef, \%data); 
	
	# blindly replace all the update key values
	foreach my $key (keys %$result) {
		$self->{$key} = $result->{$key};
	}
	
	return 1;
}

sub list_members {
	my $self = shift;
	unless ($self->{members}) {
		my $url = $self->{href} . '/members';
		my @results = $self->execute('GET', $url);
		my @members = map { SB2::Member->new($self, $_) } @results; 
		$self->{members} = \@members;
	}
	return wantarray ? @{ $self->{members} } : $self->{members};
}

sub add_member {
	my $self = shift;
	my $member = shift;
	my %permissions = @_;
	unless ($member) {
		carp "Must pass a member object or username to add a member!";
		return;
	}
	
	# set default permissions
	$permissions{'read'} ||= 'true';
	$permissions{'copy'} ||= 'true';
	
	# data
	my $data = {
		permissions => \%permissions,
	};
	
	# get member username
	if (ref($member) eq 'SB2::Member') {
		$data->{username} = $member->id; # must be longform username division/username
		printf(" >> adding member id %s\n", $data->{name}) if $self->verbose;
	}
	elsif (ref($member) eq 'SB2::Team') {
		$data->{username} = $member->id;
		$data->{type} = 'TEAM';
		printf(" >> adding team id %s\n", $data->{name}) if $self->verbose;
	}
	elsif ($member =~ /^[a-z0-9\-]+\/[\w\-\.]+$/) {
		# looks like a typical id
		$data->{username} = $member;
		printf(" >> adding given member id %s\n", $data->{name}) if $self->verbose;
	}
	elsif ($member =~ /^[\w\.\-]+@[\w\.\-]+\.(?:com|edu|org)$/) {
		# looks like an email address
		$data->{email} = $member;
		printf(" >> adding given member email %s\n", $data->{name}) if $self->verbose;
	}
	else {
		carp "unrecognized member format!";
		return;
	}
	
	
	# execute
	my $url = $self->href . '/members';
	my $result = $self->execute('POST', $url, undef, $data);
	return $result ? SB2::Member->new($self, $result) : undef;
}

sub modify_member_permission {
	my $self = shift;
	my $member = shift;
	my %permissions = @_;
	
	unless ($member) {
		carp "Must pass a member object or username to add a member!";
		return;
	}
	unless (%permissions) {
		carp "Must pass a permissions to change!";
	}
	
	# get member username
	my $username;
	if (ref($member) eq 'SB2::Member') {
		$username = $member->id; # must be longform username division/username
		printf(" >> updating member id %s\n", $username) if $self->verbose;
	}
	elsif (ref($member) eq 'SB2::Team') {
		$username = $member->id;
		printf(" >> updating team id %s\n", $username) if $self->verbose;
	}
	elsif ($member =~ /^[a-z0-9\-]+\/[\w\-\.]+$/) {
		# looks like a typical id
		$username = $member;
		printf(" >> updating given id %s\n", $username) if $self->verbose;
	}
	else {
		carp "unrecognized member format '$member'!";
		return;
	}
	
	# execute
	my $url = $self->href . "/members/$username/permissions?_method=PATCH";
	my $result = $self->execute('POST', $url, undef, \%permissions);
	return $result;
}

sub bulk_upload_path {
	my $self = shift;
	$self->{sbupload_path} ||= undef;
	
	# check for passed path
	if (defined $_[0] and -e $_[0]) {
		# assume it's good
		$self->{sbupload_path} = $_[0];
	}
	
	# look for one if not exists
	if (!defined $self->{sbupload_path}) {
		my $path = qx(which sbg-uploader.sh);
		chomp $path;
		if ($path) {
			$self->{sbupload_path} = $path;
		}
	}
	 
	return $self->{sbupload_path};
}

sub bulk_upload {
	my $self = shift;
	my @options = @_;
	
	# Grab the token
	my $token = $self->token;
	
	# execute
	my $path = $self->bulk_upload_path or return 'sbg-upload.sh path not set!';
	my $command = join(" ", $path, '--project', $self->id, '--token', 
		$token, @options);
	print " > executing: $command\n" if $self->verbose;
	my $result = qx($command);
	return $result;
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


