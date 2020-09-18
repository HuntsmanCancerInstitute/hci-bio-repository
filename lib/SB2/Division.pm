package SB2::Division;


=head1 SB2::Division

Class object representing a Division on the Seven Bridges platform.

See SB2 documentation for details.

=cut


use strict;
use Carp;
use IO::File;
use File::Spec;
use base 'SB2';
use SB2::Project;
use SB2::Member;
use SB2::Team;

1;

sub new {
	my $class = shift;
	if (ref($class)) {
		$class = ref($class);
	}
	
	my %args = @_;
	my $self = {
		div   => $args{div} || undef,
		name  => $args{name} || undef,
		href  => $args{href} || undef,
		cred  => $args{cred} || undef,
		token => $args{token} || undef,
		verb  => $args{verbose},
		end   => $args{end},
	};
	
	return bless $self, $class;
}

sub id {
	return shift->{div};
}

sub name {
	my $self = shift;
	if (not defined $self->{name}) {
		my $url = sprintf "%s/divisions/%s", $self->endpoint, $self->id;
		my $options = {
			'cache-control' => 'no-cache',
			'x-sbg-advance-access' => 'advance',
		};
		my $result = $self->execute('GET', $url, $options);
		$self->{name} = $result->{name};
	}
	return $self->{name};
}

sub href {
	my $self = shift;
	unless (defined $self->{href}) {
		# make it up
		$self->{href} = sprintf "%s/divisions/%s", $self->endpoint, $self->id;
	}
	return $self->{href};
}


*projects = \&list_projects;

sub list_projects {
	my $self = shift;
	if (not exists $self->{projects}) {
		my @results = $self->execute('GET', sprintf("%s/projects", $self->endpoint));
		my @projects = map { SB2::Project->new($self, $_) } @results;
		$self->{projects} = \@projects;
	}
	return wantarray ? @{ $self->{projects} } : $self->{projects};
}

sub create_project {
	my $self = shift;
	my %options = @_;
	unless (exists $options{name}) {
		carp "A new project requires a name!";
		return;
	}
	$options{billing_group} = $self->billing_group; # this may need to be requested
	
	# execute
	my $result = $self->execute('POST', sprintf("%s/projects", $self->endpoint), 
		undef, \%options);
	return $result ? SB2::Project->new($self, $result) : undef;
}

sub get_project {
	my $self = shift;
	my $project = shift;
	unless ($project) {
		carp "project short name must be provided!";
		return;
	}
	
	# execute
	my $url = sprintf "%s/projects/%s/%s", $self->endpoint, $self->id, $project;
	my $result = $self->execute('GET', $url);
	return $result ? SB2::Project->new($self, $result) : undef;
}

sub list_members {
	my $self = shift;
	if (not exists $self->{members}) {
		my $url = sprintf "%s/users?division=%s", $self->endpoint, $self->id;
		my @results = $self->execute('GET', $url);
			
		my @members = map { SB2::Member->new($self, $_) } @results;
		$self->{members} = \@members;
	}
	return wantarray ? @{ $self->{members} } : $self->{members};
}

sub billing_group {
	my $self = shift;
	if (not exists $self->{billing}) {
		my @results = $self->execute('GET', sprintf("%s/billing/groups", $self->endpoint));
		if (scalar @results > 1) {
			printf "More than one billing group associated with division! Using first one\n";
		}
		$self->{billing} = $results[0]->{id};
	}
	return $self->{billing};
}

sub list_teams {
	my $self = shift;
	my $h = {'x-sbg-advance-access' => 'advance'};
	my $url = sprintf "%s/teams?division=%s&_all=true", $self->endpoint, $self->division;
	my @results = $self->execute('GET', $url, $h);
	my @teams = map { SB2::Team->new($self, $_) } @results;
	return wantarray ? @teams : \@teams;
}

sub create_team {
	my $self = shift;
	my $name = shift || undef;
	unless ($name) {
		carp "A new team requires a name!";
		return;
	}
	my $data = {
		name     => $name,
		division => $self->division,
	};
	
	# execute
	my $h = {'x-sbg-advance-access' => 'advance'};
	my $result = $self->execute('POST', sprintf("%s/teams", $self->endpoint), $h, $data);
	return $result ? SB2::Team->new($self, $result) : undef;
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



