package SB2;
our $VERSION = 5;

=head1 NAME

SB2 - a Perl wrapper around the Seven Bridges API

=head1 DESCRIPTION

This is a simplified Perl wrapper around the Seven Bridges v2 
L<API|https://docs.sevenbridges.com/reference>.
 
This is not a comprehensive implementation of the full API in its 
entirety. There are limitations, and primarily includes only the 
functionality that is most relevant to the scripts in this package.

This requires a Developer token to access your Seven Bridges account. 
A Developer token must be generated through the Seven Bridges website.
One or more division tokens may be stored in a credentials file, 
a C<INI>-style text file in your home directory, the default being
F<~/.sevenbridges/credentials>. See Seven Bridges 
L<credentials documentation|https://docs.sevenbridges.com/docs/store-credentials-to-access-seven-bridges-client-applications-and-libraries>.

Many of the base URL paths are hard coded. Changes in the Seven Bridges 
API will likely break one or more methods here.

=head1 METHODS

=head2 SB2 Class

Main Class. This is inherited by all subclasses, so these functions 
should be available everywhere. Start here.

=over 4

=item new

Provide parameters as array:

    division => $division, 
    cred     => $credential_path, # default ~/.sevenbridges/credentials
    token    => $token, # if known or to overide credential file
    verbose  => 1, # default is 0
    end      => $endpoint, # API endpoint, default overriden by credential

If a division is given, then a L<SB2::Division> object is immediately 
returned. Otherwise, a generic object is returned. Not much can be 
done with a generic object, other than L<list_divisions>.

=item credentials

Return the credentials file path

=item division

Returns the division name. Required to do any work.

=item token

Returns the token for the given division, obtained automatically from 
the credentials file if not explicitly given. 

=item endpoint

Returns and sets the API endpoint. The default value is 
C<https://api.sbgenomics.com/v2/>, but this value will be 
overridden by the C<api_endpoint> tag in the credentials file.

=item verbose($verbosity)

Sets and returns verbosity boolean value: 1 is true, 0 is false.

=item execute($method, $url, \%header_options, \%data)

Main function for executing an API C<https> request. Most users shouldn't 
need to run this, unless a method doesn't exist. Pass at a minimum the 
HTTP method type (C<GET>, C<POST>, C<DELETE>) and the complete URL path.

Two additional positional items may be provided as necessary. For simple
queries, neither are required. First, a hash reference of additional header
items. The division token is automatically supplied here and isn't necessary.
Second, a hash reference of additional data content to be provided for 
C<POST> submissions. 

All results are returned as JSON and parsed into Perl objects. When expecting 
lists of results, e.g. member or project lists, these are parsed and placed 
into an array and returned as an array or array reference. Non-list results 
are parsed and returned as a hash. Be careful on what you expect. 

=item list_divisions

Primarily for super admins. Lists all divisions for which you are a member. 
Returns a list or array reference of L<SB2::Division> objects corresponding 
to each division.


=back

=head2 SB2::Division

Class object representing a lab division on the Seven Bridges platform. 
This is generally not created independently, but inherited from a L<SB2> 
object.

The following methods are available.

=over 4

=item id

The identifier, or short name, for this division.

=item href

Returns the URL for this division.

=item name

The name of the division. May be different text from the short name identifier.

=item list_projects

Return list of available projects as L<SB2::Project> objects within current division.
Restricted to those that the current user can see. 

=item create_project

Make new project. Returns L<SB2::Project> object.
Pass array of information.

    name        => $name,
    description => $description, # can be Markdown

=item get_project

Given a short name identifier, return a L<SB2::Project> object for the project.

=item list_members

Returns an array or array reference with a list of L<SB2::Members> objects 
for each member in the division.

=item billing_group

Returns the C<ID> of the C<billing_group>. 

=item list_teams

Returns a list of all the teams in the division, not necessarily those to which 
the user is a member. Returns L<SB2::Team> objects. 

=item create_team

Pass the name of a new team to create. A L<SB2::Team> object will be returned
to which members will need to be added.

=back




=head2 SB2::Project Class

Class object representing a Project in Seven Bridges. This is generally not 
created independently, but inherited from a L<SB2::Division> object.

The following methods are available.

=over 4

=item id

The identifier, or short name, of the project.

=item project

Same as L<id>.

=item href

Returns the URL for this division.

=item name

The name of the project, which may be different text from the short identifier. 

=item description

Gets the description of the project. Can set by passing text to L<update>.

=item details

Return a hash reference to additional details about a project.

=item update

Pass array of information to update the project.
Returns 1 if successful. Object metadata is updated.

    name        => $new_name,
    description => $text, # can be Markdown

=item list_members

Returns list of L<SB2::Member> objects who are members of the current project.

=item add_member

Pass the member identifier, member's email address on the platform, 
an L<SB2::Member> object, or an L<SB2::Team> object to 
add to the project. Optionally, pass additional key =E<gt> value pairs to 
set permissions. Default permissions are C<read> and C<copy> are C<TRUE>, 
and C<write>, C<execute>, and C<admin> are C<FALSE>.

=item modify_member_permission

Pass the L<SB2::Member> (or L<SB2::Team>) object and an array of key =E<gt> value 
pairs to change the member's permissions for this project. Possible keys 
include C<read>, C<copy>, C<write>, C<execute>, and C<admin>. Possible values 
include C<TRUE> and C<FALSE>.

=item bulk_upload_path($path)

Sets or returns the path to F<sbg-uploader.sh>, the shell script used to start 
the Java executable. It will automatically be searched for in the environment 
F<PATH> if not provided.

=item bulk_upload(@options)

Automatically handles setting the division, project, and token. Executes the 
F<sbg-upload.sh> script and returns the standard out text results.

=back

=head2 SB2::Member Class

Class object representing a member in Seven Bridges. This is generally not 
created independently, but inherited from either an L<SB2::Division> or 
L<SB2::Project> object. Depending upon the origin, the exact attributes may 
vary, although some methods are munged to provide some level of consistency, 
for example id and username.

=over 4

=item id

This should return C<my-division/rfranklin>, despite the member origin.

=item name

Returns the given name of the member. If the first and last names are 
indicated in the metadata, then "First Last" is returned. Otherwise, the 
username is returned.

=item username

This should return C<rfranklin>, despite the member origin. Some sources 
include division, and some don't, as the user name.

=item email

This should be included from both Division and Project origins.

=item href

Returns the URL for the member.

=back


The following attributes should be available from Division-derived members.

=over 4

=item username

=item role

Returns C<MEMBER>, C<ADMIN>, etc. 

=item first_name

=item last_name

=item email

=back

These attributes should be available from Project-derived members.

=over 4

=item type

Returns C<USER>, C<ADMIN>, etc.

=item read

Read permission for the Project.

=item copy

Copy permission for the Project.

=item write

Write permission for the Project.

=item exec

Job execution permission for the Project.

=back

=head2 SB2::Team Class

Class object representing a Team in Seven Bridges. This is generally not 
created independently, but inherited from either an L<SB2::Division> or 
L<SB2::Project> object. 

=over 4

=item id

The identifier, or short name, of the Team.

=item name

The name of the Team. 

=item href

Returns the URL for this division.

=item list_members

Returns a list of L<SB2::Member> objects for all the members on the Team.

=item add_member

Provide a member ID or L<SB2::Member> object to add to the Team. No 
permissions are required.

=item delete_member

Provide the member ID or L<SB2::Member> object to remove from the Team.
The return value may not necessarily be true. 

=back

=cut

use strict;
use Carp;
use HTTP::Tiny;
use JSON::PP;	# this is reliably installed, XS is not
				# XS would be better performance, but we're not doing anything complicated
use SB2::Division;



# Initialize 
# BEGIN {
# 	# this is only for more recent versions, so disabled
# 	# it will have to fail later if SSL stuff isn't available
# 	unless (HTTP::Tiny->can_ssl) {
# 		die "No SSL support installed. Please install Net::SSLeay and IO::Socket::SSL";
# 	}
# }
my $http = HTTP::Tiny->new();

1;



sub new {
	my $class = shift;
	if (ref($class)) {
		$class = ref($class);
	}
	
	# arguments
	my %args = @_;
	my $division = $args{div} || $args{division} || $args{profile} || undef;
	my $cred_path = $args{cred} || $args{cred_path} || $args{credentials} || undef;
	my $token = $args{token};
	my $verb = $args{verbose} || $args{verb} || 0;
	my $endpoint = $args{endpoint} || 'https://api.sbgenomics.com/v2'; # default
	
	# check for credentials file
	if (defined $cred_path) {
		unless (-e $cred_path and -r _ ) {
			croak "bad credential file path! File unreadable or doesn't exist";
		}
	}
	else {
		my $f = File::Spec->catfile($ENV{HOME}, '.sevenbridges', 'credentials');
		if (-e $f and -r _ ) {
			$cred_path = $f;
		}
		else {
			croak "no credentials file available!";
		}
	}
		
	# conditional return
	if (defined $division) {
		return SB2::Division->new(
			div     => $division,
			cred    => $cred_path,
			token   => $token,
			name    => $args{name}, # just in case?
			verbose => $verb,
			end     => $endpoint,
		);
	}
	else {
		my $self = {
			div     => 'default',
			cred    => $cred_path,
			verb    => $verb,
			end     => $endpoint,
		};
		return bless $self, $class;
	}
}

sub credentials {
	return shift->{cred};
}

sub division {
	# convenience generic method
	return shift->{div} || undef;
}

sub endpoint {
	my $self = shift;
	if (@_) {
		my $url = $_[0];
		unless ($url =~ /^https:\/\//) {
			croak "given API endpoint '$url' does not look like a URL!";
		}
		$url =~ s/\/$//; # remove trailing slash
		$self->{end} = $url;
	}
	return $self->{end};
}

sub verbose {
	my $self = shift;
	if (@_) {
		$self->{verb} = $_[0];
	}
	return $self->{verb};
}

sub token {
	my $self = shift;
	
	# check token
	unless (defined $self->{token}) {
		# need to collect the token from a credentials file
		my $cred_path = $self->{cred};
		
		# division
		my $division = $self->division;
		unless ($division) {
			confess "no division is specified!?";
		}
		
		# pull token
		my $token;
		my $fh = IO::File->new($cred_path) or 
			die "unable to read credentials files!\n";
		my $target = sprintf("[%s]", $division);
		while (my $line = $fh->getline) {
			chomp $line;
			if ($line eq $target) {
				# we found the section!
				while (my $line2 = $fh->getline) {
					if ($line2 =~ m/^\[/) {
						# we've gone too far!!!??? start of next section
						last;
					}
					elsif ($line2 =~ /^api_endpoint\s*=\s*([\w\/\/:]+)$/) {
						# we found the user's API endpoint
						# go ahead and store it
						$self->endpoint($1);
					}
					elsif ($line2 =~ /^auth_token\s*=\s*(\w+)$/) {
						# we found the user's token!
						$token = $1;
					}
				}
			}
		}
		$fh->close;
		unless ($token) {
			# carp " unable to get token from credentials file for division $division!\n";
			return;
		}
		$self->{token} = $token;
	}
	
	return $self->{token};
}

sub execute {
	my ($self, $method, $url, $headers, $data) = @_;
	
	# check method
	unless ($method eq 'GET' or $method eq 'POST' or $method eq 'DELETE') {
		confess "unrecognized method $method! Must be GET|POST|DELETE";
	}
	
	# check URL
	unless (defined $url) {
		confess "a URL is required!";
	}
	
	# check token
	my $token = $self->token;
	unless ($token) {
		my $division = $self->division;
		carp " unable to get token from credentials file for division $division!\n";
		return;
	}
	
	# add standard key values to headers
	if (defined $headers and ref($headers) ne 'HASH') {
		confess "provided header options must be a HASH reference!";
	}
	else {
		$headers ||= {};
	}
	$headers->{'Content-Type'} = 'application/json';
	$headers->{'X-SBG-Auth-Token'} = $token;
	
	# http tiny request options
	my $options = {headers => $headers};
	
	# any post content options
	if (defined $data) {
		unless (ref($data) eq 'HASH') {
			confess "provided POST data must be a HASH reference!";
		}
		$options->{content} = encode_json($data);
	}
	
	# send request
	if ($self->verbose) {
		printf " > Executing $method to $url\n";
		if ($data) {printf "   data: %s\n", $options->{content}}
	}
	my $response = $http->request($method, $url, $options) or 
		confess "can't send http request!";
	if ($self->verbose) {
		printf" > Received %s %s\n > Contents: %s\n", $response->{status}, 
			$response->{reason}, $response->{content};
	}
	
	# check response
	my $result;
	if ($response->{success}) {
		# success is a 2xx http status code, decode results
		$result = decode_json($response->{content});
	}
	elsif ($method eq 'GET' and $response->{status} eq '404') {
		# we can interpret this as a possible acceptable negative answer
		return;
	}
	elsif ($method eq 'DELETE') {
		# not sure what the status code for delete is, but it might be ok
		printf "> DELETE request returned %s: %s\n", $response->{status}, 
			$response->{reason};
		return 1;
	}
	elsif (exists $response->{reason} and $response->{reason} eq 'Internal Exception') {
		confess "http request suffered an internal exception: $response->{content}";
	}
	else {
		confess sprintf("A %s error occured: %s: %s", $response->{status}, 
			$response->{reason}, $response->{content});
		return;
	}
	
	# check for items
	if (exists $result->{items}) {
		my @items = @{ $result->{items} };
		
		# check for more items
		if (exists $result->{links} and scalar @{$result->{links}} > 0) {
			# we likely have additional items, and the next link is conveniently provided
			# get the next link
			my $next;
			foreach my $l (@{$result->{links}}) {
				if (exists $l->{rel} and $l->{rel} eq 'next') {
					$next = $l;
					last;
				}
			}
			# keep going until we get them all
			while ($next) {
				if ($self->verbose) {
					printf " > Executing next %s request to %s\n", $next->{method}, $next->{href};
				}
				my $res = $http->request($next->{method}, $next->{href}, $options);
				if ($res->{reason} eq 'OK') {
					my $result2 = decode_json($res->{content});
					if ($self->verbose) {
						printf "  > Collected %d additional items\n", scalar(@{$result2->{items}});
					}
					push @items, @{ $result2->{items} };
					undef($next);
					foreach my $l (@{$result2->{links}}) {
						if (exists $l->{rel} and $l->{rel} eq 'next') {
							$next = $l;
							last;
						}
					}
				}
				else {
					croak sprintf("Failure to get next items with URL %s\n A %s error occurred: %s: %s",
						$next->{href}, $res->{status}, $res->{reason}, $res->{content});
				}
			}
		}
		
		# make sure we don't have duplicates
		# at least the list_divisions method returns a ton of duplicates - a bug?
		# insert sanity check here as a general method, just in case the bug afflicts
		# other things too
		my %seenit;
		my @keep;
		foreach my $i (@items) {
			next if exists $seenit{ $i->{href} };
			$seenit{ $i->{href} } = 1;
			push @keep, $i;
		}
		
		# done
		return wantarray ? @keep : \@keep;
	}
	
	# appears to be a single result, not a list
	return $result;
}

sub list_divisions {
	my $self = shift;
	return unless ref($self) eq 'SB2'; # this should not be accessible by inherited objects
	my $options = {
		'x-sbg-advance-access' => 'advance'
	};
	my $token = $self->token; 
		# we don't actually need the token yet, but it will force reading credentials file
		# and update the api endpoint in case it's different from the default value
	my @items = $self->execute('GET', sprintf("%s/divisions", $self->endpoint), 
		$options);
	my $cred = $self->credentials;
	my $verb = $self->verbose;
	my $end  = $self->endpoint;
	my @divisions = map {
		SB2::Division->new(
			div     => $_->{id},
			name    => $_->{name},
			href    => $_->{href},
			cred    => $cred,
			verbose => $verb,
			end     => $end,
		);
	} @items;
	
	return wantarray ? @divisions : \@divisions;
}



__END__

=head1 AUTHOR

 Timothy J. Parnell, PhD
 Bioinformatic Analysis Shared Resource
 Huntsman Cancer Institute
 University of Utah
 Salt Lake City, UT, 84112

This package is free software; you can redistribute it and/or modify
it under the terms of the Artistic License 2.0.  


