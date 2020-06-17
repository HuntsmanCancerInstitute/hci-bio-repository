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

If a division is given, then a L<SB2::Division> object is immediately 
returned. Otherwise, a generic object is returned.

=item credentials

Return the credentials file path

=item division

Returns the division name. Required to do any work.

=item token

Returns the token for the given division, obtained automatically from 
the credentials file if not explicitly given. 

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

=back

=head2 SB2::Division

Class object representing a lab division on the Seven Bridges platform.

=over 4

=item id

The identifier, or short name, for this division.

=item href

A URL for this division.

=item name

The name of the division. May be different text from the short name identifier.

=item list_projects

Return list of available projects as SB2::Project objects within current division.
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

Object representing a Project in Seven Bridges.

=over 4

=item id

The identifier, or short name, of the project.

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

Pass the member identifier, email, a L<SB2::Member>, or L<SB2::Team> object to 
add to the project. Optionally, pass additional key,value pairs to 
set permissions. Default permissions are C<read> and C<copy> are true, 
and C<write>, C<execute>, and C<admin> are false.

=item bulk_upload_path($path)

Sets or returns the path to F<sbg-uploader.sh>, the shell script used to start 
the Java executable. It will automatically be searched for in the F<PATH> 
if not provided.

=item bulk_upload(@options)

Automatically handles setting the division, project, and token. Executes the 
F<sbg-upload.sh> script and returns the standard out text results.

=back

=head2 SB2::Member Class

These are all read-only functions. Attributes vary depending on whether the member 
was derived from a L<SB2::Division> or L<SB2::Project> object. Some attributes 
are munged to provide some level of consistency, for example id and username.

=over 4

=item id

This should return C<my-division/rfranklin>, despite the member origin.

=item username

This should return C<rfranklin>, despite the member origin. Some sources 
include division, and some don't, as the user name.

=item email

This should be included from both Division and Project origins.

=back


These attributes should be available from Division-derived members.

=over 4

=item username

=item role

Returns C<MEMBER>, C<ADMIN>, or C<>. 

=item first_name

=item last_name

=item email

=back

These attributes should be available from Project-derived members.

=over 4

=item type

Returns C<USER>, C<ADMIN>, etc.

=item read

=item copy

=item write

=item exec

=back



=cut

use strict;
use Carp;
use HTTP::Tiny;
use JSON::PP;	# this is reliably installed, XS is not
				# XS would be better performance, but we're not doing anything complicated

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
		);
	}
	else {
		my $self = {
			div   => 'default',
			cred  => $cred_path,
			verb  => $verb,
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

sub verbose {
	my $self = shift;
	if (@_) {
		$self->{verb} = $_[0];
	}
	return $self->{verb};
}

sub token {
	my $self = shift;
	my $division = shift || undef; # in case we want to try for default?
	
	# check token
	unless (defined $self->{token}) {
		# need to collect the token from a credentials file
		my $cred_path = $self->{cred};
		
		# pull token
		my $division = $self->division;
		my $token;
		my $fh = IO::File->new($cred_path) or 
			die "unable to read credentials files!\n";
		my $target = sprintf("[%s]", $division);
		while (not defined $token) {
			my $line = $fh->getline or last;
			chomp $line;
			if ($line eq $target) {
				# we found the section!
				while (my $line2 = $fh->getline) {
					if ($line2 =~ m/^\[ /) {
						# we've gone too far!!!??? start of next section
						last;
					}
					elsif ($line2 =~ /^auth_token\s*=\s*(\w+)$/) {
						# we found it!
						$token = $1;
						last;
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
	
		# done
		return wantarray ? @items : \@items;
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
	my @items = $self->execute('GET', 'https://api.sbgenomics.com/v2/divisions', 
		$options);
	my $cred = $self->credentials;
	my $verb = $self->verbose;
	my @divisions = map {
		SB2::Division->new(
			div     => $_->{id},
			name    => $_->{name},
			href    => $_->{href},
			cred    => $cred,
			verbose => $verb,
		);
	} @items;
	
	return wantarray ? @divisions : \@divisions;
}



####################################################################################
package SB2::Division;
use strict;
use Carp;
use IO::File;
use File::Spec;
use base 'SB2';

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
		verb  => $args{verbose}
	};
	
	return bless $self, $class;
}

sub id {
	return shift->{div};
}

sub name {
	my $self = shift;
	if (not defined $self->{name}) {
		my $url = sprintf "https://api.sbgenomics.com/v2/divisions/%s", $self->id;
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
		$self->{href} = 'https://api.sbgenomics.com/v2/divisions/' . $self->id;
	}
	return $self->{href};
}


*projects = \&list_projects;

sub list_projects {
	my $self = shift;
	if (not exists $self->{projects}) {
		my @results = $self->execute('GET', 'https://api.sbgenomics.com/v2/projects');
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
	my $result = $self->execute('POST', 'https://api.sbgenomics.com/v2/projects', 
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
	my $url = sprintf "https://api.sbgenomics.com/v2/projects/%s/%s", $self->id, $project;
	my $result = $self->execute('GET', $url);
	return $result ? SB2::Project->new($self, $result) : undef;
}

sub list_members {
	my $self = shift;
	if (not exists $self->{members}) {
		my $url = sprintf "https://api.sbgenomics.com/v2/users?division=%s", 
			$self->id;
		my @results = $self->execute('GET', $url);
			
		my @members = map { SB2::Member->new($self, $_) } @results;
		$self->{members} = \@members;
	}
	return wantarray ? @{ $self->{members} } : $self->{members};
}

sub billing_group {
	my $self = shift;
	if (not exists $self->{billing}) {
		my @results = $self->execute('GET', 'https://api.sbgenomics.com/v2/billing/groups');
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
	my $url = sprintf "https://api.sbgenomics.com/v2/teams?division=%s&_all=true", 
		$self->division;
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
	my $result = $self->execute('POST', 'https://api.sbgenomics.com/v2/teams/', $h, $data);
	return $result ? SB2::Team->new($self, $result) : undef;
}


####################################################################################
package SB2::Project;
use strict;
use Carp;
use base 'SB2';

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
	elsif ($member =~ /^([a-z0-9\-]+)\/([\w\-\.]+)$/) {
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
	return $result;
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
	elsif ($member =~ /^([a-z0-9\-]+)\/([\w\-\.]+)$/) {
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





####################################################################################
package SB2::Member;
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




####################################################################################
package SB2::Team;
use strict;
use Carp;
use base 'SB2';

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
	my $url = sprintf "https://api.sbgenomics.com/v2/teams/%s/members", $self->{id};
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
 Dept of Oncological Sciences
 Huntsman Cancer Institute
 University of Utah
 Salt Lake City, UT, 84112

This package is free software; you can redistribute it and/or modify
it under the terms of the Artistic License 2.0.  


