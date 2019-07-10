package SB;
our $VERSION = 1.1;

=head1 NAME

SB - a Perl wrapper around the Seven Bridges SB command line tool

=head1 DESCRIPTION

This is a simplified Perl wrapper around the Seven Bridges command line tool, sb.
This tool has limited functionality compared to the other API toolkits, notably the 
Python API. It's particularly lacking regarding folder support. 

But for limited stuff, it's (mostly) fine.

=head1 METHODS

=head2 SB Class

Main Class. Inherited by subclasses. Start here.

=over 4

=item new

Provide parameters as array:

    sb       => $sb_path, # default found in PATH
    cred     => $credential_path, # default ~/.sevenbridges/credentials
    division => $division # required

=item sb_path

=item credentials

=item division

=item verbose($verbosity)

=item execute(@options)

Main function for executing the sb utility. Most users shouldn't need to 
run this, unless a wrapper doesn't exist. Pass an array of command line options. 
The C<--profile> is automatically set for the division name, as is the 
credentials path. All results are returned as JSON and parsed into Perl hashes.
Lists of results are parsed and placed into an array. Values returned as array 
or array reference (if expecting more than one), or just the first value (when 
expecting only one). Be careful on what you expect. 

=item projects

Return list of available projects as SB::Project objects within current division.

=item create_project

Make new project. Returns SB::Project.
Pass array of information.

    name        => $name,
    description => $description, # can be Markdown

=item bulk_uploader($path_to_sbg-uploader.sh, @options)

Handles setting the division and providing the proper token from credentials file.

=back

=head2 SB::Project Class

Most of these are read-only functions, except for 

=over 4

=item id

=item name

=item description

=item update

Pass array of information to update the project.
Returns 1 if successful. Object metadata is updated.

    name        => $new_name,
    description => $text, # can be Markdown


=item list_members

Returns list of SB::Member objects as part of current project.

=item list_files

Returns list of SB::File objects in current project.

B<NOTE>: The stupid command line tool does not support folders! There is currently 
no way to navigate beyond a folder. Flat file structure only!

=upload($file)

Returns new SB::File object.

=back

=head2 SB::Member Class

These are all read-only functions. The sb tool doesn't allow modifying anyone 
but yourself. 

=over 4

=item user

=item admin

=item read

=item copy

=item write

=item exec

=back

=head2 SB::File Class

Objects representing files. Most of these are read-only functions, except for update().

=over 4 

=item id

=item name

=item project

=item download($path)

=item size

=item metadata

=item url

=item copy($new_project_id)

=item update

Pass array of information to update the file.
Returns 1 if successful. 

    name     => $new_name,
    tags     => [qw(tag1 tag2)],
    metadata => ????????

=back


=cut

use strict;
use warnings;
use Carp;
use IO::File;
use File::Spec;
use JSON::PP;	# this is reliably installed, XS is not
				# XS would be better performance, but we're not doing anything complicated

# global value that sb was found
our $SB_FOUND = 0;
our $VERBOSE  = 0;

1;

sub new {
	my $class = shift;
	if (ref($class)) {
		$class = ref($class);
	}
	my %args = @_;
	
	my $self = {
		sb   => undef,
		div  => undef,
		cred => undef,
	};
	bless $self, $class;
	
	# look for credentials file
	$args{cred_path} ||= $args{cred} || undef;
	if (defined $args{cred_path}) {
		$self->credentials($args{cred_path}) or croak "bad credentials!";
	}
	else {
		my $f = File::Spec->catfile($ENV{HOME}, '.sevenbridges', 'credentials');
		$self->credentials($f) or croak "no credentials!";
	}
	
	# ideally also accept a token value, which would be written to a temp credentials 
	# file and used
	
	# look for division or profile
	$args{division} ||= $args{profile} || undef;
	if (defined $args{division}) {
		$self->division($args{division});
	}
	
	# look for executable
	$args{sb_path} ||= $args{sb} || undef;
	if (defined $args{sb_path}) {
		$self->sb_path($args{sb_path}) or croak "bad sb path!";
	}
	else {
		my $p = `which sb`;
		chomp $p;
		unless (defined $p) {
			croak("unable to find sb path! Please define!");
		}
		$self->sb_path($p) or croak "bad sb path!";
	}
	
	printf " > initialized: sb %s, cred %s, div %s\n", $self->{sb}, $self->{cred}, $self->{div} if $VERBOSE;
	
	return $self;
}


sub sb_path {
	my $self = shift;
	if (@_) {
		$self->{sb} = $_[0];
		# check 
		unless ($SB_FOUND) {
			my $result = $self->execute('version');
			if ($result and ref($result) eq 'HASH' and $result->{version}) {
				# we are good
				$SB_FOUND = 1;
			}
			else {
				croak "sb path doesn't look good! this was the result:\n $result\n";
			}
		}
	}
	return $self->{sb};
}


sub credentials {
	my $self = shift;
	if (@_) {
		my $f = $_[0];
		if (-e $f) {
			$self->{cred} = $f;
		}
		else {
			croak "given credential file '$f' doesn't exist!";
		}
	}
	return $self->{cred};
}


sub division {
	my $self = shift;
	if (@_) {
		$self->{div} = $_[0];
	}
	return $self->{div};
}

sub verbose {
	my $self = shift;
	if (@_) {
		$VERBOSE = $_[0];
	}
	return $VERBOSE;
}

sub execute {
	my $self = shift;
	my @args = @_;
	my $command = join(' ', $self->{sb}, 
						@args, 
						'--output', 'json', 
						'--profile', $self->{div},
						'--config', $self->{cred},
						);
	print " > executing: $command\n" if $VERBOSE;
	my $raw = qx($command);
	chomp $raw;
	my @results;
	foreach (split "\n", $raw) {
		push @results, decode_json($_);
	}
	return wantarray ? @results : scalar(@results) == 1 ? $results[0] : \@results;
	
}


sub projects {
	my $self = shift;
	return if (ref($self) ne 'SB'); # don't want this called from inherited object
	if (not exists $self->{projects}) {
		my @results = $self->execute('projects', 'list');
		my @projects = map { SB::Project->new($self, $_) } @results;
		$self->{projects} = \@projects;
	}
	return wantarray ? @{ $self->{projects} } : $self->{projects};
}

sub create_project {
	my $self = shift;
	my %args = @_;
	my @commands = ('projects', 'create');
	
	# name
	if (exists $args{name}) {
		push @commands, '--name', $args{name};
	}
	else {
		croak "name is required for creating projects!";
	}
	
	# description
	if (exists $args{description}) {
		push @commands, '--description', sprintf("\"%s\"", $args{description});
	}
	
	# execute
	my $result = $self->execute(@commands);
	return SB::Project->new($self, $result);
}

sub bulk_uploader {
	my $self = shift;
	my $sbupload_path = shift;
	my @options = @_;
	
	# Grab the token
	my $token;
	my $fh = IO::File->new($self->credentials) or 
		die "unable to read credentials files!\n";
	my $target = sprintf("[%s]", $self->division);
	while (not defined $token) {
		my $line = $fh->getline or last;
		chomp $line;
		if ($line eq $target) {
			# we found the section!
			while (my $line2 = $fh->getline) {
				if ($line2 =~ /^\[/) {
					# we've gone too far!!!??? start of next section
					last;
				}
				elsif ($line2 =~ /^auth_token\s+=\s+(\w+)$/) {
					# we found it!
					$token = $1;
					last;
				}
			}
		}
	}
	$fh->close;
	unless ($token) {
		print " unable to get token from credentials!\n";
		return;
	}
	
	# execute
	my $command = join(" ", $sbupload_path, '--project', $self->division, '--token', 
						$token, @options);
	print " > executing: $command\n" if $VERBOSE;
	my $result = qx($command);
	return $result;
}



####################################################################################
package SB::Project;
use base 'SB';
use Carp;

sub new {
	my ($class, $parent, $result) = @_;
	my $self = {
		sb   => $parent->{sb},
		div  => $parent->{div},
		cred => $parent->{cred},
		id   => $result->{id},
		name => $result->{name},
	};
	if (exists $result->{description}) {
		$self->{description} = $result->{description};
	}
	if (exists $result->{description}) {
		$self->{billing} = $result->{billing_group};
	}
	return bless $self, $class;
}

sub id {
	return shift->{id};
}

sub project {
	return shift->{id};
}

sub name {
	my $self = shift;
	if (@_) {
		# updating the name
		$self->update('name', $_[0]);
	}
	return $self->{name};
}

sub description {
	my $self = shift;
	if (@_) {
		# setting a description
		$self->update('description', $_[0]);
	}
	else {
		# get description
		unless (exists $self->{description}) {
			my $result = $self->execute('projects', 'get', $self->{id});
			$self->{description} = $result->{description};
			$self->{billing} = $result->{billing_group};
		}
	}
	return $self->{description};
}

sub update {
	my $self = shift;
	my %args = @_;
	my @commands = ('projects', 'update', $self->{id});
	
	# description
	if ($args{description}) {
		push @commands, ('--description', sprintf("\"%s\"", $args{description}) );
	}
	
	# name
	if ($args{name}) {
		push @commands, ('--name', sprintf("\"%s\"", $args{name}) );
	}
	
	# execute
	my $result = $self->execute(@commands); 
	$self->{name} = $result->{name};
	$self->{description} = $result->{description};
	$self->{billing} = $result->{billing_group};
	
	return 1;
}


sub list_members {
	my $self = shift;
	unless ($self->{members}) {
		my @results = $self->execute('members', 'list', '--project', $self->{id});
		my @members = map { SB::Member->new($self, $_) } @results; 
		$self->{members} = \@members;
	}
	return wantarray ? @{ $self->{members} } : $self->{members};
}

sub list_files {
	my $self = shift;
	unless (exists $self->{files}) {
		my @results = $self->execute('files', 'list', '--project', $self->{id});
		my @files = map { SB::File->new($self, $_) } @results; 
		$self->{files} = \@files;
	}
	return wantarray ? @{ $self->{files} } : $self->{files};
}

sub upload {
	my $self = shift;
	my $file = shift;
	croak "file '$file' doesn't exist!" if not -e $file;
	my @command = ('files', 'upload', $file, '--project', $self->{id});
	my $result = $self->execute(@command);
	return SB::File->new($self, $result);
}





####################################################################################
package SB::Member;
use base 'SB';

sub new {
	my ($class, $parent, $result) = @_;
	printf " > making member %s with admin %s, copy %s, read %s, write %s, exec %s\n", $result->{username}, $result->{permissions}{admin}, $result->{permissions}{copy}, $result->{permissions}{read}, $result->{permissions}{write}, $result->{permissions}{execute} if $VERBOSE;
	my $self = {
		sb     => $parent->{sb},
		div    => $parent->{div},
		cred   => $parent->{cred},
		user   => $result->{username},
		admin  => $result->{permissions}{admin},
		copy   => $result->{permissions}{copy},
		read   => $result->{permissions}{read},
		write  => $result->{permissions}{write},
		exec   => $result->{permissions}{execute},
	};
	return bless $self, $class;
}

sub user {
	return shift->{user};
}

sub name {
	return shift->{user};
}

sub admin {
	my $self = shift;
	return $self->{admin};
}

sub copy {
	my $self = shift;
	return $self->{copy};
}

sub write {
	my $self = shift;
	return $self->{write};
}

sub read {
	my $self = shift;
	return $self->{read};
}

sub exec {
	my $self = shift;
	return $self->{exec};
}






####################################################################################
package SB::File;
use base 'SB';
use Carp;

sub new {
	my ($class, $parent, $result) = @_;
	my $self = {
		sb      => $parent->{sb},
		div     => $parent->{div},
		cred    => $parent->{cred},
		project => $parent->project,
		id      => $result->{id},
		name    => $result->{name},
	};
	return bless $self, $class;
}

sub id {
	return shift->{id};
}

sub name {
	return shift->{name};
}

sub project {
	return shift->{project};
}

sub download {
	my $self = shift;
	my $path = shift || './'; # current directory
	$self->execute('download', '--destination', $path, '--file', $self->{id});
	# not sure what is returned....
}

sub info {
	my $self = shift;
	my $result = $self->execute('files', 'get', $self->{id});
	$self->{size} = $result->{size};
	$self->{created} = $result->{created_on};
	$self->{modified} = $result->{modified_on};
	$self->{type} = $result->{type};
	$self->{metadata} = $result->{metadata};
	$self->{tags} = $result->{tags};
	return 1;
}

sub size {
	my $self = shift;
	unless (exists $self->{size}) {
		$self->info;
	}
	return $self->{size};
}

sub metadata {
	my $self = shift;
	unless (exists $self->{metadata}) {
		$self->info;
	}
	return $self->{metadata};
}

sub url {
	my $self = shift;
	my $result = $self->execute('files', 'url', $self->{id});
	return $result->{url};
}

sub copy {
	my $self = shift;
	my $project = shift;
	my $result = $self->execute('files', 'copy', $self->{id}, '--project', $project);
	# I assume new information about new file is returned????
	return SB::File->new($self, $result);
}

sub update {
	my $self = shift;
	my %args = @_;
	my @command = ('files', 'update', $self->{id});
	
	# name
	if ($args{name}) {
		push @command, '--name', $args{name};
	}
	
	# tags
	$args{tag} ||= $args{tags} || undef;
	if ($args{tag}) {
		push @command, '--tag';
		if (ref($args{tag}) eq 'ARRAY') {
			push @command, @{$args{tag}};
		}
		else {
			push @command, $args{tag};
		}
	}
	
	# metadata
	# I don't know what format this should be.....
	
	# execute and update
	my $result = $self->execute(@command);
	$self->{name} = $result->{name};
	$self->{tags} = $result->{tags}; # ?????
	$self->{metadata} = $result->{metadata};
	return 1;
}
