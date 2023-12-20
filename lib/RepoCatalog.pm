package RepoCatalog;


use strict;
use English qw(-no_match_vars);
use Carp;
use IO::File;
use DBM::Deep;

our $VERSION = 6.0;


# General private values

my $DEFAULT_PATH  = "~/test/repository.db";
my $repo_epoch = 2005;



### Numbers for identifying years - there are some exceptions!!???
my %anal2year = (
	19		=> 2008,
	81		=> 2009,
	202		=> 2010,
	422		=> 2011,
	1325	=> 2012,
	2290	=> 2013,
	2817	=> 2014,
	3377	=> 2015,
	4060	=> 2016,
	5379	=> 2017,
	5693	=> 2018,
	6091	=> 2019,
	1000000 => 2020, # impossibly big number means current year
);
my %req2year = (
	5034	=> 2005,
	5288	=> 2006,
	6146	=> 2007, 
	6536	=> 2008, # has one exception
	7118	=> 2009, # has exceptions
	7914	=> 2010, # has exceptions
	8929	=> 2011, # has exceptions
	8999	=> 2012, # has exceptions
	10590	=> 2013,
	11325	=> 2014,
	12068	=> 2015,
	13987	=> 2016,
	14838	=> 2017,
	15743	=> 2018,
	17680	=> 2019,
	1000000 => 2020, # impossibly big number means current year
);

my $internal_org = qr/(?: Bioinformatics \s Shared \s Resource | HTG \s Core \s Facility | SYSTEM )/x;
my $HEADER = "ID\tPath\tName\tDate\tGroup\tUserEmail\tUserFirst\tUserLast\tLabFirst\tLabLast\tPIEmail\tCORELab\tProfile\tBucket\tPrefix\tExternal\tStatus\tApplication\tOrganism\tGenome\tSize\tLastSize\tAge\tScan\tUpload\tHidden\tDeleted\tEmailed\n";


# Functions

sub new {
	my $class = shift;
	my $path  = shift || $DEFAULT_PATH;
	
	# open the database file
	my $db;
	if (-e $path) {
		$db = DBM::Deep->new($path) or 
			croak "unable to open database file '$path'! $OS_ERROR";
		# check if current version
		my $first = $db->first_key;
		my $data = $db->get($first);
		if ( scalar @{ $data } != 28 ) {
			croak "Database first entry does not have 28 fields! Old database?";
		}
	}
	else {
		# make a new database file
		$db = DBM::Deep->new(
			file     => $path,
		) or croak "unable to initialize database file '$path'! $OS_ERROR";
	}
		
	my $self = {
		file    => $path,
		db      => $db,
	};
	
	return bless $self, $class;
}

sub file {
	return shift->{file};
}

sub db {
	return shift->{db};
}

sub entry {
	my ($self, $project) = @_;
	confess "no project provided!" unless defined $project;
	if ( $self->{db}->exists($project) ) {
		# return existing project
		return RepoEntry->new( $self->{db}->get($project) );
	}
	else {
		return;
	}
}

sub new_entry {
	my ($self, $project) = @_;
	unless ($project) {
		carp "no project identifier provided!";
		return;
	}
	if ( $self->{db}->exists($project) ) {
		carp (" project $project exists!\n");
		# go ahead and return the entry
		return RepoEntry->new( $self->{db}->get($project) );
	}
	else {
		# make a new entry
		# the project ID is always the first element in the array
		my @data = ($project, map { q() } (1..27));
		my $p = $self->{db}->put($project, \@data);
		if ($p) {
			return RepoEntry->new( $self->{db}->get($project) );
		}
		else {
			confess("unable to store a new entry in database!");
		}
	}
}

sub delete_entry {
	my ($self, $project) = @_;
	croak "no project provided!" unless defined $project;
	return $self->{db}->delete($project);
}

sub optimize {
	my $self = shift;
	return $self->{db}->optimize();
}

sub list_all {
	my $self = shift;
	my %opts = @_;
	my $year = (exists $opts{year} and defined $opts{year}) ? $opts{year} : $repo_epoch;
	my $core   = (exists $opts{core} and defined $opts{core}) ? $opts{core} : undef;
	my $min_age = (exists $opts{age} and $opts{age} =~ /^\d+$/) ? $opts{age} : 0;
	my $max_age = (exists $opts{maxage} and $opts{maxage} =~ /^\d+$/) ? $opts{maxage} : 0;
	my $ext  = (exists $opts{external} and $opts{external}) ? $opts{external} : 'N';
	my $min_size = (exists $opts{size} and $opts{size} =~ /^\d+$/) ? $opts{size} : 0;
	
	# scan through list
	my @list;
	my $key = $self->{db}->first_key;
	while ($key) {
		my $E = $self->entry($key);
		if (
			substr($E->date, 0, 4) >= $year and
			$E->age >= $min_age and
			( $max_age ? ($E->age <= $max_age) ? 1 : 0 : 1) and
			( $min_size ? ($E->size >= $min_size) ? 1 : 0 : 1)
		) {
			# we have a possible candidate
			if (defined $core) {
				if ($core and $E->core_lab) {
					push @list, $key;
				}
				elsif (not $core and not $E->core_lab) {
					push @list, $key if $E->external eq $ext;
				}
				# else doesn't match
			}
			else {
				push @list, $key;
			}
		}
		$key = $self->{db}->next_key($key);
	}
	@list = sort {$a cmp $b} @list;
	return wantarray ? @list : \@list;
}

sub list_projects_for_pi {
	my $self = shift;
	my %opts;
	if (scalar(@_) == 1) {
		$opts{name} = $_[0];
	}
	else {
		%opts = @_;
	}
	my $name = lc($opts{name}) || undef;
	unless ($name) {
		carp "must provide PI last name!";
		return;
	}
	my $year = (exists $opts{year} and defined $opts{year}) ? $opts{year} : $repo_epoch;
	my $min_age = (exists $opts{age} and $opts{age} =~ /^\d+$/) ? $opts{age} : 0;
	my $max_age = (exists $opts{maxage} and $opts{maxage} =~ /^\d+$/) ? $opts{maxage} : 0;
	my $min_size = (exists $opts{size} and $opts{size} =~ /^\d+$/) ? $opts{size} : 0;
	# CORE lab and external status is based on PI, so no need to filter those
	
	# scan through list
	my @list;
	my $key = $self->{db}->first_key;
	while ($key) {
		my $E = $self->entry($key);
		if (
			lc $E->lab_last eq $name and
			substr($E->date, 0, 4) >= $year and
			$E->age >= $min_age and
			( $max_age ? ($E->age and $E->age <= $max_age) ? 1 : 0 : 1) and
			( $min_size ? ($E->size and $E->size >= $min_size) ? 1 : 0 : 1)
		) {
			push @list, $key;
		}
		$key = $self->{db}->next_key($key);
	}
	@list = sort {$a cmp $b} @list;
	return wantarray ? @list : \@list;
}

sub export_to_file {
	my ($self, $file, $transform) = @_;
	croak "no output file provided!" unless defined $file;
	$transform ||= 0;
	my $fh = IO::File->new($file, '>') or 
		croak "unable to open $file for writing! $OS_ERROR\n";
	$fh->binmode(':utf8');
	$fh->print($HEADER);
	
	# iterate and export as a tab-delimited file
	my $key = $self->{db}->first_key;
	while ($key) {
		my $E = $self->entry($key);
		my $string = $E->print_string($transform);
		$fh->print($string);
		$key = $self->{db}->next_key($key);
	}
	$fh->close;
	return 1;
}


sub import_from_file {
	my ($self, $file, $force) = @_;
	croak "no output file provided!\n" unless defined $file;
	$force ||= 0;
	my $fh = IO::File->new($file, '<') or 
		croak "unable to open $file for reading! $OS_ERROR\n";
	$fh->binmode(':utf8');
	my $firstline = $fh->getline;
	unless ( $firstline eq $HEADER ) {
		croak "header doesn't match an exported file format!\n";
	}
	
	# check catalog
	my $key = $self->{db}->first_key || undef;
	if ($key) {
		carp "\nWARNING! Catalog file is not new!\n";
		if ($force) {
			print "\nWARNING! Forcibly importing data into an existing database! Existing entries will be overwritten!\n";
		}
		else {
			print "\nWARNING! Attempt to import data into an existing database is not allowed!\n Use --force to do so\n";
			return;
		}
	}
	
	# import one at a time
	my $i = 0;
	while (my $line = $fh->getline) {
		chomp $line;
		my @data = split /\t/, $line;
		my $id = $data[0];
		$self->{db}->put($id => \@data);
		$i++;
	}
	$fh->close;
	return $i;
}



sub find_requests_to_upload {
	my $self = shift;
	my %opts = @_;
	my $year = (exists $opts{year} and defined $opts{year}) ? $opts{year} : $repo_epoch;
	my $min_size = (exists $opts{size} and $opts{size} =~ /^\d+$/) ? $opts{size} : 100000000;
		# set the minimum size to 100 MB
	my $min_age = (exists $opts{age} and defined $opts{age}) ? $opts{age} : 0; 
	my $max_age = (exists $opts{maxage} and defined $opts{maxage}) ? $opts{maxage} : 60; 
	
	# scan through list
	my @list;
	my $key = $self->{db}->first_key;
	while ($key) {
		my $E = $self->entry($key);
		if (
			$E->lab_last !~ $internal_org and
			$E->is_request and
			$E->request_status eq 'COMPLETE' and
			$E->core_lab and 
			substr($E->date, 0, 4) >= $year and
			$E->age >= $min_age and
			$E->age <= $max_age and
			not $E->hidden_datestamp
		) {
			# we have a candidate
			# we could try to filter on application type, but there are so many 
			# possibilities that it's generally easier to screen for size
			# fortunately fastq files are big
			# too big, and we might miss small MiSeq projects
			# too little, and we might include large sample-quality projects
			if ($E->size > $min_size) {
				# size is bigger than 25 MB, looks like a candidate
				if ($E->upload_datestamp) {
					# already been uploaded? Make sure we're considerably bigger
					if (
						$E->upload_age > $E->age and
						$E->last_size and 
						($E->size - $E->last_size) > $min_size
					) {
						# must have added new fastq files???
						push @list, $key;
					}
				}
				else {
					# not uploaded yet! let's do it
					push @list, $key;
				}
			} 
		}
		$key = $self->{db}->next_key($key);
	}
	@list = sort {$a cmp $b} @list;
	return wantarray ? @list : \@list;
}


sub find_requests_to_hide {
	my $self = shift;
	my %opts = @_;
	my $year = (exists $opts{year} and defined $opts{year}) ? $opts{year} : $repo_epoch;
	my $core   = (exists $opts{core} and defined $opts{core}) ? $opts{core} : undef;
	my $min_age = (exists $opts{age} and $opts{age} =~ /^\d+$/) ? $opts{age} : 180;
	my $max_age = (exists $opts{maxage} and $opts{maxage} =~ /^\d+$/) ? $opts{maxage} : 100000;
	my $ext  = (exists $opts{external} and $opts{external}) ? $opts{external} : 'N';
	my $min_size = (exists $opts{size} and $opts{size} =~ /^\d+$/) ? $opts{size} : 100000000;
		# set minimum size to 100 MB
	
	# scan through list
	my @list;
	my $key = $self->{db}->first_key;
	while ($key) {
		my $E = $self->entry($key);
		if (
			$E->lab_last !~ $internal_org and
			$E->is_request and
			$E->request_status eq 'COMPLETE' and        # finished
			not $E->hidden_datestamp and                # not hidden yet
			$E->size > $min_size and                    # size > minimum size
			substr($E->date, 0, 4) >= $year and         # current year
			$E->age >= $min_age and                     # older than 6 months
			( $max_age ? ($E->age <= $max_age) ? 1 : 0 : 1)
		) {
			# we have a possible candidate
			if (defined $core) {
				if ($core and $E->core_lab) {
					push @list, $key;
				}
				elsif (not $core and not $E->core_lab) {
					push @list, $key if $E->external eq $ext;
				}
				# else doesn't match
			}
			else {
				push @list, $key;
			}
		}
		$key = $self->{db}->next_key($key);
	}
	@list = sort {$a cmp $b} @list;
	return wantarray ? @list : \@list;
}


sub find_requests_to_delete {
	my $self = shift;
	my %opts = @_;
	my $year = (exists $opts{year} and defined $opts{year}) ? $opts{year} : $repo_epoch;
	my $core   = (exists $opts{core} and defined $opts{core}) ? $opts{core} : undef;
	my $min_age = (exists $opts{age} and $opts{age} =~ /^\d+$/) ? $opts{age} : 60;
	my $max_age = (exists $opts{maxage} and $opts{maxage} =~ /^\d+$/) ? $opts{maxage} : 0;
	my $ext  = (exists $opts{external} and $opts{external}) ? $opts{external} : 'N';
	my $min_size = (exists $opts{size} and $opts{size} =~ /^\d+$/) ? $opts{size} : 0;
	
	# scan through list
	my @list;
	my $key = $self->{db}->first_key;
	while ($key) {
		my $E = $self->entry($key);
		if (
			$E->lab_last !~ $internal_org and
			$E->is_request and
			$E->request_status eq 'COMPLETE' and        # finished
			$E->hidden_datestamp and                    # hidden
			not $E->deleted_datestamp and               # not yet deleted
			$E->hidden_age >= $min_age and              # older than 60 days
			substr($E->date, 0, 4) >= $year and         # current year
			( $max_age ? ($E->hidden_age <= $max_age) ? 1 : 0 : 1) and
			( $min_size ? ($E->size >= $min_size) ? 1 : 0 : 1)
		) {
			# we have a possible candidate
			if (defined $core) {
				if ($core and $E->core_lab) {
					push @list, $key;
				}
				elsif (not $core and not $E->core_lab) {
					push @list, $key if $E->external eq $ext;
				}
				# else doesn't match
			}
			else {
				push @list, $key;
			}
		}
		$key = $self->{db}->next_key($key);
	}
	@list = sort {$a cmp $b} @list;
	return wantarray ? @list : \@list;
}

sub find_analysis_to_upload {
	my $self = shift;
	my %opts = @_;
	my $year = (exists $opts{year} and defined $opts{year}) ? $opts{year} : $repo_epoch;
	my $min_age = (exists $opts{age} and $opts{age} =~ /^\d+$/) ? $opts{age} : 270;
	my $max_age = (exists $opts{maxage} and $opts{maxage} =~ /^\d+$/) ? $opts{maxage} : 0;
	my $min_size = (exists $opts{size} and $opts{size} =~ /^\d+$/) ? $opts{size} : 100000000;
		# set minimum size to 100 MB
	
	# scan through list
	my @list;
	my $key = $self->{db}->first_key;
	while ($key) {
		my $E = $self->entry($key);
		if (
			$E->lab_last !~ $internal_org and
			not $E->is_request and
			$E->core_lab and                            # has division
			not $E->hidden_datestamp   and              # not already hidden
			$E->size > $min_size and                    # size > minimum
			$E->age >= $min_age and                     # older than 9 months
			substr($E->date, 0, 4) >= $year and         # current year
			( $max_age ? ($E->age <= $max_age) ? 1 : 0 : 1)
		) {
			# we have a candidate
			push @list, $key;
		}
		$key = $self->{db}->next_key($key);
	}
	@list = sort {$a cmp $b} @list;
	return wantarray ? @list : \@list;
}


sub find_analysis_to_hide {
	my $self = shift;
	my %opts = @_;
	my $year = (exists $opts{year} and defined $opts{year}) ? $opts{year} : $repo_epoch;
	my $core   = (exists $opts{core} and defined $opts{core}) ? $opts{core} : undef;
	my $min_age = (exists $opts{age} and $opts{age} =~ /^\d+$/) ? $opts{age} : 270;
	my $max_age = (exists $opts{maxage} and $opts{maxage} =~ /^\d+$/) ? $opts{maxage} : 0;
	my $ext  = (exists $opts{external} and $opts{external}) ? $opts{external} : 'N';
	my $min_size = (exists $opts{size} and $opts{size} =~ /^\d+$/) ? $opts{size} : 100000000;
		# set minimum size to 100 MB
	
	# scan through list
	my @list;
	my $key = $self->{db}->first_key;
	while ($key) {
		my $E = $self->entry($key);
		if (
			$E->lab_last !~ $internal_org and
			not $E->is_request and
			not $E->hidden_datestamp and                # not already hidden
			$E->size > $min_size and                    # size > minimum
			$E->age >= $min_age and                     # older than 9 months
			substr($E->date, 0, 4) >= $year and         # current year
			( $max_age ? ($E->age <= $max_age) ? 1 : 0 : 1)			
		) {
			# we have a possible candidate
			if (defined $core) {
				if ($core and $E->core_lab) {
					push @list, $key;
				}
				elsif (not $core and not $E->core_lab) {
					push @list, $key if $E->external eq $ext;
				}
				# else doesn't match
			}
			else {
				push @list, $key;
			}
		}
		$key = $self->{db}->next_key($key);
	}
	@list = sort {$a cmp $b} @list;
	return wantarray ? @list : \@list;
}


sub find_analysis_to_delete {
	my $self = shift;
	my %opts = @_;
	my $year = (exists $opts{year} and defined $opts{year}) ? $opts{year} : $repo_epoch;
	my $core   = (exists $opts{core} and defined $opts{core}) ? $opts{core} : undef;
	my $min_age = (exists $opts{age} and $opts{age} =~ /^\d+$/) ? $opts{age} : 60;
	my $max_age = (exists $opts{maxage} and $opts{maxage} =~ /^\d+$/) ? $opts{maxage} : 0;
	my $ext  = (exists $opts{external} and $opts{external}) ? $opts{external} : 'N';
	my $min_size = (exists $opts{size} and $opts{size} =~ /^\d+$/) ? $opts{size} : 0;
	
	# scan through list
	my @list;
	my $key = $self->{db}->first_key;
	while ($key) {
		my $E = $self->entry($key);
		if (
			$E->lab_last !~ $internal_org and
			not $E->is_request and
			$E->hidden_datestamp and                    # hidden
			not $E->deleted_datestamp and               # not yet deleted
			$E->hidden_age >= $min_age and              # older than 60 days
			substr($E->date, 0, 4) >= $year and         # current year
			( $max_age ? ($E->hidden_age <= $max_age) ? 1 : 0 : 1) and
			( $min_size ? ($E->size >= $min_size) ? 1 : 0 : 1)
		) {
			# we have a possible candidate
			if (defined $core) {
				if ($core and $E->core_lab) {
					push @list, $key;
				}
				elsif (not $core and not $E->core_lab) {
					push @list, $key if $E->external eq $ext;
				}
				# else doesn't match
			}
			else {
				push @list, $key;
			}
		}
		$key = $self->{db}->next_key($key);
	}
	@list = sort {$a cmp $b} @list;
	return wantarray ? @list : \@list;
}

sub header {
	return $HEADER;
}

# search for other things? projects to upload, hide, delete?
1;


######################## RepoEntry #######################################################

package RepoEntry;

use strict;
use Carp;
use constant {
	ID          => 0,     # gnomex ID
	PATH        => 1,     # bio-repo file server path
	NAME        => 2,     # gnomex project name
	DATE        => 3,     # gnomex project made date yyyy-mm-dd format
	GROUP       => 4,     # gnomex group fold name
	USEREMAIL   => 5,     # user email address
	USERFIRST   => 6,     # user first name
	USERLAST    => 7,     # user last name
	LABFIRST    => 8,     # PI first name
	LABLAST     => 9,     # PI last name
	PIEMAIL     => 10,    # PI email address
	CORELAB     => 11,    # name of CORE lab
	PROFILE     => 12,    # IAM profile name for access
	BUCKET      => 13,    # s3 bucket name
	PREFIX      => 14,    # s3 prefix
	EXTERNAL    => 15,    # y or n boolean
	STATUS      => 16,    # gnomex request status
	APPLICATION => 17,    # gnomex request application type
	ORGANISM    => 18,    # gnomex organism string
	GENOME      => 19,    # gnomex genome build
	SIZE        => 20,    # current project file size total in bytes
	LASTSIZE    => 21,    # previous project file size total in bytes
	AGE         => 22,    # unix timestamp for youngest observed file in project
	SCAN        => 23,    # unix timestamp for scanning
	UPLOAD      => 24,    # unix timestamp for uploading
	HIDDEN      => 25,    # unix timestamp for hiding
	DELETED     => 26,    # unix timestamp for deleting
	EMAILED     => 27,    # unix timestamp for emailing
	DAY         => 86400, # 60 seconds * 60 minutes * 24 hours
};


sub new {
	my ($class, $data) = @_;
	if (ref($data) !~ /DBM..Deep/) {
		confess "not a DBM::Deep reference!";
		return;
	}
	my $self = {
		data => $data,
	};
	return bless $self, $class;
}


sub is_request {
	my $self = shift;
	if ($self->{data}->[ID] =~ /\d+R$/) {
		return 1;
	}
	else {
		return 0;
	}
}


sub id {
	my $self = shift;
	return $self->{data}->[ID];
}


sub path {
	my $self = shift;
	if (@_) {
		$self->{data}->[PATH] = $_[0];
	}
	return $self->{data}->[PATH];
}


sub name {
	my $self = shift;
	if (@_) {
		$self->{data}->[NAME] = $_[0];
	}
	return $self->{data}->[NAME];
}


sub date {
	my $self = shift;
	if (@_) {
		$self->{data}->[DATE] = $_[0];
	}
	return $self->{data}->[DATE];
}


sub group {
	my $self = shift;
	if (@_) {
		$self->{data}->[GROUP] = $_[0];
	}
	return $self->{data}->[GROUP];
}


sub user_email {
	my $self = shift;
	if (@_) {
		$self->{data}->[USEREMAIL] = $_[0];
	}
	return $self->{data}->[USEREMAIL];
}


sub user_first {
	my $self = shift;
	if (@_) {
		$self->{data}->[USERFIRST] = $_[0];
	}
	return $self->{data}->[USERFIRST];
}


sub user_last {
	my $self = shift;
	if (@_) {
		$self->{data}->[USERLAST] = $_[0];
	}
	return $self->{data}->[USERLAST];
}


sub lab_first {
	my $self = shift;
	if (@_) {
		$self->{data}->[LABFIRST] = $_[0];
	}
	return $self->{data}->[LABFIRST];
}


sub lab_last {
	my $self = shift;
	if (@_) {
		$self->{data}->[LABLAST] = $_[0];
	}
	return $self->{data}->[LABLAST];
}


sub pi_email {
	my $self = shift;
	if (@_) {
		$self->{data}->[PIEMAIL] = $_[0];
	}
	return $self->{data}->[PIEMAIL];
}


sub core_lab {
	my $self = shift;
	if (@_) {
		$self->{data}->[CORELAB] = $_[0];
	}
	return $self->{data}->[CORELAB];
}

sub profile {
	my $self = shift;
	if (@_) {
		$self->{data}->[PROFILE] = $_[0];
	}
	return $self->{data}->[PROFILE];
}

sub bucket {
	my $self = shift;
	if ( @_ and defined $_[0] ) {
		$self->{data}->[BUCKET] = $_[0];
	}
	return $self->{data}->[BUCKET] || q();
}

sub prefix {
	my $self = shift;
	if ( @_ and defined $_[0] ) {
		$self->{data}->[PREFIX] = $_[0];
	}
	return $self->{data}->[PREFIX] || q();
}


sub external {
	my $self = shift;
	if (@_) {
		$self->{data}->[EXTERNAL] = $_[0];
	}
	return $self->{data}->[EXTERNAL];
}


sub request_status {
	my $self = shift;
	if (@_) {
		$self->{data}->[STATUS] = $_[0];
	}
	return $self->{data}->[STATUS];
}


sub request_application {
	my $self = shift;
	if (@_) {
		$self->{data}->[APPLICATION] = $_[0];
	}
	return $self->{data}->[APPLICATION];
}


sub organism {
	my $self = shift;
	if (@_) {
		$self->{data}->[ORGANISM] = $_[0];
	}
	return $self->{data}->[ORGANISM];
}


sub genome {
	my $self = shift;
	if (@_) {
		$self->{data}->[GENOME] = $_[0];
	}
	return $self->{data}->[GENOME];
}


sub size {
	my $self = shift;
	if (@_) {
		my $newsize = $_[0];
		my $cursize = $self->{data}->[SIZE];
		if ($cursize) {
			my $delta = abs($cursize - $newsize);
			if ($delta > 25_000_000 or ($delta / $cursize) > 0.1) {
				# there's a significant change of greater than 25 MB or 10%
				# then store the last size
				$self->{data}->[LASTSIZE] = $cursize;
			}
		}
		$self->{data}->[SIZE] = $newsize;
	}
	return $self->{data}->[SIZE] || 0;
}


sub last_size {
	my $self = shift;
	return $self->{data}->[LASTSIZE] || 0;
}


sub youngest_age {
	my $self = shift;
	if (@_ and defined $_[0] and $_[0] > 1) {
		$self->{data}->[AGE] = $_[0];
	}
	my $a = $self->{data}->[AGE];
	return $a if defined $a;
	return;
}


sub age {
	my $self = shift;
	# calculate current age in days
	my $a = $self->youngest_age;
	if (defined $a) {
		return sprintf("%.0f", (time - $a) / DAY);
	}
	return;
}


sub upload_age {
	my $self = shift;
	my $u = $self->upload_datestamp;
	if ($u > 1) {
		return sprintf("%.0f", (time - $u) / DAY);
	}
	return;
}


sub scan_datestamp {
	my $self = shift;
	if (@_) {
		$self->{data}->[SCAN] = $_[0];
	}
	return $self->{data}->[SCAN] || 0;
}


sub upload_datestamp {
	my $self = shift;
	if (@_) {
		$self->{data}->[UPLOAD] = $_[0];
	}
	return $self->{data}->[UPLOAD] || 0;
}


sub hidden_datestamp {
	my $self = shift;
	if (@_) {
		$self->{data}->[HIDDEN] = $_[0];
	}
	return $self->{data}->[HIDDEN] || 0;
}


sub hidden_age {
	my $self = shift;
	my $h = $self->hidden_datestamp;
	if ($h) {
		return sprintf("%.0f", (time - $h) / DAY);
	}
	return;
}

sub deleted_datestamp {
	my $self = shift;
	if (@_) {
		$self->{data}->[DELETED] = $_[0];
	}
	return $self->{data}->[DELETED] || 0;
}


sub emailed_datestamp {
	my $self = shift;
	if (@_) {
		$self->{data}->[EMAILED] = $_[0];
	}
	return $self->{data}->[EMAILED] || 0;
}

sub project_url {
	my $self = shift;
	my $b = $self->{data}->[BUCKET];
	my $p = $self->{data}->[PREFIX];
	return unless ( length $b and length $p );
	return sprintf("s3://%s/%s/", $b, $p );
}




sub print_string {
	my $self = shift;
	my $transform = shift || 0;
	
	# collect the data
	# following the same order as the stored array - see the constant hash above
	my @data = (
		$self->id,
		$self->path,
		$self->name,
		$self->date,
		$self->group,
		$self->user_email,
		$self->user_first,
		$self->user_last,
		$self->lab_first,
		$self->lab_last,
		$self->pi_email || q(),
		$self->core_lab || q(),
		$self->profile || q(),
		$self->bucket || q(),
		$self->prefix || q(),
		$self->external || q(),
		$self->request_status || q(),
		$self->request_application || q(),
		$self->organism || q(),
		$self->genome || q(),
		$self->size || q(),
		$self->last_size || q(),
		$self->youngest_age || q(),
		$self->scan_datestamp || q(),
		$self->upload_datestamp || q(),
		$self->hidden_datestamp || q(),
		$self->deleted_datestamp || q(),
		$self->emailed_datestamp || q(),
	);
	
	# transform posix times as necessary
	if ($transform) {
		
		# convert times from epoch to YYYYMMDD
		for my $i (SCAN, UPLOAD, HIDDEN, DELETED, EMAILED) {
			next unless (defined $data[$i] and $data[$i]);
			my @times = localtime($data[$i]);
			if ($times[5] == 69 or $times[5] == 70) {
				$data[$i] = q();
			}
			else {
				$data[$i] = sprintf("%04d-%02d-%02d", 
					$times[5] + 1900, $times[4] + 1, $times[3]);
			}
		}
		
		# express age in days
		$data[AGE] = sprintf("%d days", $self->age);
		
		# convert sizes
		for my $i (SIZE, LASTSIZE) {
			if ($data[$i] > 1000000000) {
				$data[$i] = sprintf("%.1fG", $data[$i] / 1000000000);
			}
			elsif ($data[$i] > 1000000) {
				$data[$i] = sprintf("%.1fM", $data[$i] / 1000000);
			}
			elsif ($data[$i] > 1000) {
				$data[$i] = sprintf("%.1fK", $data[$i] / 1000);
			}
			else {
				$data[$i] = sprintf("%dB", $data[$i]);
			}
		}
	}
	
	# return as tab-delimited string
	return sprintf("%s\n", join("\t", @data));
}


1;

__END__

=head1 NAME 

RepoCatalog - Indexed catalog database for HCI-Bio-Repository

=head1 DESCRIPTION

Maintains an indexed database file based on 
[DBM::Deep](https://metacpan.org/release/DBM-Deep) 
with essential information for all the projects in the Repository. 

Projects are indexed by their GNomEx ID, i.e. C<1234R> or C<A5678>.

The database file is organized as a hash of arrays. Each key is the 
GNomEx ID, and each value is an anonymous array. When iterating or 
querying the database file, a L<RepoEntry> object is returned for each 
database entry, i.e. GNomEx project. This object has functions to get/set 
specific values in the database entry. 

=head1 FUNCTIONS

=item new

Provide the path to an index file. If the file does not exist, a new 
index file will be generated for you.

B<NOTE> While L<DBM::Deep> database files can support multiple 
processes reading/writing, this is not fully implemented here, and 
multiple processes writing to the file may or may not work successfully.
You've been warned.

=item file

Returns the file path

=item db

Returns the low level L<DBM::Deep> object. 

=item entry

Provide a project ID. If an entry for the ID does not exist yet, 
a new one will be generated. Returns a L<RepoEntry> object. The 
object is tied to the database. Setting values using the functions 
of this object will be immediately written back to the database file.

=item delete_entry

Provide a project ID to remove.

=item optimize

Runs the L<DBM::Deep> C<optimize> function on the file.

=item export

Provide a path where the database file entries will be written as 
a tab-delimited text file. A header line is included.

=item header

Returns the standard header line used in printing and exporting 
files.

=back

=head1 NAME

RepoEntry - A Repository project entry in the catalog

=head1 DESCRIPTION

An object representing a project entry in the catalog. This is 
what users interact with working with the catalog. 

=head1 FUNCTIONS

Provides a number of get/set functions for the various fields for 
a Repository project entry. 

The timestamp fields return Unix epoch time, which must be converted 
to a human readable format. 

=over 4

=item is_request

=item id

=item path

=item name

=item date

=item group

=item user_email

=item user_first

=item user_last

=item lab_first

=item lab_last

=item pi_email

=item core_lab

=item profile

=item bucket

=item prefix

=item external

=item request_status

=item request_application

=item organism

=item genome

=item size

=item last_size

=item youngest_age

=item age

=item scan_datestamp

=item upload_datestamp

=item hidden_datestamp

=item deleted_datestamp

=item emailed_datestamp

=back


=head1 AUTHOR

 Timothy J. Parnell, PhD
 Bioinformatics Shared Resource
 Huntsman Cancer Institute
 University of Utah
 Salt Lake City, UT, 84112

This package is free software; you can redistribute it and/or modify
it under the terms of the Artistic License 2.0.  



