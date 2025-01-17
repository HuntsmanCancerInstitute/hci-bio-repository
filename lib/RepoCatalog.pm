package RepoCatalog;


use strict;
use English qw(-no_match_vars);
use Carp;
use IO::File;
use DBM::Deep;

our $VERSION = 7.2;


# General private values

my $DEFAULT_PATH  = "~/test/repository.db";
my $HEADER = "ID\tPath\tName\tDate\tGroup\tUserEmail\tUserFirst\tUserLast\tLabFirst\tLabLast\tPIEmail\tCORELab\tProfile\tBucket\tPrefix\tExternal\tStatus\tApplication\tOrganism\tGenome\tSize\tLastSize\tAge\tScan\tUpload\tHidden\tDeleted\tEmailed\tAAUpload\tQCScan\tAutoAnalysisFolder\n";
my $ARRAY_SIZE = 31;  # size of DB Entry array, see RepoEntry index list

# default search values
my $repo_epoch = 2005;
my $internal_org = qr/(?: Bioinformatics \s Shared \s Resource | HTG \s Core \s Facility | SYSTEM )/x;
my $req_up_min_size    = 26214400;  # minimum Request size to upload, 25 MB
my $req_up_min_age     = 0;  # minimum age for request upload
my $req_up_max_age     = 180;  # maximum age for request upload
my $req_hide_min_age   = 180;  # minimum age for hiding request
my $req_hide_max_age   = 100000;  # maximum age for hiding request
my $req_del_min_age    = 30;  # minimum age to delete hidden request
my $anal_up_min_age    = 360;  # minimum age for analysis upload
my $anal_up_max_age    = 100000;  # maximum age for analysis upload
my $anal_up_min_size   = 1024;  # minimum Analysis size to upload, 1 Kb
my $anal_hide_min_age  = 360;  # minimum age to hide analysis
my $anal_hide_max_age  = 100000;  # maximum age to hide analysis
my $anal_del_min_age   = 60;  # minimum age to delete hidden analysis

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
		if ( scalar @{ $data } != $ARRAY_SIZE ) {
			croak "Database first entry does not have $ARRAY_SIZE fields! Old database?";
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
		my @data = ( map { q() } (1 .. $ARRAY_SIZE) );
		$data[0] = $project;
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
			( $max_age ? ($E->age < $max_age) ? 1 : 0 : 1) and
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
		my $size; 
		{
			# use the biggest size available for checking
			my $a = $E->size || 0;
			my $b = $E->last_size || 0;
			$size = $a > $b ? $a : $b;
		}
		if (
			lc $E->lab_last eq $name and
			substr($E->date, 0, 4) >= $year and
			$E->age >= $min_age and
			( $max_age  ? $E->age < $max_age ? 1 : 0 : 1) and
			( $min_size ? $size >= $min_size  ? 1 : 0 : 1)
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
	
	# load table into file structure - this may be big
	my $i = 0;
	my %import;
	while (my $line = $fh->getline) {
		my @data = split /\t/, $line;
		unless (scalar @data == $ARRAY_SIZE) {
			$i++;
			croak " ! line $i does not have $ARRAY_SIZE fields!";
		}
		chomp $data[-1];
		my $id = $data[0];
		$import{$id} = \@data;
		$i++;
	}
	$fh->close;
	$self->{db}->import( \%import );
	return $i;
}



sub find_requests_to_upload {
	my $self = shift;
	my %opts = @_;
	my $year = (exists $opts{year} and defined $opts{year}) ? $opts{year} : $repo_epoch;
	my $min_size = (exists $opts{size} and $opts{size} =~ /^\d+$/) ? $opts{size} :
		$req_up_min_size;
	my $min_age = (exists $opts{age} and defined $opts{age}) ? $opts{age} :
		$req_up_min_age; 
	my $max_age = (exists $opts{maxage} and defined $opts{maxage}) ? $opts{maxage} :
		$req_up_max_age; 
	
	# scan through list
	my @list;
	my $key = $self->{db}->first_key;
	while ($key) {
		my $E = $self->entry($key);
		if (
			$E->is_request and
			$E->request_status eq 'COMPLETE' and
			$E->core_lab and 
			$E->scan_datestamp > 1 and
			$E->hidden_datestamp == 0 and
			$E->age > $min_age and
			$E->age < $max_age and
			$E->lab_last !~ $internal_org and
			substr($E->date, 0, 4) >= $year
		) {
			# we have a candidate
			# we could try to filter on application type, but there are so many 
			# possibilities that it's generally easier to screen for size
			# fortunately fastq files are big
			# too big, and we might miss small MiSeq projects
			# too little, and we might include large sample-quality projects
			if ($E->size > $min_size) {
				# size is bigger than 25 MB, looks like a candidate
				if ($E->upload_datestamp > 1) {
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
	my $min_age = (exists $opts{age} and $opts{age} =~ /^\d+$/) ? $opts{age} :
		$req_hide_min_age;
	my $max_age = (exists $opts{maxage} and $opts{maxage} =~ /^\d+$/) ? $opts{maxage} :
		$req_hide_max_age;
	my $ext  = (exists $opts{external} and $opts{external}) ? $opts{external} : 'N';
	my $min_size = (exists $opts{size} and $opts{size} =~ /^\d+$/) ? $opts{size} :
		$req_up_min_size;  # same size as upload
	
	# scan through list
	my @list;
	my $key = $self->{db}->first_key;
	while ($key) {
		my $E = $self->entry($key);
		if (
			$E->is_request and
			$E->request_status eq 'COMPLETE' and        # finished
			$E->scan_datestamp > 1 and                  # scanned
			$E->hidden_datestamp == 0 and               # not hidden yet
			$E->size > $min_size and                    # size > minimum size
			$E->age > $min_age and                      # older than minimum age
			$E->age < $max_age and                      # less than maximum age
			$E->lab_last !~ $internal_org and           # not internal lab
			substr($E->date, 0, 4) >= $year             # older than start
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
	my $min_age = (exists $opts{age} and $opts{age} =~ /^\d+$/) ? $opts{age} :
		$req_del_min_age;
	my $max_age = (exists $opts{maxage} and $opts{maxage} =~ /^\d+$/) ? $opts{maxage} :
		$req_hide_max_age;
	my $ext  = (exists $opts{external} and $opts{external}) ? $opts{external} : 'N';
	
	# scan through list
	my @list;
	my $key = $self->{db}->first_key;
	while ($key) {
		my $E = $self->entry($key);
		if (
			$E->is_request and
			$E->request_status eq 'COMPLETE' and        # finished
			$E->hidden_datestamp > 1 and                # hidden
			$E->deleted_datestamp < 1 and               # not yet deleted
			$E->hidden_age > $min_age and               # hidden for minimum time
			$E->lab_last !~ $internal_org and           # not internal lab
			substr($E->date, 0, 4) >= $year             # current year
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
	my $min_age = (exists $opts{age} and $opts{age} =~ /^\d+$/) ? $opts{age} :
		$anal_up_min_age;
	my $max_age = (exists $opts{maxage} and $opts{maxage} =~ /^\d+$/) ? $opts{maxage} :
		$anal_up_max_age;
	my $min_size = (exists $opts{size} and $opts{size} =~ /^\d+$/) ? $opts{size} :
		$anal_up_min_size;
	
	# scan through list
	my @list;
	my $key = $self->{db}->first_key;
	while ($key) {
		my $E = $self->entry($key);
		if (
			not $E->is_request and
			$E->core_lab and                            # has division
			$E->hidden_datestamp == 0 and               # not already hidden
			$E->size > $min_size and                    # size > minimum
			$E->age > $min_age and                      # older than min age
			$E->age < $max_age and                      # less than max age
			$E->lab_last !~ $internal_org and           # not internal lab
			substr($E->date, 0, 4) >= $year             # current year
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
	my $min_age = (exists $opts{age} and $opts{age} =~ /^\d+$/) ? $opts{age} :
		$anal_hide_min_age;
	my $max_age = (exists $opts{maxage} and $opts{maxage} =~ /^\d+$/) ? $opts{maxage} :
		$anal_hide_max_age;
	my $ext  = (exists $opts{external} and $opts{external}) ? $opts{external} : 'N';
	my $min_size = (exists $opts{size} and $opts{size} =~ /^\d+$/) ? $opts{size} :
		$anal_up_min_size;
	
	# scan through list
	my @list;
	my $key = $self->{db}->first_key;
	while ($key) {
		my $E = $self->entry($key);
		if (
			not $E->is_request and
			$E->hidden_datestamp < 1 and                # not already hidden
			$E->size > $min_size and                    # size > minimum
			$E->age > $min_age and                      # older than min age
			$E->age < $max_age	and                     # less than max age
			substr($E->date, 0, 4) >= $year and         # current year
			$E->lab_last !~ $internal_org               # not internal lab
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
	my $min_age = (exists $opts{age} and $opts{age} =~ /^\d+$/) ? $opts{age} :
		$anal_del_min_age;
	my $max_age = (exists $opts{maxage} and $opts{maxage} =~ /^\d+$/) ? $opts{maxage} :
		$anal_up_max_age;
	my $ext  = (exists $opts{external} and $opts{external}) ? $opts{external} : 'N';
	my $min_size = (exists $opts{size} and $opts{size} =~ /^\d+$/) ? $opts{size} : 0;
	
	# scan through list
	my @list;
	my $key = $self->{db}->first_key;
	while ($key) {
		my $E = $self->entry($key);
		if (
			not $E->is_request and
			$E->hidden_datestamp > 1 and                # hidden
			$E->deleted_datestamp == 0 and              # not yet deleted
			$E->hidden_age > $min_age and               # hidden for min number days
			$E->hidden_age < $max_age and               # hidden for max number days
			$E->lab_last !~ $internal_org and           # not hidden lab
			substr($E->date, 0, 4) >= $year             # current year
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

sub find_autoanal_req {
	my $self = shift;
	my %opts = @_;
	my $year = (exists $opts{year} and defined $opts{year}) ? $opts{year} : $repo_epoch;
	my $min_age = (exists $opts{age} and defined $opts{age}) ? $opts{age} :
		$req_up_min_age; 
	my $max_age = (exists $opts{maxage} and defined $opts{maxage}) ? $opts{maxage} :
		$req_up_max_age; 

	# scan through list
	my @list;
	my $key = $self->{db}->first_key;
	while ($key) {
		my $E = $self->entry($key);
		if (
			$E->is_request and
			$E->autoanal_folder and
			$E->hidden_datestamp == 0 and 
			$E->age > $min_age and
			$E->age < $max_age and
			substr($E->date, 0, 4) >= $year and
			$E->lab_last !~ $internal_org
		) {
			push @list, $key;
		}
		$key = $self->{db}->next_key($key);
	}
	@list = sort {$a cmp $b} @list;
	return wantarray ? @list : \@list;
}

sub find_autoanal_to_upload {
	my $self = shift;
	my %opts = @_;
	my $year = (exists $opts{year} and defined $opts{year}) ? $opts{year} : $repo_epoch;
	my $min_age = (exists $opts{age} and defined $opts{age}) ? $opts{age} :
		$req_hide_min_age; 
	my $max_age = (exists $opts{maxage} and defined $opts{maxage}) ? $opts{maxage} :
		$req_hide_max_age; 
	
	# scan through list
	my @list;
	my $key = $self->{db}->first_key;
	while ($key) {
		my $E = $self->entry($key);
		if (
			$E->is_request and
			$E->request_status eq 'COMPLETE' and
			$E->autoanal_folder and
			$E->core_lab and 
			$E->hidden_datestamp == 0 and
			$E->age > $min_age and
			$E->age < $max_age and
			$E->lab_last !~ $internal_org and
			substr($E->date, 0, 4) >= $year
		) {
			
			# we have a candidate
			# going to assume the autoanalysis folder, if it is present, is not empty
			if ( $E->autoanal_up_datestamp > 1 ) {
				# it has been uploaded before? check the age
				# this doesn't guarantee that the differences are in the autoanalysis
				# folder, just that something is younger in the folder
				if ( $E->autoanal_upload_age > $E->age ) {
					push @list, $key;
				}
			}
			else {
				# has not been uploaded yet
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
	AAUPLOAD    => 28,    # unix timestamp for Request AutoAnalysis upload
	QCSCAN      => 29,    # unix timestamp for request QC folder scan
	AAFOLD      => 30,    # Request AutoAnalysis folder name
	DAY         => 86400, # 60 seconds * 60 minutes * 24 hours
	KB          => 1024,  # binary size prefixes
	MB          => 1048576,
	GB          => 1073741824,
	TB          => 1099511627776,
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
		my $cursize = $self->{data}->[SIZE] || 0;
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


sub youngest_datestamp {
	my $self = shift;
	if (@_ and defined $_[0] and $_[0] > 1) {
		$self->{data}->[AGE] = $_[0];
	}
	my $a = $self->{data}->[AGE];
	return $a if defined $a;
	return -1;
}


sub age {
	my $self = shift;
	# calculate current age in days
	my $a = $self->youngest_datestamp;
	if (defined $a and $a > 1) {
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

sub autoanal_up_datestamp {
	my $self = shift;
	if (@_) {
		$self->{data}->[AAUPLOAD] = $_[0];
	}
	return $self->{data}->[AAUPLOAD] || 0;
}

sub autoanal_upload_age {
	my $self = shift;
	my $u = $self->autoanal_up_datestamp;
	if ($u > 1) {
		return sprintf("%.0f", (time - $u) / DAY);
	}
	return;
}

sub qc_scan_datestamp {
	my $self = shift;
	if (@_) {
		$self->{data}->[QCSCAN] = $_[0];
	}
	return $self->{data}->[QCSCAN] || 0;
}

sub autoanal_folder {
	my $self = shift;
	if (@_) {
		$self->{data}->[AAFOLD] = $_[0];
	}
	return $self->{data}->[AAFOLD];
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
		$self->youngest_datestamp || 0,
		$self->scan_datestamp || 0,
		$self->upload_datestamp || 0,
		$self->hidden_datestamp || 0,
		$self->deleted_datestamp || 0,
		$self->emailed_datestamp || 0,
		$self->autoanal_up_datestamp || 0,
		$self->qc_scan_datestamp || 0,
		$self->autoanal_folder || q()
	);
	
	# transform posix times as necessary
	if ($transform) {
		
		# convert times from epoch to YYYYMMDD
		for my $i (SCAN, UPLOAD, HIDDEN, DELETED, EMAILED, AAUPLOAD, QCSCAN) {
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
			if ($data[$i] > TB) {
				$data[$i] = sprintf("%.1fT", $data[$i] / TB);
			}
			elsif ($data[$i] > GB) {
				$data[$i] = sprintf("%.1fG", $data[$i] / GB);
			}
			elsif ($data[$i] > MB) {
				$data[$i] = sprintf("%.1fM", $data[$i] / MB);
			}
			elsif ($data[$i] > 1000) {
				$data[$i] = sprintf("%.1fK", $data[$i] / KB);
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

=item export_to_file

Provide a path where the database file entries will be written as 
a tab-delimited text file. A header line is included. Provide a 
second boolean value to transform date stamp values into human readable
dates and sizes from bytes into short values with suffix (K, M, G, T).
B<NOTE> that transformed files should not be used for import.

=item import_from_file

Import a tab-delimited text file into the database. Provide the path
to the file as the first argument. The header line must exist and must
match the internal header format. If the catalog database file is not 
new, i.e. it contains data, then the file will not be loaded. A second,
true boolean value must be provided to force the file to be loaded and
overwrite any existing data. A warning will be given.

=item header

Returns the standard header line used in printing and exporting 
files.

=back

=head2 Catalog search functions

These are functions for searching for entries in the catalog database. 

=over 4

=item list_all

A general function to list all projects. Pass an array of key value pairs 
for filtering. Possible keys include

=over 4

=item year - include projects in this year or later

=item core - boolean to include projects with a AWS CORE account

=item age - minimum age in days to include

=item maxage - exclude projects older than this in days

=item external - boolean to include projects marked as external user

=item size - minimum size of the project in bytes to include

=back

=item list_projects_for_pi

List projects for a specific Principal Investigator. Pass an array of key
value pairs in the same manner as L<list_all>. Include the last name of 
the PI as the value to the C<name> key. Alternatively, just simply pass a 
single value of the last name. The name matching is case insensitive.

=item find_requests_to_upload

Canned search function to find Experiment Request projects that are ready 
to upload to AWS. Modified search values can be provided by passing an array
of key value pairs as described in L<list_all>.

=item find_requests_to_hide

Canned search function to find Experiment Request projects that are ready 
to hide. Modified search values can be provided by passing an array of
key value pairs as described in L<list_all>.

=item find_requests_to_delete

Canned search function to find Experiment Request projects that are ready 
to delete. Modified search values can be provided by passing an array of
key value pairs as described in L<list_all>.

=item find_analysis_to_upload

Canned search function to find Analysis projects that are ready 
to upload to AWS. Modified search values can be provided by passing an array
of key value pairs as described in L<list_all>.

=item find_analysis_to_hide

Canned search function to find Experiment Request projects that are ready 
to hide. Modified search values can be provided by passing an array of
key value pairs as described in L<list_all>.

=item find_analysis_to_delete

Canned search function to find Experiment Request projects that are ready 
to delete. Modified search values can be provided by passing an array of
key value pairs as described in L<list_all>.

=item find_autoanal_req

Canned search function to find Experiment Requests that have an AutoAnalysis
folder. Additional search values can be provided by passing an array of
key value pairs as described in L<list_all>.

=item find_autoanal_to_upload

Canned search function to find Experiment Requests with an AutoAnalysis
folder that needs to be uploaded to AWS prior to hiding. Modified search 
values can be provided by passing an array of key value pairs as described 
in L<list_all>.

=back

=head1 NAME

RepoEntry - A Repository project entry in the catalog

=head1 DESCRIPTION

An object representing a project entry in the catalog. This is 
what users interact with working with the catalog. 

=head1 FUNCTIONS

Provides a number of get/set functions for the various fields for 
a Repository project entry. Most are self-explanatory. Call the function
to return the value. Pass a value to set the field.

The timestamp fields return Unix epoch time, which must be converted 
to a human readable format. 

The age functions return the difference in days between the current time
and the recorded date time stamp.

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

=item youngest_datestamp

=item age

=item scan_datestamp

=item upload_datestamp

=item hidden_datestamp

=item deleted_datestamp

=item emailed_datestamp

=itme autoanal_up_datestamp

=item autoanal_upload_age

=item qc_scan_datestamp

=item autoanal_folder

=back


=head1 AUTHOR

 Timothy J. Parnell, PhD
 Bioinformatics Shared Resource
 Huntsman Cancer Institute
 University of Utah
 Salt Lake City, UT, 84112

This package is free software; you can redistribute it and/or modify
it under the terms of the Artistic License 2.0.  



