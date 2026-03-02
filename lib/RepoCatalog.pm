package RepoCatalog;


use strict;
use English qw(-no_match_vars);
use Carp;
use IO::File;
use DBM::Deep;
use constant {
	LAB_UPLOAD  => 1,     # Boolean whether uploads allowed
	LAB_ACCT    => 2,     # CORE lab account name
};

our $VERSION = 'v9.0.0';


### General private values

# Catalog definition
my $HEADER = "ID\tPath\tName\tDate\tGroup\tUserEmail\tUserFirst\tUserLast\tLabFirst\tLabLast\tCORELab\tBucket\tPrefix\tExternal\tStatus\tApplication\tOrganism\tGenome\tSize\tLastSize\tAge\tScan\tUpload\tHidden\tDeleted\tEmailed\tAAUpload\tQCScan\tAutoAnalysisFolder\n";
my $ARRAY_SIZE = 29;  # size of DB Entry array, see RepoEntry index list
my $REQUIRED_INTS = q(18 19 20 21 22 23 24 25 26 27);

my $LAB_INFO_HEADER = "Name\tEmail\tAllow Upload\tCORE Lab\n";
my $LAB_SIZE        = 4;
my $ACCOUNT_HEADER  = "CORE_Lab\tProfile\tAWS_Account\n";
my $ACCOUNT_SIZE    = 3;

# default search values
my $repo_epoch = 2005;
my $internal_org = qr/(?: Bioinformatics \s Shared \s Resource | HTG \s Core \s Facility | SYSTEM )/x;
my $req_up_min_size    = 26214400;  # minimum Request size to upload, 25 MB
my $req_up_min_age     = 0;         # minimum age for request upload
my $req_up_max_age     = 730;       # maximum age for request upload
my $req_hide_min_age   = 180;       # minimum age for hiding request
my $req_hide_max_age   = 100000;    # maximum age for hiding request
my $req_del_min_age    = 30;        # minimum age to delete hidden request
my $anal_up_min_age    = 360;       # minimum age for analysis upload
my $anal_up_max_age    = 100000;    # maximum age for analysis upload
my $anal_up_min_size   = 1024;      # minimum Analysis size to upload, 1 Kb
my $anal_hide_min_age  = 360;       # minimum age to hide analysis
my $anal_hide_max_age  = 100000;    # maximum age to hide analysis
my $anal_del_min_age   = 60;        # minimum age to delete hidden analysis

# Functions

sub new {
	my $class = shift;
	my $path  = shift;
	unless ($path) {
		croak "FATAL: No database file provided!!";
	}
	
	# open the database file
	my $db;
	if (-e $path) {
		$db = DBM::Deep->new($path) or 
			croak "FATAL: unable to open database file '$path'! $OS_ERROR";

		# check if current version
		my $head = $db->get('HEADER') || undef;
		my $headstr;
		if ($head) {
			$headstr = sprintf "%s\n", join( "\t", @{$head} );
		}
		unless ( $head and scalar( @{$head} ) == $ARRAY_SIZE and $headstr eq $HEADER ) {
			print " Incorrect version catalog!!!\n This is likely an old version.\n";
			print " Update an old exported table file and import into a new catalog\n";
			print " Current header is the following:\n$HEADER";
			croak "ERROR";
		}
	}
	else {
		# make a new database file
		# use the DBM::Deep defaults which use a file backing and hash format
		$db = DBM::Deep->new(
			file     => $path,
		) or croak "FATAL: unable to initialize database file '$path'! $OS_ERROR";
		
		# default entries
		my @header = split /\t/, $HEADER;
		chomp $header[-1];
		$db->put( 'HEADER', \@header );
		my $lab = { 'default' => [ map { q() } (1 .. $LAB_SIZE) ] };
		$db->put( 'LABS', $lab );
		my $acct = { 'default' => [ map { q() } (1 .. $ACCOUNT_SIZE) ] };
		$db->put( 'ACCOUNTS', $acct );
	}
		
	# return
	my $lab = $db->get('LABS');
	my $act = $db->get('ACCOUNTS');
	my $self = {
		file    => $path,
		db      => $db,
		lab     => $lab,
		account => $act
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
	my ( $self, $project ) = @_;
	confess "no project provided!" unless defined $project;
	if ( $self->{db}->exists($project) ) {

		# return existing project
		return RepoEntry->new(
			$self->{db}->get($project),
			$self->{lab},
			$self->{account}
		);
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
		return RepoEntry->new(
			$self->{db}->get($project),
			$self->{lab},
			$self->{account}
		);
	}
	else {
		# make a new entry
		# the project ID is always the first element in the array
		my @data = ( map { q() } (1 .. $ARRAY_SIZE) );
		$data[0] = $project;
		foreach my $n (split /\s/, $REQUIRED_INTS) {
			$data[$n] = 0;
		}
		my $p = $self->{db}->put($project, \@data);
		if ($p) {
			return RepoEntry->new(
				$self->{db}->get($project),
				$self->{lab},
				$self->{account}
			);
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
	my $email = exists $opts{emailed} ? $opts{emailed} : -1;
	
	# scan through list
	my @list;
	my $key = $self->{db}->first_key;
	
	while ($key) {

		# skip metadata keys
		if ( $key eq 'HEADER' or $key eq 'LABS' or $key eq 'ACCOUNTS' ) {
			$key = $self->{db}->next_key($key);
			next;
		}
		
		# process catalog entries
		my $E = $self->entry($key);
		if (
			substr($E->date, 0, 4) >= $year and
			$E->age >= $min_age and
			( $max_age ? ($E->age < $max_age) ? 1 : 0 : 1) and
			( $min_size ? ($E->size >= $min_size) ? 1 : 0 : 1) and
			( ( $email == -1 ) or ( $email == 0 && $E->emailed_datestamp == 0 ) or
				( $email == 1 && $E->emailed_datestamp > 1 ) )
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
	my $sorted = $self->sort_list(\@list);
	return wantarray ? @{ $sorted } : $sorted;
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
	my $email = exists $opts{emailed} ? $opts{emailed} : -1;
	
	# scan through list
	my @list;
	my $key = $self->{db}->first_key;
	while ($key) {

		# skip metadata keys
		if ( $key eq 'HEADER' or $key eq 'LABS' or $key eq 'ACCOUNTS' ) {
			$key = $self->{db}->next_key($key);
			next;
		}
		
		# process catalog entries
		my $E = $self->entry($key);

		# calculate size
		my $size; 
		{
			# use the biggest size available for checking
			my $a = $E->size || 0;
			my $b = $E->last_size || 0;
			$size = $a > $b ? $a : $b;
		}

		# check lab name, date, age, size
		if (
			lc $E->lab_last eq $name and
			substr($E->date, 0, 4) >= $year and
			$E->age >= $min_age and
			( $max_age  ? $E->age < $max_age ? 1 : 0 : 1) and
			( $min_size ? $size >= $min_size  ? 1 : 0 : 1) and
			( ( $email == -1 ) or ( $email == 0 && $E->emailed_datestamp == 0 ) or
				( $email == 1 && $E->emailed_datestamp > 1 ) )
		) {
			push @list, $key;
		}
		$key = $self->{db}->next_key($key);
	}
	my $sorted = $self->sort_list(\@list);
	return wantarray ? @{ $sorted } : $sorted;
}

sub export_to_file {
	my ($self, $file, $transform) = @_;
	croak "no output file provided!" unless defined $file;
	$transform ||= 0;
	my $fh = IO::File->new($file, '>') or 
		croak "unable to open $file for writing! $OS_ERROR\n";
	$fh->binmode(':utf8');
	$fh->print($HEADER);
	
	# generate list and sort
	my @list;
	my $key = $self->{db}->first_key;
	while ($key) {
		unless ( $key eq 'HEADER' or $key eq 'LABS' or $key eq 'ACCOUNTS' ) {
			push @list, $key;
		}
		$key = $self->{db}->next_key($key);
	}
	my $sorted = $self->sort_list(\@list);
	
	# iterate and export as a tab-delimited file
	foreach my $key ( @{$sorted} ) {
		my $E = $self->entry($key);
		my $string = $E->print_string($transform);
		$fh->print($string);
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
		my $n = scalar( split /\t/, $firstline );
		print " Import file '$file' doesn't match the expected format!!\n";
		print " Expecting $ARRAY_SIZE elements and this has $n elements\n";
		print " Current header is the following:\n$HEADER";
		print "\n Please update the table.\n";
		croak "FAILURE to import!\n";
	}
	
	# check catalog
	my $count = 0;
	my $key = $self->{db}->first_key || undef;
	while ($key) {
		$count++;
		$key = $self->{db}->next_key($key);
	}
	if ($count > 3) {
		# even a new catalog now has 3 default keys
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
	my @check = split /\s/, $REQUIRED_INTS;
	my $i = 1;
	my %import;
	while (my $line = $fh->getline) {
		my @data = split /\t/, $line;
		unless (scalar @data == $ARRAY_SIZE) {
			$i++;
			croak " ! lab information line $i does not have $ARRAY_SIZE fields!\n";
		}
		foreach my $n (@check) {
			if ($data[$n] =~ /\d+/) {
				$data[$n] = int $data[$n];
			}
			else {
				$data[$n] = 0;
			}
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

sub import_labs {
	my $self = shift;
	my $file = shift;
	croak "no lab information file provided!\n" unless defined $file;
	my $fh = IO::File->new($file, '<') or 
		croak "unable to open $file for reading! $OS_ERROR\n";
	my $firstline = $fh->getline;
	unless ( $firstline eq $LAB_INFO_HEADER ) {
		croak "header doesn't match an expected file format! Should be\n$LAB_INFO_HEADER\n";
	}
	my %lab;
	my $i = 1;
	while (my $line = $fh->getline) {
		my @data = split /\t/, $line;
		unless (scalar @data == $LAB_SIZE) {
			$i++;
			croak " ! account information line $i does not have $LAB_SIZE fields!\n";
		}
		chomp $data[-1];
		my $name = shift @data;
		$lab{$name} = \@data;
		$i++;
	}
	$fh->close;
	$self->{db}->put( 'LABS', \%lab );
	return $i;
}

sub import_accounts {
	my $self = shift;
	my $file = shift;
	croak "no account information file provided!\n" unless defined $file;
	my $fh = IO::File->new($file, '<') or 
		croak "unable to open $file for reading! $OS_ERROR\n";
	my $firstline = $fh->getline;
	unless ( $firstline eq $ACCOUNT_HEADER ) {
		croak "header doesn't match an expected file format! Should be\n$ACCOUNT_HEADER\n";
	}
	my %acct;
	my $i = 0;
	while (my $line = $fh->getline) {
		my @data = split /\t/, $line;
		unless (scalar @data == $ACCOUNT_SIZE) {
			$i++;
			croak " ! line $i does not have $ACCOUNT_SIZE fields!";
		}
		
		# check for a valid profile name and store in hash
		next unless $data[1] =~ /[a-z]+/i;
		chomp $data[-1];
		my $name = shift @data;
		$acct{$name} = \@data;
		$i++;
	}
	$fh->close;
	$self->{db}->put( 'ACCOUNTS', \%acct );
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
	my $email = exists $opts{emailed} ? $opts{emailed} : -1;
	
	# scan through list
	my @list;
	my $key = $self->{db}->first_key;
	while ($key) {

		# skip metadata keys
		if ( $key eq 'HEADER' or $key eq 'LABS' or $key eq 'ACCOUNTS' ) {
			$key = $self->{db}->next_key($key);
			next;
		}
		
		# process catalog entries
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
			substr($E->date, 0, 4) >= $year and
			( ( $email == -1 ) or ( $email == 0 && $E->emailed_datestamp == 0 ) or
				( $email == 1 && $E->emailed_datestamp > 1 ) )
		) {
			# we have a candidate
			# we could try to filter on application type, but there are so many 
			# possibilities that it's generally easier to screen for size
			# fortunately fastq files are big
			# too big, and we might miss small MiSeq projects
			# too little, and we might include large sample-quality projects
			if ($E->size > $min_size) {
				# size is bigger than minimum size, looks like a candidate
				if ($E->upload_datestamp > 1) {
					# already been uploaded
					if ( $E->upload_age > $E->age ) {
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
	my $sorted = $self->sort_list(\@list);
	return wantarray ? @{ $sorted } : $sorted;
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
	my $email = exists $opts{emailed} ? $opts{emailed} : -1;
	
	# scan through list
	my @list;
	my $key = $self->{db}->first_key;
	while ($key) {

		# skip metadata keys
		if ( $key eq 'HEADER' or $key eq 'LABS' or $key eq 'ACCOUNTS' ) {
			$key = $self->{db}->next_key($key);
			next;
		}
		
		# process catalog entries
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
			substr($E->date, 0, 4) >= $year and         # older than start
			( ( $email == -1 ) or ( $email == 0 && $E->emailed_datestamp == 0 ) or
				( $email == 1 && $E->emailed_datestamp > 1 ) )
		) {
			# we have a possible candidate
			if (defined $core) {
				if ($core and $E->core_lab and $E->upload_datestamp > 1) {
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
	my $sorted = $self->sort_list(\@list);
	return wantarray ? @{ $sorted } : $sorted;
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
	my $email = exists $opts{emailed} ? $opts{emailed} : -1;
	
	# scan through list
	my @list;
	my $key = $self->{db}->first_key;
	while ($key) {

		# skip metadata keys
		if ( $key eq 'HEADER' or $key eq 'LABS' or $key eq 'ACCOUNTS' ) {
			$key = $self->{db}->next_key($key);
			next;
		}
		
		# process catalog entries
		my $E = $self->entry($key);
		if (
			$E->is_request and
			$E->request_status eq 'COMPLETE' and        # finished
			$E->hidden_datestamp > 1 and                # hidden
			$E->deleted_datestamp < 1 and               # not yet deleted
			$E->hidden_age > $min_age and               # hidden for minimum time
			$E->lab_last !~ $internal_org and           # not internal lab
			substr($E->date, 0, 4) >= $year and         # current year
			( ( $email == -1 ) or ( $email == 0 && $E->emailed_datestamp == 0 ) or
				( $email == 1 && $E->emailed_datestamp > 1 ) )
		) {
			# we have a possible candidate
			# check core status
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
	my $sorted = $self->sort_list(\@list);
	return wantarray ? @{ $sorted } : $sorted;
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
	my $email = exists $opts{emailed} ? $opts{emailed} : -1;
	
	# scan through list
	my @list;
	my $key = $self->{db}->first_key;
	while ($key) {

		# skip metadata keys
		if ( $key eq 'HEADER' or $key eq 'LABS' or $key eq 'ACCOUNTS' ) {
			$key = $self->{db}->next_key($key);
			next;
		}
		
		# process catalog entries
		my $E = $self->entry($key);
		if (
			not $E->is_request and
			$E->core_lab and                            # has division
			$E->hidden_datestamp == 0 and               # not already hidden
			$E->size > $min_size and                    # size > minimum
			$E->age > $min_age and                      # older than min age
			$E->age < $max_age and                      # less than max age
			$E->lab_last !~ $internal_org and           # not internal lab
			substr($E->date, 0, 4) >= $year and         # current year
			( ( $email == -1 ) or ( $email == 0 && $E->emailed_datestamp == 0 ) or
				( $email == 1 && $E->emailed_datestamp > 1 ) )
		) {
			# we have a candidate
			push @list, $key;
		}
		$key = $self->{db}->next_key($key);
	}
	my $sorted = $self->sort_list(\@list);
	return wantarray ? @{ $sorted } : $sorted;
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
	my $email = exists $opts{emailed} ? $opts{emailed} : -1;
	
	# scan through list
	my @list;
	my $key = $self->{db}->first_key;
	while ($key) {

		# skip metadata keys
		if ( $key eq 'HEADER' or $key eq 'LABS' or $key eq 'ACCOUNTS' ) {
			$key = $self->{db}->next_key($key);
			next;
		}
		
		# process catalog entries
		my $E = $self->entry($key);
		if (
			not $E->is_request and
			$E->hidden_datestamp < 1 and                # not already hidden
			$E->size > $min_size and                    # size > minimum
			$E->age > $min_age and                      # older than min age
			$E->age < $max_age	and                     # less than max age
			substr($E->date, 0, 4) >= $year and         # current year
			$E->lab_last !~ $internal_org and           # not internal lab
			( ( $email == -1 ) or ( $email == 0 && $E->emailed_datestamp == 0 ) or
				( $email == 1 && $E->emailed_datestamp > 1 ) )
		) {
			# we have a possible candidate
			# check core status
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
	my $sorted = $self->sort_list(\@list);
	return wantarray ? @{ $sorted } : $sorted;
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
	my $email = exists $opts{emailed} ? $opts{emailed} : -1;
	
	# scan through list
	my @list;
	my $key = $self->{db}->first_key;
	while ($key) {

		# skip metadata keys
		if ( $key eq 'HEADER' or $key eq 'LABS' or $key eq 'ACCOUNTS' ) {
			$key = $self->{db}->next_key($key);
			next;
		}
		
		# process catalog entries
		my $E = $self->entry($key);
		if (
			not $E->is_request and
			$E->hidden_datestamp > 1 and                # hidden
			$E->deleted_datestamp == 0 and              # not yet deleted
			$E->hidden_age > $min_age and               # hidden for min number days
			$E->hidden_age < $max_age and               # hidden for max number days
			$E->lab_last !~ $internal_org and           # not hidden lab
			substr($E->date, 0, 4) >= $year and         # current year
			( ( $email == -1 ) or ( $email == 0 && $E->emailed_datestamp == 0 ) or
				( $email == 1 && $E->emailed_datestamp > 1 ) )
		) {
			# we have a possible candidate
			# check core status
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
	my $sorted = $self->sort_list(\@list);
	return wantarray ? @{ $sorted } : $sorted;
}

sub find_autoanal_req {
# !!!!! This needs a core lab option!!!!!
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

		# skip metadata keys
		if ( $key eq 'HEADER' or $key eq 'LABS' or $key eq 'ACCOUNTS' ) {
			$key = $self->{db}->next_key($key);
			next;
		}
		
		# process catalog entries
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
	my $sorted = $self->sort_list(\@list);
	return wantarray ? @{ $sorted } : $sorted;
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

		# skip metadata keys
		if ( $key eq 'HEADER' or $key eq 'LABS' or $key eq 'ACCOUNTS' ) {
			$key = $self->{db}->next_key($key);
			next;
		}
		
		# process catalog entries
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
	my $sorted = $self->sort_list(\@list);
	return wantarray ? @{ $sorted } : $sorted;
}

sub header {
	return $HEADER;
}

sub sort_list {
	my ($self, $unsorted) = @_;
	my @list;
	foreach my $i ( @{ $unsorted } ) {
		if ($i =~ /(\d+)R/) {
			push @list, [ 'R', $1, $i ];
		}
		elsif ($i =~ /A(\d+)/) {
			push @list, [ 'A', $1, $i ];
		}
		elsif ($i =~ /(\d+)/) {
			push @list, [ 'X', $1, $i ];
		}
		else {
			push @list, [ 'Z', 0, $i ];
		}
	}
	my @s = map { $_->[2] }
			sort { $a->[0] cmp $b->[0] or $a->[1] <=> $b->[1] or $a->[2] cmp $b->[2] }
			@list;
	return \@s;
}

sub check_lab {
	my $self = shift;
	my $name = shift || undef;
	return 0 unless $name;
	if ( ref($name) eq 'RepoEntry' ) {
		$name = sprintf "%s %s", $name->lab_first, $name->lab_last;
	}
	if ( exists $self->{lab}->{$name} ) {
		return 1;
	}
	else {
		return 0;
	}
}

sub allow_upload {
	my $self = shift;
	my $name = shift || undef;
	return unless $name;
	if ( ref($name) eq 'RepoEntry' ) {
		$name = sprintf "%s %s", $name->lab_first, $name->lab_last;
	}
	if ( exists $self->{lab}->{$name} ) {
		my $v = $self->{lab}->{$name}->[LAB_UPLOAD];
		if ($v eq 'Y') {
			return 1;
		}
		elsif ($v eq 'N') {
			return 0;
		}
		else {
			print " ! lab '$name' has an invalid upload response '$v'\n";
			return 0;
		}
	}
	else {
		return 0;
	}
}

sub get_upload_account {
	my $self = shift;
	my $name = shift || undef;
	return unless $name;
	if ( ref($name) eq 'RepoEntry' ) {
		$name = sprintf "%s %s", $name->lab_first, $name->lab_last;
	}
	if ( exists $self->{lab}->{$name} ) {
		my $v = $self->{lab}->{$name}->[LAB_UPLOAD];
		if ($v eq 'Y') {
			return $self->{lab}->{$name}->[LAB_ACCT];
		}
		else {
			return;
		}
	}
	else {
		return;
	}
}


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
	CORELAB     => 10,    # name of CORE lab
	BUCKET      => 11,    # s3 bucket name
	PREFIX      => 12,    # s3 prefix
	EXTERNAL    => 13,    # y or n boolean
	STATUS      => 14,    # gnomex request status
	APPLICATION => 15,    # gnomex request application type
	ORGANISM    => 16,    # gnomex organism string
	GENOME      => 17,    # gnomex genome build
	SIZE        => 18,    # current project file size total in bytes
	LASTSIZE    => 19,    # previous project file size total in bytes
	AGE         => 20,    # unix timestamp for youngest observed file in project
	SCAN        => 21,    # unix timestamp for scanning
	UPLOAD      => 22,    # unix timestamp for uploading
	HIDDEN      => 23,    # unix timestamp for hiding
	DELETED     => 24,    # unix timestamp for deleting
	EMAILED     => 25,    # unix timestamp for emailing
	AAUPLOAD    => 26,    # unix timestamp for Request AutoAnalysis upload
	QCSCAN      => 27,    # unix timestamp for request QC folder scan
	AAFOLD      => 28,    # Request AutoAnalysis folder name
	LAB_EMAIL   => 0,     # Email address for the PI
	LAB_UPLOAD  => 1,     # Boolean whether uploads allowed
	LAB_ACCT    => 2,     # CORE lab account name
	ACT_PROFILE => 0,     # service account profile name for accessing account
	ACT_NUMBER  => 1,     # AWS account number used for lab uploads
	DAY         => 86400, # 60 seconds * 60 minutes * 24 hours
	KB          => 1024,  # binary size prefixes
	MB          => 1048576,
	GB          => 1073741824,
	TB          => 1099511627776,
};
my $BASE_URL = 'https://hci-apps-ext.hci.utah.edu/core-browser';

sub new {
	my ($class, $data, $lab, $account) = @_;
	if (ref($data) !~ /DBM..Deep/) {
		confess "not a DBM::Deep reference!";
		return;
	}
	my $self = {
		data    => $data,
		lab     => $lab,
		account => $account
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
		carp 'pi_email() is a read-only method!';
	}
	my $labname = sprintf "%s %s", $self->lab_first, $self->lab_last;
	if ( $labname and exists $self->{lab}->{$labname} ) {
		return $self->{lab}->{$labname}->[LAB_EMAIL];
	}
	else {
		print " ! No lab information available for '$labname'\n";
		return;
	}
}

sub allow_upload {
	my $self = shift;
	my $labname = sprintf "%s %s", $self->lab_first, $self->lab_last;
	if ( $labname and exists $self->{lab}->{$labname} ) {
		my $v = $self->{lab}->{$labname}->[LAB_UPLOAD];
		if ($v eq 'Y') {
			return $self->{lab}->{$labname}->[LAB_ACCT];
		}
		elsif ($v eq 'N') {
			return 0;
		}
		else {
			print " ! lab '$labname' has an invalid upload response '$v'\n";
			return 0;
		}
	}
	else {
		return 0;
	}
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
		carp 'profile() is a read-only method!';
	}
	my $core = $self->core_lab;
	return unless $core;
	if ( exists $self->{account}->{$core} ) {
		return $self->{account}->{$core}->[ACT_PROFILE];
	}
	else {
		print " ! No account information available for '$core'\n";
		return;
	}
}

sub account_number {
	my $self = shift;
	if (@_) {
		carp 'account_number() is a read-only method!';
	}
	my $core = $self->core_lab;
	return unless $core;
	if ( exists $self->{account}->{$core} ) {
		return $self->{account}->{$core}->[ACT_NUMBER];
	}
	else {
		print " ! No account information available for '$core'\n";
		return 0;
	}
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
			if ($delta > 1024) {
				# there's a significant change of greater than 1Kb
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
	if (@_ and defined $_[0]) {
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

sub project_s3_uri {
	my $self = shift;
	my $bucket = $self->bucket;
	my $prefix = $self->prefix;
	return q() unless ( $bucket and $prefix );
	return sprintf("s3://%s/%s/", $bucket, $prefix );
}

sub project_core_url {
	my $self   = shift;
	my $org    = $self->core_lab;
	my $number = $self->account_number;
	my $bucket = $self->bucket;
	my $prefix = $self->prefix;
	return q() unless ( $org and $number and $bucket and $prefix );

	# escape spaces
	$org =~ s/\ /%20/g;

	# generate url to CORE Browser - this assumes always AWS accounts
	my $url = sprintf "%s/goto?organization=%s&account=AWS%%20%s&path=/%s/%s", $BASE_URL,
		$org, $number, $bucket, $prefix;
	return $url;
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
		$self->core_lab || q(),
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
		for my $i (AGE, SCAN, UPLOAD, HIDDEN, DELETED, EMAILED, AAUPLOAD, QCSCAN) {
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

There are three special keys in the database catalog file corresponding
to metadata about labs and AWS CORE lab accounts. This avoids storing
redundant information in each project entry.

=over 4

=item header

This is stored under the C<HEADER> key and points to an array of the current
project table column header names.

=item lab information

This is stored under the C<LABS> key, and the value is an anonymous hash,
the keys of which are the "First Last" names of the lab Principal Investigator
as indicated in the GNomEx database. The value is an anonymous array of the
PI email address, a boolean (Y or N) whether uploads are allowed, and the name
of the default CORE lab account name, usually "First Last Lab".

=item AWS accounts

This is stored under the C<ACCOUNTS> key, and the value is another anonymous
hash, the keys of which are the CORE Lab name, usually "First Last Lab", and the
value an array of two values, the IAM service account profile name and the
AWS account number. Some labs have multiple accounts, but usually only one is
designated for GNomEx uploads.

=back 

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

item import_labs

Import a lab information file. This is a tab-delimited text file used to
populate the lab information metadata key in the database. Pass the path
to the file. It will always import all records (as opposed to updating
each individually). It returns the number of records imported. 

=item import_accounts

Import CORE lab account information. This is a tab-delimited text file
used to populate the account information metadata key in the database.
Pass the path to the file. It will always import all records (as
opposed to updating each individually). It returns the number of
records imported. 

=item header

Returns the standard header line used in printing and exporting 
files.

=item check_lab

Pass a lab name ("First Last") or a RepoEntry object to the method to
check whether information metadata about the lab is present in the
database. Returns 1 (true) or 0 (false).

=item allow_upload

Pass a lab name ("First Last") or a RepoEntry object to determine
whether a lab is allowed to upload or not. Returns 1 (true) or 0 (false).

=item get_upload_account

Pass a lab name ("First Last") or a RepoEntry object, and if the lab
has a default CORE lab account name, it is returned.

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

This is now a read-only function.

=item allow_upload

=item core_lab

=item profile

This is now a read-only function.

=item account_number

This is a read-only function.

=item bucket

=item prefix

=item external

=item request_status

=item request_application

=item organism

=item genome

=item size

This gets/sets the current size of the project in bytes. When setting, the
previous size is automatically stored as the last size.

=item last_size

This a read-only function.

=item youngest_datestamp

=item age

Calculates the age in days from the youngest datestamp to now.

=item scan_datestamp

=item upload_datestamp

=item hidden_datestamp

=item deleted_datestamp

=item emailed_datestamp

=itme autoanal_up_datestamp

=item autoanal_upload_age

Calculates the age in days for the autoanalysis folder.

=item qc_scan_datestamp

=item autoanal_folder

=item project_s3_uri

=item project_core_url

=item print_string

Returns a printable, tab-delimited string with new line ending
representation of the project. Pass a true/false value to transform
datestamps from Unix epoch integers to date-time formatted strings,
and sizes in bytes to a magnitude suffix (K, M, G, T) using binary
(base 2, instead of base 10) transformations.

=back


=head1 AUTHOR

 Timothy J. Parnell, PhD
 Bioinformatics Shared Resource
 Huntsman Cancer Institute
 University of Utah
 Salt Lake City, UT, 84112

This package is free software; you can redistribute it and/or modify
it under the terms of the Artistic License 2.0.  



