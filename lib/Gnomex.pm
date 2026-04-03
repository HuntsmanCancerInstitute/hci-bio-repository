package Gnomex;

use strict;
use English qw(-no_match_vars);
use Carp;
use IO::File;
use File::Spec;
use DBI;
# DBD::ODBC and Microsoft ODBC SQL driver is required - see below
use hciCore qw( generate_prefix generate_bucket );

our $VERSION = 'v9.1.0';




#### Default Database parameters
my $default_server = 'hci-db.hci.utah.edu';
my $default_port = 1433;
my $default_driver = '{ODBC Driver 17 for SQL Server}';
my $default_database = 'GNomEx';
my $default_permfile = File::Spec->catfile($ENV{HOME}, '.gnomex');
my $default_date = '2018-01-01';

# GNomEx database queries 
# these are a bit painful, but I'm getting out what I want
# currently they return all or nearly all projects, and then I subsequently filter
# filtering could be opitimized
my $anal_query = <<QUERY;
SELECT Analysis.idAnalysis AnalysisNumber, 
Analysis.name AnalysisName, 
Analysis.createDate AnalysisDate, 
AnalysisGroup.name GroupName, 
appuser.email UserEMail,
appuser.firstname UserFirstname, 
appuser.lastname UserLastName, 
lab.firstname LabFirstName, 
lab.lastname LabLastName, 
lab.isExternalPricing, 
lab.isExternalPricingCommercial, 
organism.organism Organism, 
genomebuild.genomebuildname GenomeBuild 
FROM Analysis  
left join AnalysisGroupItem on AnalysisGroupItem.idAnalysis = Analysis.idAnalysis 
left join AnalysisGroup on AnalysisGroupItem.idAnalysisGroup = AnalysisGroup.idAnalysisGroup 
left join appuser on appuser.idappuser = Analysis.idappuser 
left join lab on lab.idlab = Analysis.idLab 
left join organism on organism.idorganism = Analysis.idorganism 
left join AnalysisGenomeBuild on AnalysisGenomeBuild.idAnalysis = Analysis.idAnalysis 
left join genomebuild on AnalysisGenomeBuild.idgenomebuild = genomebuild.idgenomebuild 
WHERE Analysis.createDate >= '%s'
order by Analysis.idAnalysis;
QUERY
# WHERE Analysis.createDate > (select dateadd(year, -3, getdate()))

my $req_query = <<QUERY;
SELECT request.number  RequestNumber, 
request.name RequestName, 
request.createDate RequestDate, 
project.name ProjectName, 
appuser.email UserEMail,
appuser.firstname UserFirstname, 
appuser.lastname UserLastName, 
lab.firstname LabFirstName, 
lab.lastname LabLastName, 
lab.isExternalPricing, 
lab.isExternalPricingCommercial, 
request.codeRequestStatus,
application.application Application
FROM request 
left join project on project.idproject = request.idproject 
left join lab on lab.idlab = request.idlab 
left join appuser on appuser.idappuser = request.idappuser 
left join application on application.codeapplication = request.codeapplication
WHERE request.idCoreFacility = 1 AND request.createDate > '%s'
ORDER BY request.number;
QUERY
# WHERE request.createDate > (select dateadd(year, -2, getdate()))


sub new {
	my $class = shift;
	my %opts = @_;
	
	# defaults
	$opts{server}   ||= $default_server;
	$opts{port}     ||= $default_port;
	$opts{driver}   ||= $default_driver;
	$opts{database} ||= $default_database;
	$opts{catalog}  ||= undef;
	
	# GNomEx database credentials
	$opts{pass} ||= undef;
	$opts{user} ||= undef;
	$opts{perm} ||= $default_permfile;
	if (not $opts{pass} or not $opts{user}) {
		if ($opts{perm} and -e $opts{perm}) {
			# excellent, we have permissions file
			my $fh = IO::File->new($opts{perm}) or die 
				"unable to open file '$opts{perm}'! $OS_ERROR\n";
			my $u = $fh->getline;
			chomp $u;
			$opts{user} ||= $u;
			my $p = $fh->getline;
			chomp $p;
			$opts{pass} ||= $p;
			$fh->close;
		}
		else {
			carp "user and password not provided, Gnomex permissions file not available\n GnomEx file should be text file $default_permfile with username and password as single bare words on lines 1 and 2\n";
			return;
		}
	}
	
	# open database handle
	if ($opts{user} and $opts{pass}) {
		my $dsn = sprintf "dbi:ODBC:driver=%s;database=%s;Server=%s;port=%d;uid=%s;pwd=%s",
			$opts{driver}, $opts{database}, $opts{server}, $opts{port}, $opts{user}, 
			$opts{pass};
		$opts{dbh} = DBI->connect($dsn);
		unless ($opts{dbh}) { 
			carp "Can't connect to database! $DBI::errstr";
			return;
		}
	}
	else {
		carp "Must pass GNomEx user name and password, or valid permissions file! No connection made";
		return;
	}
	
	# check database
	if ( $opts{catalog} and ref( $opts{catalog} ) ne 'RepoCatalog' ) {
		my $r = ref $opts{catalog};
		carp "Catalog parameter is not a RepoCatalog object! It is a $r";
		return;
	}
	
	# Return successfully built object
	my $self = {
		catalog => $opts{catalog},
		dbh     => $opts{dbh},
	};
	
	return bless $self, $class;
}

sub fetch_analyses {
	my $self = shift;
	my $date = shift || $default_date;
	my $Catalog  = $self->{catalog} || undef;
	unless ($Catalog) {
		carp " Must initialize Gnomex object with a Catalog object!";
		return;
	}
	unless ($date =~ /^\d{4} \- \d{2} \- \d{2} $/x) {
		carp " Provided date must be YYYY-MM-DD!";
		return;
	}
	
	# prepare and execute query
	my $query1 = sprintf $anal_query, $date;
	my $sth = $self->{dbh}->prepare($query1);
	$sth->execute();

	
	# walk through the database results
	my @update_list;
	my @new_list;
	my @nochange_list;
	while (my @row = $sth->fetchrow_array) {
		
		# check
		unless ($row[0]) {
			printf " database returned an item without an identifier: %s\n", join ", ", @row;
			next;
		}
		
		# check date
		$row[2] =~ s/\s+ \d\d: \d\d: \d\d \.\d+ $//x; # clean up time from date
		my ($year) = $row[2] =~ /^(\d{4})/;
		
		# prefix Analysis number with A
		$row[0] = 'A' . $row[0]; 
		
		# remove undefined nulls
		foreach (@row) {
			$_ = q() if not defined;
		}
		
		# get entry
		my $E = $Catalog->entry($row[0]);
		if ($E) {
			# an existing project, just need to update 
			my $u = 0;
			
			# check and update lab name
			if ( $row[7] ne $E->lab_first or $row[8] ne $E->lab_last ) {
				$E->lab_first($row[7]);
				$E->lab_last($row[8]);
				printf "  > updating lab name to '%s %s' for %s\n", $row[7], 
					$row[8], $E->id;
				$u++;
			}			
			
			# reconfirm external status
			if ( ( $row[9] eq 'Y' or $row[10] eq 'Y' ) and $E->external eq 'N' ) {
				printf "  > updating %s to external status\n", $E->id;
				$E->external('Y');
				$u++;
			}
			
			# check to see if we have a CORE lab
			if ( $E->external eq 'N' and $Catalog->check_lab($E) ) {
				# for university clients only

				# collect the CORE labs for this project
				my $default_lab = $Catalog->get_upload_account($E);
				my $alt_lab = $Catalog->get_upload_account(
					sprintf("%s %s", $row[5], $row[6]) ); # based on username
					
				# check CORE lab status
				if (
					# check length of values to ensure comparing real values
					# also skip if it's already been uploaded or hidden
					( length($E->core_lab) > 1 or length($default_lab) > 1 )
					and $E->core_lab ne $default_lab
					and not $E->upload_datestamp and not $E->hidden_datestamp
				) {
					# there's a difference here
					# we assume the lab information file is correct and updated
					if ( $E->allow_upload) {
						
						# must be updated lab information
						# only update if this project has not been hidden
						if ( not $E->hidden_datestamp ) {
							printf "  > updating CORE Lab for %s from '%s' to '%s'\n",
								$row[0], $E->core_lab, $default_lab;
							$E->core_lab($default_lab);
							generate_bucket($E);
							generate_prefix($E);
							$u++;
						}
					}
					else {

						# not supposed to be allowed to upload
						# must have been manually deliberately set so leave it
					}
				}
				
				# check alternate CORE lab
				if ( 
					not $E->core_lab and not $default_lab and $alt_lab
					and not $E->upload_datestamp and not $E->hidden_datestamp
				) {
					# sometimes a PI with an AWS CORE lab account will submit a
					# project under a collaborator's PI lab account without a CORE
					# account, so in that case reassign to the submitting PI's account
					# looking at you H***** and J** and D***** and....
					printf 
		"  > assigning CORE account for %s from PI %s %s to User's account '%s'\n",
						$row[0], $row[7], $row[8], $alt_lab;
					$E->core_lab( $alt_lab );
					generate_bucket($E);
					generate_prefix($E);
					$u++;
				}
			}
			elsif ( not $Catalog->check_lab($E) ) {
				printf "  ! no lab information for '%s %s' for %s\n", $E->lab_first,
					$E->lab_last, $E->id;
			}
			
			
			# check user info
			if ($row[4] ne $E->user_email) {
				printf "  > updating user %s %s email address for %s\n", $E->user_first, 
					$E->user_last, $E->id;
				$E->user_email($row[4]);
				$u++;
			}
			if ( $row[5] ne $E->user_first or $row[6] ne $E->user_last ) {
				$E->user_first($row[5]);
				$E->user_last($row[6]);
				printf "  > updating user name %s %s for %s\n", $E->user_first, 
					$E->user_last, $E->id;
				$u++;
			}
			
			# update project name and group
			if ($row[1] ne $E->name) {
				printf "  > updating project name for %s\n", $E->id;
				$E->name($row[1]);
				$u++;
				if ( $E->core_lab ) {
					if ( $E->upload_datestamp < 1000 ) {
						generate_prefix($E);
					}
					else {
						printf "    ! name changed after upload\n"
					}
				}
			}
			if ($row[3] ne $E->group) {
				printf "  > updating project group for %s\n", $E->id;
				$E->group($row[3]);
				$u++;
				if ( $E->core_lab ) {
					if ( $E->upload_datestamp < 1000 ) {
						generate_bucket($E);
					}
					else {
						printf "    ! group changed after upload\n"
					}
				}
			}
			
			# update organism
			if ( $row[11] ne $E->organism ) {
				$E->organism($row[11]);
				$u++;
			}
			if ( $row[12] ne $E->genome ) {
				$E->organism($row[12]);
				$u++;
			}
			
			# add to return lists
			if ($u) {
				push @update_list, $row[0];
			}
			else {
				push @nochange_list, $row[0];
			}
			
		}
		else {
			# a brand new project
			$E = $Catalog->new_entry($row[0]);
			unless ($E) {
				printf " failed to create database entry for '%s', skipping\n", $row[0];
				next;
			}
			push @new_list, $row[0];
			
			# let's fill it out
			$E->path("/Repository/AnalysisData/$year/$row[0]");
			$E->name($row[1]);
			$E->date($row[2]);
			$E->group($row[3]);
			$E->user_email($row[4]);
			$E->user_first($row[5]);
			$E->user_last($row[6]);
			$E->lab_first($row[7]);
			$E->lab_last($row[8]);
			$E->organism($row[11]);
			$E->genome($row[12]);
		
			# lab information
			if ($row[9] eq 'Y' or $row[10] eq 'Y') {
				$E->external('Y');
			}
			else {
				# not an external lab
				$E->external('N');
				if ( $Catalog->check_lab($E) ) {
					my $default_core = $Catalog->get_upload_account($E);
					my $alt_core = $Catalog->get_upload_account(
						sprintf("%s %s", $row[5], $row[6]) ); # based on username
					if ($default_core) {
						# this lab has an account 
						$E->core_lab($default_core);
						generate_bucket($E);
						generate_prefix($E);
					}
					elsif ($alt_core) {
						# user has a CORE lab account
						# usually a PI submitting as a user under another PI lab
						printf 
			"  > assigning CORE account for %s from PI %s %s to User's account '%s'\n",
							$row[0], $row[7], $row[8], $alt_core;
						$E->core_lab($alt_core);
						generate_bucket($E);
						generate_prefix($E);
					
					}
				}
				else {
					printf " ! Missing lab information for '%s %s' for %s!\n", $row[7],
						$row[8], $row[0];
				}
			}
		}
	} 
	
	# finished
	return (\@update_list, \@new_list, \@nochange_list);
}


sub fetch_requests {
	my $self = shift;
	my $date = shift || $default_date;
	my $Catalog  = $self->{catalog} || undef;
	unless ($Catalog) {
		carp " Must initialize Gnomex object with a Catalog object!";
		return;
	}
	unless ($date =~ /^\d{4} \- \d{2} \- \d{2} $/x) {
		carp " Provided date must be YYYY-MM-DD!";
		return;
	}
	
	# prepare and execute query
	my $query1 = sprintf $req_query, $date;
	my $sth = $self->{dbh}->prepare($query1);
	$sth->execute();
	
	# walk through the database results
	my @update_list;
	my @new_list;
	my @nochange_list;
	while (my @row = $sth->fetchrow_array) {
				
		# check date
		$row[2] =~ s/\s+ \d\d: \d\d: \d\d \.\d+ $//x; # clean up time from date
		my ($year) = $row[2] =~ /^(\d{4})/;
		
		# clean up things
		$row[0] =~ s/\d+$//; # remove straggling number from request, ex 1234R1
		foreach (@row) {
			# remove undefined nulls
			$_ = q() if not defined;
		}

		# get entry
		my $E = $Catalog->entry($row[0]);
		if ($E) {
			# update existing project as necessary
			# basically just two database fields we're really concerned about here
			my $u = 0;
			
			# status
			if ( $E->request_status ne 'COMPLETE' and $E->request_status ne $row[11] ) {
				# do not update if already marked completed, because sometimes it's
				# not updated appropriately in GNomEx and has to be done manually
				$E->request_status($row[11]);
				$u++;
			}

			# check lab name
			if ( $row[7] ne $E->lab_first or $row[8] ne $E->lab_last ) {
				$E->lab_first($row[7]);
				$E->lab_last($row[8]);
				printf "  > updating lab name to '%s %s' for %s\n", $row[7], 
					$row[8], $E->id;
				$u++;
			}

			# reconfirm external status
			if ( ( $row[9] eq 'Y' or $row[10] eq 'Y' ) and $E->external eq 'N' ) {
				printf "  > updating %s to external status\n", $E->id;
				$E->external('Y');
				$u++;
			}

			# check CORE lab status
			if ( $E->external eq 'N' and $Catalog->check_lab($E) ) {
				# for university clients only
				
				# collect the CORE labs for this project
				my $default_lab = $Catalog->get_upload_account($E);
				my $alt_lab = $Catalog->get_upload_account(
					sprintf("%s %s", $row[5], $row[6]) ); # based on username


					
				# check CORE lab status
				if (
					# check length of values to ensure comparing real values
					# also skip if it's already been uploaded or hidden
					( length($E->core_lab) > 1 or length($default_lab) > 1 )
					and $E->core_lab ne $default_lab
					and not $E->upload_datestamp and not $E->hidden_datestamp
				) {
					# there's a difference here
					# we assume the lab information file is correct and updated
					if ( $E->allow_upload ) {
						printf "  > updating CORE lab for %s from '%s' to '%s'\n",
							$row[0], $E->core_lab, $default_lab;
						$E->core_lab($default_lab);
						$u++;
					}
					else {
						# not supposed to be allowed to upload
						# must have been manually deliberately set so leave it
					}
				}
				
				# check alternate Core lab division
				if ( 
					not $E->core_lab and not $default_lab and $alt_lab
					and not $E->upload_datestamp and not $E->hidden_datestamp
				) {
					# sometimes a PI with an AWS CORE lab account will submit a
					# project under a collaborator's PI lab account without a CORE
					# account, so in that case reassign to the submitting PI's account
					# looking at you H***** and J** and D***** and....
					printf 
		"  > assigning CORE account for %s from PI %s %s to User's account '%s'\n",
						$row[0], $row[7], $row[8], $alt_lab;
					$E->core_lab($alt_lab);
					$u++;
				}
			}
			elsif ( not $Catalog->check_lab($E) ) {
				printf "  ! no lab information for '%s %s' for %s\n", $E->lab_first,
					$E->lab_last, $E->id;
			}
			
			# check user info
			if ($row[4] ne $E->user_email) {
				printf "  > updating user %s %s email address for %s\n", $E->user_first, 
					$E->user_last, $E->id;
				$E->user_email($row[4]);
				$u++;
			}
			if ( $row[5] ne $E->user_first or $row[6] ne $E->user_last ) {
				$E->user_first($row[5]);
				$E->user_last($row[6]);
				printf "  > updating user name %s %s for %s\n", $E->user_first, 
					$E->user_last, $E->id;
				$u++;
			}
			
			# update project name and group
			if ($row[1] ne $E->name) {
				printf "  > updating project name for %s\n", $E->id;
				$E->name($row[1]);
				$u++;
				if ( $E->core_lab and $E->bucket ) {
					if ( $E->upload_datestamp < 1000 ) {
						generate_prefix($E);
					}
					else {
						printf "    ! name changed after upload\n"
					}
				}
			}
			if ($row[3] ne $E->group) {
				printf "  > updating project group for %s\n", $E->id;
				$E->group($row[3]);
				$u++;
				if ( $E->core_lab and $E->bucket ) {
					if ( $E->upload_datestamp < 1000 ) {
						generate_bucket($E);
					}
					else {
						printf "    ! group changed after upload\n"
					}
				}
			}
			
			# add to return lists
			if ($u) {
				push @update_list, $row[0];
			}
			else {
				push @nochange_list, $row[0];
			}
			
		}
		else {
			# a brand new project
			$E = $Catalog->new_entry($row[0]);
			unless ($E) {
				printf " failed to create database entry for '%s', skipping\n", $row[0];
				next;
			}
			push @new_list, $row[0];
			
			# let's fill it out
			$E->path("/Repository/MicroarrayData/$year/$row[0]");
			$E->name($row[1]);
			$E->date($row[2]);
			$E->group($row[3]);
			$E->user_email($row[4]);
			$E->user_first($row[5]);
			$E->user_last($row[6]);
			$E->lab_first($row[7]);
			$E->lab_last($row[8]);
			$E->request_status($row[11]);
			$E->request_application($row[12]);
		
			# lab information
			if ($row[9] eq 'Y' or $row[10] eq 'Y') {
				$E->external('Y');
			}
			else {
				# not an external lab
				$E->external('N');
				# check CORE lab information
				if ( $Catalog->check_lab($E) ) {
					my $default_core = $Catalog->get_upload_account($E);
					my $alt_core = $Catalog->get_upload_account(
						sprintf("%s %s", $row[5], $row[6]) ); # based on username
					if ($default_core) {
						# this lab has an account, but do not set buckets
						$E->core_lab($default_core);
					}
					elsif ($alt_core) {
						# user has a CORE lab account, but do not set buckets
						# usually a PI submitting as a user under another PI lab
						printf 
			"  > assigning CORE account for %s from PI %s %s to User's account '%s'\n",
							$row[0], $row[7], $row[8], $alt_core;
						$E->core_lab($alt_core);
					
					}
				}
				else {
					printf " ! Missing lab information for '%s %s' for %s!\n", $row[7],
						$row[8], $row[0];
				}
			}
		}
	}
	
	# finished
	return (\@update_list, \@new_list, \@nochange_list);
}

sub DESTROY {
	my $self = shift;
	$self->{dbh}->disconnect;
}

1;

__END__

=head1 NAME 

Gnomex - HCI-specific library for interacting with the GNomEx database

=head1 DESCRIPTION

These are subroutines for fetching new data from the GNomEx database.

=head1 AUTHOR

 Timothy J. Parnell, PhD
 Bioinformatics Shared Resource
 Huntsman Cancer Institute
 University of Utah
 Salt Lake City, UT, 84112

This package is free software; you can redistribute it and/or modify
it under the terms of the Artistic License 2.0.  



