package Gnomex;

our $VERSION = 5;

=head1 NAME 

Gnomex - HCI-specific library for interacting with the GNomEx database

=head1 DESCRIPTION

These are subroutines for fetching new data from the GNomEx database.

=cut


use strict;
use Carp;
use IO::File;
use File::Spec;
use DBI;
# DBD::ODBC and Microsoft ODBC SQL driver is required - see below
use RepoProject;

1;



#### Default Database parameters
my $default_server = 'hci-db.hci.utah.edu';
my $default_port = 1433;
my $default_driver = '{ODBC Driver 17 for SQL Server}';
my $default_database = 'GNomEx';
my $default_permfile = File::Spec->catfile($ENV{HOME}, '.gnomex');
my $default_year = 2018;

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
order by Analysis.idAnalysis;
QUERY

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
join project on project.idproject = request.idproject 
join lab on lab.idlab = request.idlab 
join appuser on appuser.idappuser = request.idappuser 
join application on application.codeapplication = request.codeapplication
WHERE request.idCoreFacility = 1 
ORDER BY request.createDate;
QUERY


sub new {
	my $class = shift;
	my %opts = @_;
	
	# defaults
	$opts{server} ||= $default_server;
	$opts{port} ||= $default_port;
	$opts{driver} ||= $default_driver;
	$opts{database} ||= $default_database;
	
	# catalog must be present
	unless (exists $opts{catalog} and ref($opts{catalog}) eq 'RepoCatalog') {
		croak "Must initialize with catalog option to a RepoCatalog object!";
	}
	
	# GNomEx database credentials
	$opts{pass} ||= undef;
	$opts{user} ||= undef;
	$opts{perm} ||= $default_permfile;
	if ($opts{perm} and -e $opts{perm}) {
		# excellent, we have permissions file
		my $fh = IO::File->new($opts{perm}) or die 
			"unable to open file '$opts{perm}'! $!\n";
		$opts{user} = $fh->getline;
		chomp $opts{user};
		$opts{pass} = $fh->getline;
		chomp $opts{pass};
		$fh->close;
	}
	
	# lab information
	$opts{lab} ||= undef;
	if ($opts{lab} and -e $opts{lab}) {
		# open file
		my $fh = IO::File->new($opts{lab}) or 
			croak "unable to open $opts{lab}! $!";
		my $header = $fh->getline;
		
		# check header
		unless ($header =~ /^Name	Email	Allow.Upload	SB.Division$/) {
			croak "lab division file must have four columns: Name, Email, Allow_Upload, SB_Division\n";
		}
		
		# load lab information
		my %lab2info;
		while (my $line = $fh->getline) {
			chomp $line;
			my @bits = split("\t", $line);
			my $name = shift @bits;
			$lab2info{$name} = \@bits;
		}
		# printf " Loaded %d labs information\n", scalar(keys %lab2info);
		$fh->close;
		$opts{lab} = \%lab2info;
	}
	else {
		croak "Must pass a lab information file!";
		return;
	}
	
	# open database handle
	if ($opts{user} and $opts{pass}) {
		my $dsn = sprintf "dbi:ODBC:driver=%s;database=%s;Server=%s;port=%d;uid=%s;pwd=%s",
			$opts{driver}, $opts{database}, $opts{server}, $opts{port}, $opts{user}, 
			$opts{pass};
		$opts{dbh} = DBI->connect($dsn) or 
			carp "Can't connect to database! $DBI::errstr";
			return;
	}
	else {
		carp "Must pass GNomEx user name and password, or valid permissions file! No connection made";
		return;
	}
	
	# Return successfully built object
	my $self = {
		catalog => $opts{catalog},
		lab     => $opts{lab},
		dbh     => $opts{dbh},
	};
	
	return bless $self, $class;
}

sub fetch_analyses {
	my $self = shift;
	my $year_to_pull = shift || $default_year;
	my $Catalog  = $self->{catalog};
	my $lab2info = $self->{lab};
	
	# prepare and execute query
	my $sth = $self->{dbh}->prepare($anal_query);
	$sth->execute();

	
	# walk through the database results
	my $skip_count   = 0;
	my $update_count = 0;
	my $new_count    = 0;
	while (my @row = $sth->fetchrow_array) {
		
		# check date
		$row[2] =~ s/ \d\d:\d\d:\d\d\.\d+$//; # clean up time from date
		my ($year) = $row[2] =~ /^(\d{4})/;
		if ($year_to_pull and $year < $year_to_pull) {
			$skip_count++;
			next;
		}
		
		# prefix Analysis number with A
		$row[0] = 'A' . $row[0]; 
		
		# remove undefined nulls
		foreach (@row) {
			$_ = q() if not defined $_;
		}
		
		# get entry
		my $E = $Catalog->entry($row[0]);
		if ($E) {
			# an existing project, just need to update 
			$update_count++;
			
			# basically check to see if we have a sb lab division
			if ($E->external eq 'N' and not $E->division) {
				my $lab = sprintf("%s %s", $row[7], $row[8]);
				if (exists $lab2info->{$lab} and $lab2info->{$lab}->[1] eq 'Y') {
					$E->division($lab2info->{$lab}->[2]);
				}
			}
		}
		else {
			# a brand new project
			$E = $Catalog->new_entry($row[0]);
			$new_count++;
			
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
		
			# external lab and division information
			if ($row[9] eq 'Y' or $row[10] eq 'Y') {
				$E->external('Y');
			}
			else {
				# not an external lab
				$E->external('N');
				# check SB division information
				my $lab = sprintf("%s %s", $row[7], $row[8]);
				if (exists $lab2info->{$lab}) {
					$E->pi_email($lab2info->{$lab}->[0]);
					if ($lab2info->{$lab}->[1] eq 'Y') {
						# we're allowed to upload
						$E->division($lab2info->{$lab}->[2]);
					}
				}
				else {
					print " ! Missing lab information for '$lab'!\n";
				}
			}
		}
		
		# collect server stats
		my $path = $E->path;
		if (-e $path) {
			my $project = RepoProject->new($path);
			if ($project) {
				my ($size, $age) = $project->get_size_age;
				if ($size) {
					$E->size($size);
				}
				if ($age) {
					$E->youngest_age($age);
				}
			}
		}
		else {
			print " ! Missing project file path: $path";
		}
	} 
	
	printf " Finished processing %d Analysis project database entries\n", 
		$skip_count + $update_count + $new_count;
	printf "  %d skipped\n  %d updated\n  %d new\n", $skip_count, $update_count, 
		$new_count;
}


sub fetch_requests {
	my $self = shift;
	my $year_to_pull = shift || $default_year;
	my $Catalog  = $self->{catalog};
	my $lab2info = $self->{lab};
	
	# prepare and execute query
	my $sth = $self->{dbh}->prepare($req_query);
	$sth->execute();
	
	# walk through the database results
	my $skip_count   = 0;
	my $update_count = 0;
	my $new_count    = 0;
	while (my @row = $sth->fetchrow_array) {
		
		# check date
		$row[2] =~ s/ \d\d:\d\d:\d\d\.\d+$//; # clean up time from date
		my ($year) = $row[2] =~ /^(\d{4})/;
		if ($year_to_pull and $year < $year_to_pull) {
			$skip_count++;
			next;
		}
		
		# clean up things
		$row[0] =~ s/\d+$//; # remove straggling number from request, ex 1234R1
		foreach (@row) {
			# remove undefined nulls
			$_ = q() if not defined $_;
		}

	
		# get entry
		my $E = $Catalog->entry($row[0]);
		if ($E) {
			# an existing project, just need to update 
			$update_count++;
			$E->request_status($row[11]);
			
			# check to see if we have a sb lab division
			if ($E->external eq 'N' and not $E->division) {
				my $lab = sprintf("%s %s", $row[7], $row[8]);
				if (exists $lab2info->{$lab} and $lab2info->{$lab}->[1] eq 'Y') {
					$E->division($lab2info->{$lab}->[2]);
				}
			}
		}
		else {
			# a brand new project
			$E = $Catalog->new_entry($row[0]);
			$new_count++;
			
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
		
			# external lab and division information
			if ($row[9] eq 'Y' or $row[10] eq 'Y') {
				$E->external('Y');
			}
			else {
				# not an external lab
				$E->external('N');
				# check SB division information
				my $lab = sprintf("%s %s", $row[7], $row[8]);
				if (exists $lab2info->{$lab}) {
					$E->pi_email($lab2info->{$lab}->[0]);
					if ($lab2info->{$lab}->[1] eq 'Y') {
						# we're allowed to upload
						$E->division($lab2info->{$lab}->[2]);
					}
				}
				else {
					print " > Missing lab information for '$lab'!\n";
				}
			}
		}
		
		# collect server stats
		my $path = $E->path;
		if (-e $path) {
			my $project = RepoProject->new($E->path);
			if ($project) {
				my ($size, $age) = $project->get_size_age;
				if ($size) {
					$E->size($size);
				}
				if ($age) {
					$E->youngest_age($age);
				}
			}
		}
		else {
			print " ! Missing project file path: $path";
		}
		
	} 
	printf " Finished processing %d Experiment Request project database entries\n", 
		$skip_count + $update_count + $new_count;
	printf "  %d skipped\n  %d updated\n  %d new\n", $skip_count, $update_count, 
		$new_count;
}

sub DESTROY {
	my $self = shift;
	$self->{dbh}->disconnect;
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


