#!/usr/bin/env perl

use strict;
use warnings;
use IO::File;
use File::Spec;
use DBI;
# DBD::ODBC and Microsoft ODBC SQL driver is required - see below
use Getopt::Long;
use FindBin qw($Bin);
use lib $Bin;
use RepoCatalog;
use RepoProject;

my $VERSION = 5;


######## Documentation
my $doc = <<END;
A script to collect essential information from the GNomEx database 
for processing Repository Projects. This needs a username and 
password for the database. Enter username and password on line 1 and 2, 
respectively, in a restricted text file.

Information is stored in a catalog file specified by the parameters.
Information is only retrieved when a catalog file is provided.
Seven Bridges division information, and current lab PI email addresses, 
must be provided in a separate file, and will be added to the catalog 
entries.

fetch_gnomex_data.pl <options>

    --analysis <path>       Provide the path to an analysis catalog file
    --request <path>        Provide the path to a request catalog file
    --division <path>       Provide the path to a lab division file.
    --year <integer>        Restrict reporting to indicated year and newer
    --stats                 Get project size and stats, must be on server!
    --perm <path>           GNomEx database permissions file (default ~/.gnomex)
END



=cut 

=head 2 Installing database support on Mac

Using Homebrew L<https://brew.sh>. Also see 
L<https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos>

   brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
   brew install msodbcsql17 mssql-tools unixODBC

Accept the end-user license

=head2 Installing database support on CentOS Linux

Set up repo by following instructions at 
L<https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server>

Then install

	yum install msodbcsql17 unixODBC

Accept the end-user license for the MS driver

Install Perl modules

	yum install perl-DBI perl-DBD-ODBC

=cut




####### Database parameters
my $server = 'hci-db.hci.utah.edu';
my $port = 1433;
my $driver = '{ODBC Driver 17 for SQL Server}';
my $database = 'GNomEx';
my $user;
my $pass;





####### Input
my $anal_cat_file;
my $req_cat_file;
my $lab_div_file;
my $year_to_pull;
my $collect_stats;
my $perm_file = File::Spec->catfile($ENV{HOME}, '.gnomex');
if (scalar(@ARGV) > 1) {
	GetOptions(
		'analysis=s'        => \$anal_cat_file,
		'request=s'         => \$req_cat_file,
		'division=s'        => \$lab_div_file,
		'year=i'            => \$year_to_pull,
		'perm=s'            => \$perm_file,
		'stats!'            => \$collect_stats,
	) or die "please recheck your options!\n\n";
}
else {
	print $doc;
	exit;
}

### Check inputs
if ($year_to_pull and $year_to_pull !~ /^\d{4}$/) {
	die "'$year_to_pull' not a valid year! Must be four digits\n";
}
unless ($lab_div_file and -e $lab_div_file) {
	die "SB lab division file is required!\n";
}
unless ($anal_cat_file or $req_cat_file) {
	die "At least one catalog file must be provided!\n";
}





####### Database connection
if (-e $perm_file) {
	# excellent, we have permissions file
	my $fh = IO::File->new($perm_file) or die 
		"unable to open file '$perm_file'! $!\n";
	$user = $fh->getline;
	chomp $user;
	$pass = $fh->getline;
	chomp $pass;
	$fh->close;
}
else {
	# old style
	eval {require IO::Prompter; 1;};
	if ($@) {
		die "No permissions file!\nCan't load IO::Prompter! $@ \n" ;
	}
	else {
		$user = prompt("Enter the user (pipeline): ", -def=>'pipeline');
		$pass = prompt("Enter the password:  ", -echo=>"*");
	}
	
	die "no user or password provided!\n" if (not $user and not $pass);
}

# connect
my $dsn = "dbi:ODBC:driver=$driver;database=$database;Server=$server;port=$port;uid=$user;pwd=$pass";
my $dbh = DBI->connect($dsn) or 
	die "Can't connect to database! $DBI::errstr\n";





######## Lab division information
my %lab2info;
{
	my $fh = IO::File->new($lab_div_file) or 
		die "unable to open $lab_div_file! $!\n";
	my $header = $fh->getline;
	unless ($header =~ /^Name	Email	Allow.Upload	SB.Division$/) {
		die "lab division file must have four columns: Name, Email, Allow_Upload, SB_Division\n";
	}
	while (my $line = $fh->getline) {
		chomp $line;
		my @bits = split("\t", $line);
		my $name = shift @bits;
		$lab2info{$name} = \@bits;
	}
	printf " Loaded %d labs information\n", scalar(keys %lab2info);
	$fh->close;
}





############ Analysis SQL query

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

my @anal_headers = ('AnalysisNumber', 'AnalysisName', 'AnalysisDate', 'GroupName',
	'UserEMail', 'UserFirstName', 'UserLastName', 'LabFirstName', 'LabLastName',
	'isExternalPricing','isExternalCommercialPricing','Organism','GenomeBuild', 'Path');

if ($anal_cat_file) {
	
	# Open catalog
	my $Catalog = RepoCatalog->new($anal_cat_file) or 
		die "unable to initialize catalog file $anal_cat_file!";
	
	# prepare and execute query
	my $sth = $dbh->prepare($anal_query);
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
				if (exists $lab2info{$lab} and $lab2info{$lab}->[1] eq 'Y') {
					$E->division($lab2info{$lab}->[2]);
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
				if (exists $lab2info{$lab}) {
					$E->pi_email($lab2info{$lab}->[0]);
					if ($lab2info{$lab}->[1] eq 'Y') {
						# we're allowed to upload
						$E->division($lab2info{$lab}->[2]);
					}
				}
				else {
					printf "Missing lab information for '$lab'!\n";
				}
			}
		}
		
		# collect server stats
		if ($collect_stats) {
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
	} 
	printf " Finished processing %d Analysis project database entries\n", 
		$skip_count + $update_count + $new_count;
	printf "  %d skipped\n  %d updated\n  %d new\n", $skip_count, $update_count, 
		$new_count;
}



############ Request SQL query

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

my @req_headers = ('RequestNumber', 'RequestName', 'RequestDate', 'ProjectName',
	'UserEMail','UserFirstName', 'UserLastName', 'LabFirstName','LabLastName',
	'isExternalPricing','isExternalCommercialPricing', 'RequestStatus', 'Application', 
	'Path');

if ($req_cat_file) {
	
	# Open catalog
	my $Catalog = RepoCatalog->new($req_cat_file) or 
		die "unable to initialize catalog file $req_cat_file!";
	
	# prepare and execute query
	my $sth = $dbh->prepare($req_query);
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
				if (exists $lab2info{$lab} and $lab2info{$lab}->[1] eq 'Y') {
					$E->division($lab2info{$lab}->[2]);
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
				if (exists $lab2info{$lab}) {
					$E->pi_email($lab2info{$lab}->[0]);
					if ($lab2info{$lab}->[1] eq 'Y') {
						# we're allowed to upload
						$E->division($lab2info{$lab}->[2]);
					}
				}
				else {
					printf "Missing lab information for '$lab'!\n";
				}
			}
		}
		
		# collect server stats
		if ($collect_stats) {
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
		
	} 
	printf " Finished processing %d Experiment Request project database entries\n", 
		$skip_count + $update_count + $new_count;
	printf "  %d skipped\n  %d updated\n  %d new\n", $skip_count, $update_count, 
		$new_count;
}

### Finished
$dbh->disconnect;


__END__

=head1 AUTHOR

 Timothy J. Parnell, PhD
 Dept of Oncological Sciences
 Huntsman Cancer Institute
 University of Utah
 Salt Lake City, UT, 84112

This package is free software; you can redistribute it and/or modify
it under the terms of the Artistic License 2.0.  

