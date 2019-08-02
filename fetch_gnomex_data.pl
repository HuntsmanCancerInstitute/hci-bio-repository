#!/usr/bin/env perl

use strict;
use warnings;
use POSIX qw(strftime);
use IO::File;
use IO::Prompter;
use DBI;
# DBD::ODBC is required as database driver
# use GetOpt::Long;

my $VERSION = 2;

my $date = strftime("%Y-%B-%d", localtime);

# database
# database driver was installed on my Mac with brew
#    brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
#    brew install mssql-tools; accept license
my $dsn = "dbi:ODBC:driver={ODBC Driver 17 for SQL Server};database=GNomEx;Server=hci-db.hci.utah.edu;port=1433";

# prompt for credentials
my $user = prompt("Enter the user (pipeline): ", -def=>'pipeline');
my $pass = prompt("Enter the password:  ", -echo=>"*");
die "no user or password provided!\n" if (not $user and not $pass);
$dsn .= ";uid=$user;pwd=$pass";

# connect
my $dbh = DBI->connect($dsn) or 
	die "Can't connect to database! $DBI::errstr\n";


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


# prepare and execute query
my $sth = $dbh->prepare($anal_query);
$sth->execute();

# Export 
my $outfile = 'analysis.' . $date . '.txt';
my $outfh = IO::File->new($outfile, 'w') or 
	die "unable to write to $outfile! $!\n";
$outfh->printf("%s\n", join("\t", @anal_headers));
my $i = 0;
while (my @row = $sth->fetchrow_array) {
	# clean up row
	$row[0] = 'A'.$row[0]; # prefix Analysis number with A
	$row[2] =~ s/ \d\d:\d\d:\d\d\.\d+$//; # clean up time from date
	foreach (@row) {
		# remove undefined nulls
		$_ = q() if not defined $_;
	}
	# calculate path
	my ($year) = $row[2] =~ /^(\d{4})/;
	push @row, "/Repository/AnalysisData/$year/$row[0]";
	
	# print
	$outfh->printf("%s\n", join("\t", @row));
	$i++;
} 
$outfh->close;
undef $sth;
print " Wrote $i Analysis rows to $outfile\n";





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


# prepare and execute query
$sth = $dbh->prepare($req_query);
$sth->execute();

# Export 
$outfile = 'requests.' . $date . '.txt';
$outfh = IO::File->new($outfile, 'w') or 
	die "unable to write to $outfile! $!\n";
$outfh->printf("%s\n", join("\t", @req_headers));
$i = 0;
while (my @row = $sth->fetchrow_array) {
	
	# remove undefined nulls
	foreach (@row) {
		$_ = q() if not defined $_;
	}
	
	# clean up things
	$row[0] =~ s/\d+$//; # remove straggling number from request, ex 1234R1
	$row[2] =~ s/ \d\d:\d\d:\d\d\.\d+$//; # clean up time from date
	
	# calculate path
	my ($year) = $row[2] =~ /^(\d{4})/;
	push @row, "/Repository/MicroarrayData/$year/$row[0]";
	
	# print
	$outfh->printf("%s\n", join("\t", @row));
	$i++;
} 
$outfh->close;
print " Wrote $i Request rows to $outfile\n";


### Finished
$dbh->disconnect;

