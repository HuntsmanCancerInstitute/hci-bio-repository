#!/usr/bin/env perl

# A script to check Legacy GnomEx projects in SB divisions


use strict;
use Getopt::Long;
use IO::File;
use FindBin qw($Bin);
use lib $Bin;
use SB;

my $doc = <<END;

A script to check the legacy GNomEx project in lab's SB divisions.
This will present whether it is present, and who has membership in it.

It will optionally rename the project to a new, more obvious name. It 
will also replace the default description with a new Markdown description
detailing what is in there. These values are hardcoded below.

Provide an input list of divisions. It should be a two-column, tab-delimited, 
text file with the Seven Bridges division ID and the name of the default legacy 
GNomEx project (default is the PI "First Last" name). It assumes a header line.

You can run just one lab division without an input file. Give 

Options:
    --in <file>         Input file of SB division and project name
    --out <file>        Output result file
    --div <text>        For one division, give SB division ID
    --title <text>      For one division, give the legacy project title
    --change            Flag to change project name and description 
    --sb <file>         Path to sb utility
    --cred <file>       Path to alternate credentials file

END

my $new_name = 'GNomEx Legacy Data';
my $description = <<END;
# GNomEx Legacy Data

This is your lab's legacy data prior to 2018 exported from GNomEx. In most cases, this may be your only copy! **DO NOT DELETE!**

To preven accidental deletion, follow these guidelines:

- Limit access to division members with copy only privileges. Division administrators will always have full permission rights!

- Do NOT work in this directory. Create a new project, and copy your files to the new project. Copying files do not incur any additional charges. Work with your files in the new project.

- If do intend to delete, be careful regarding which file you select. Remember that deletion is permanent. **There is no undelete, backup, or recovery.**

GNomEx projects are organized into folders. The two top folders are **requests**, representing GNomEx Experiment Requests (example 1234R), and **analysis**, representing GNomEx Analysis projects (example A5678). Each project is in its own subfolder and are tagged with the project ID.

These files are archived to Amazon Glacier. They should be copied to a new project and 
restored there in order to view, download, or work with them.

Contact bioinformaticscore\@utah.edu if you have any questions.
END


######## Process command line options
my $infile;
my $outfile;
my $given_division;
my $given_title;
my $change = 0;
my $sb_path;
my $cred_path;
my $verbose = 0;

if (scalar(@ARGV) > 1) {
	GetOptions(
		'in=s'          => \$infile,
		'out=s'         => \$outfile,
		'div=s'         => \$given_division,
		'title=s'       => \$given_title,
		'change!'       => \$change,
		'sb=s'          => \$sb_path,
		'cred=s'        => \$cred_path,
		'verbose!'      => \$verbose,
	) or die "please recheck your options!\n\n$doc\n";
}
else {
	print $doc;
	exit;
}

#### Check options
if ($infile and not $outfile) {
	die "no outfile specified!\n";
}



#### Process individual
if ($given_division and $given_title) {
	my $result = process_division($given_division, $given_title);
	print $result;
	exit;
}




#### Open input file
unless ($infile) {
	die "no infile provided!\n";
}
my $fh = IO::File->new($infile, 'r') or 
	die "can't open '$infile'! $!\n";
my $header = $fh->getline;


#### Open output file
my $outfh = IO::File->new($outfile, 'w') or 
	die "can't open '$outfile'! $!\n";
$outfh->print("Division\tProjectFound\tMembers\tNumber\tNumberAdmin\tUpdated\n");

#### Process input list
while (my $line = $fh->getline) {
	
	# collect sb division and expected name
	chomp $line;
	my ($sb_division, $title) = split("\t", $line);
	
	# process
	my $result = process_division($sb_division, $title);
	$outfh->print($result);
}

$fh->close;
$outfh->close;
print "\nall done!\n";




sub process_division {
	my ($sb_division, $title) = @_;
	
	# Initialize
	my $sb = SB->new(
		division    => $sb_division,
		sb          => $sb_path,
		cred        => $cred_path,
	) or warn "unable to initialize sb command line tool!";
	$sb->verbose($verbose);

	# Query division projects
	my $legacy;
	foreach my $p ($sb->projects) {
		if ($p->name eq $title) {
			$legacy = $p;
			last;
		}
	}
	unless ($legacy) {
		return sprintf("%s\tNo\tNone\t0\t0\tN\n", $sb_division);
	}
	
	# members
	my @members;
	my $number_admins = 0;
	foreach my $m ($legacy->list_members) {
		push @members, $m->user;
		$number_admins++ if $m->admin;
	}
	
	# change
	my $changed;
	if ($change) {
		my $success = $legacy->update(
			name        => $new_name,
			description => $description,
		);
		if ($success and $legacy->name eq $new_name) {
			$changed = 'Y';
		}
		else {
			$changed = 'ERROR';
		}
	}
	else {
		$changed = 'N';
	}
	
	# finish
	print "finished with $sb_division\n";
	return sprintf("%s\tY\t%s\t%d\t%d\t%s\n", $sb_division, 
		join(',', @members), scalar(@members), $number_admins, $changed);
}


