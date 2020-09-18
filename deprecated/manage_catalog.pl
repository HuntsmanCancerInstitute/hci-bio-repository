#!/usr/bin/env perl

use strict;
use warnings;
use IO::File;
use Getopt::Long;
use Time::Local;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use RepoCatalog;
use RepoProject;

my $VERSION = 5;


######## Documentation
my $doc = <<END;
A script to manage a catalog. Catalogs are local database files for storing 
metadata regarding GNomEx projects, including information extracted from 
the GNomEx LIMS SQL database, Seven Bridges lab division information, and 
file directory information from the file server, as well as dates for various 
steps in processing. Catalogs may be exported/imported as tab-delimited files.

manage_catalog.pl --cat <file.db> <options>

  Required:
    --cat <path>              Provide the path to a catalog file
  
  Entry selection: (select one)
    --list <path>             File of project identifiers to work on
                                may be tab-delimited, only 1st column used
    --list_req_up             Print or work on Request IDs for upload to SB
    --list_req_hide           Print or work on Request IDs for hiding
    --list_req_delete         Print or work on Request IDs for deletion
    --list_anal_up            Print or work on Analysis IDs for uploading to SB
    --list_anal_hide          Print or work on Analysis IDs for hiding
    --list_anal_delete        Print or work on Analysis IDs for deletion
    --list_lab <pi_lastname>  Print or select based on PI last name
    --all                     Apply to all catalog entries
  
  Selection modifiers
    --year <YYYY>             Filter entries to given year or newer
    --age <days>              Filter entries for minimum age
    --max_age <days>          Filter entries for maximum age
    --size <bytes>            Filter entries for minimum project size
                                allows K, M, and G suffix
    --sb                      Include only projects with SB division
    --nosb                    Exclude projects with SB division
    --external                Include only external projects (assumes no SB division)
    --noexternal              Exclude external projects (assumes no SB division)
    
  Action on entries: 
    --status                  Print the status of listed projects
    --info                    Print basic information of listed projects
    --path                    Print the repository path to the project
    --print                   Print all the information of listed projects
    --scan_size_age           Update project size and age from file server
    --update_size             Import current (col2) previous (col3) sizes from listfile
    --update_scan <YYYYMMDD>  Update project scan timestamp
    --update_del <YYYYMMDD>   Update project deletion timestamp
    --update_hide <YYYYMMDD>  Update project hide timestamp
    --update_up <YYYYMMDD>    Update project upload timestamp
    --update_em <YYYYMMDD>    Update project email timestamp
    --update_sb <text>        Update SB division name. Use 'none' to clear.
    
  Action on catalog:
    --export <path>           Dump the contents to tab-delimited text file
    --transform               When exporting transform to human conventions
    --import <path>           Import an exported table, requires non-transformed
    
    --optimize                Run the db file optimize routine (!?)
    
END
 




####### Input
my $cat_file;
my $list_req_upload = 0;
my $list_req_hide = 0;
my $list_req_delete = 0;
my $list_anal_upload = 0;
my $list_anal_hide = 0;
my $list_anal_delete = 0;
my $list_pi = 0;
my $list_file;
my $list_all = 0;
my $year;
my $include_sb;
my $min_age;
my $max_age;
my $min_size;
my $external;
my $show_status = 0;
my $show_info = 0;
my $show_path = 0;
my $print_info = 0;
my $scan_size_age;
my $import_sizes = 0;
my $update_scan_date;
my $update_hide_date;
my $update_upload_date;
my $update_delete_date;
my $update_email_date;
my $update_division;
my $dump_file;
my $transform = 0;
my $import_file;
my $run_optimize;
my $force;

if (scalar(@ARGV) > 1) {
	GetOptions(
		'cat=s'             => \$cat_file,
		'list_req_up!'      => \$list_req_upload,
		'list_req_hide!'    => \$list_req_hide,
		'list_req_delete'   => \$list_req_delete,
		'list_anal_up!'     => \$list_anal_upload,
		'list_anal_hide!'   => \$list_anal_hide,
		'list_anal_delete'  => \$list_anal_delete,
		'list_lab|list_pi=s' => \$list_pi,
		'list=s'            => \$list_file,
		'all!'              => \$list_all,
		'sb!'               => \$include_sb,
		'year=i'            => \$year,
		'age=i'             => \$min_age,
		'max_age=i'         => \$max_age,
		'size=s'            => \$min_size,
		'external!'         => \$external,
		'status!'           => \$show_status,
		'info!'             => \$show_info,
		'path!'             => \$show_path,
		'print!'            => \$print_info,
		'scan_size_age!'    => \$scan_size_age,
		'update_size!'      => \$import_sizes,
		'update_scan=i'     => \$update_scan_date,
		'update_hide=i'     => \$update_hide_date,
		'update_up=i'       => \$update_upload_date,
		'update_del=i'      => \$update_delete_date,
		'update_email=i'    => \$update_email_date,
		'update_sb=s'       => \$update_division,
		'export=s'          => \$dump_file,
		'import=s'          => \$import_file,
		'force!'            => \$force,
		'transform!'        => \$transform,
		'optimize!'         => \$run_optimize,
	) or die "please recheck your options!\n\n";
}
else {
	print $doc;
	exit;
}

unless ($cat_file) {
	die "No catalog file provided!\n";
}

# sanity checks
{
	# request search
	my $sanity = $list_req_upload + $list_req_hide + $list_req_delete + $list_all + $list_pi;
	if ($sanity > 1) {
		die "Only 1 Request search allowed at a time!\n";
	}
	elsif ($sanity == 1) {
		die "No search functions if exporting!\n" if $dump_file;
		die "No search functions if importing!\n" if $import_file;
	}
	
	# analysis search
	$sanity = 0;
	$sanity = $list_anal_upload + $list_anal_hide + $list_anal_delete + $list_all + $list_pi;
	if ($sanity > 1) {
		die "Only 1 Analysis search allowed at a time!\n";
	}
	elsif ($sanity == 1) {
		die "No search functions if exporting!\n" if $dump_file;
		die "No search functions if importing!\n" if $import_file;
	}
	
	# print function
	$sanity = 0;
	$sanity = $show_status + $show_info + $show_path + $print_info;
	if ($sanity > 1) {
		die "Only 1 printing function allowed at a time!\n";
	}
}
if ($year and $year !~ /\d{4}/) {
	die "year must be four digits!\n";
}
if (defined $external) {
	$external = $external ? 'Y' : 'N';
	$include_sb = 0 if $external eq 'Y';
}
if ($min_size) {
	# we're using base10 size rather than base2 for simplicity, too many rounding errors
	if ($min_size =~ /^([\d]+)k$/i) {
		$min_size = $1 * 1000;
	}
	elsif ($min_size =~ /^(\d+)m$/i) {
		$min_size = $1 * 1000000;
	}
	elsif ($min_size =~ /^(\d+)g$/i) {
		$min_size = $1 * 1000000000;
	}
	elsif ($min_size =~ /^\d+$/) {
		# this is ok, just a number
	}
	else {
		die "Minimum size filter must be integer! K, M, and G suffix is allowed\n"
	}
}


####### Open Catalog
my $Catalog = RepoCatalog->new($cat_file) or 
	die "Cannot open catalog file '$cat_file'!\n";

# import catalog is done 
if ($import_file) {
	my $n = $Catalog->import_from_file($import_file, $force);
	if ($n) {
		print " Imported $n records from file '$import_file'\n";
	}
	else {
		print " Import from file '$import_file' failed!\n";
	}
	exit;
}





####### List functions
my @action_list;
if ($list_file) {
	my $fh = IO::File->new($list_file) or 
		die "Cannot open import file '$list_file'! $!\n";
	my $header = $fh->getline;
	while (my $l = $fh->getline) {
		chomp $l;
		push @action_list, $l;
	}
	printf " loaded %d lines from $list_file\n", scalar(@action_list);
	$fh->close;
}
elsif (@ARGV) {
	@action_list = @ARGV;
	# printf " using %d items provided on command line\n", scalar(@action_list);
}
elsif ($list_all) {
	@action_list = $Catalog->list_all(
		age      => $min_age, 
		maxage   => $max_age,
		year     => $year, 
		sb       => $include_sb,
		external => $external,
		size     => $min_size,
	);
}
elsif ($list_req_upload) {
	die "Can't find entries if list provided!\n" if @action_list;
	@action_list = $Catalog->find_requests_to_upload(
		year     => $year,
		age      => $min_age,
		maxage   => $max_age,
		size     => $min_size,
	);
}
elsif ($list_req_hide) {
	die "Can't find entries if list provided!\n" if @action_list;
	@action_list = $Catalog->find_requests_to_hide(
		age      => $min_age, 
		maxage   => $max_age,
		year     => $year, 
		sb       => $include_sb,
		external => $external,
		size     => $min_size,
	);
}
elsif ($list_req_delete) {
	die "Can't find entries if list provided!\n" if @action_list;
	@action_list = $Catalog->find_requests_to_delete(
		age      => $min_age, 
		maxage   => $max_age,
		year     => $year, 
		sb       => $include_sb,
		external => $external,
		size     => $min_size,
	);
}
elsif ($list_anal_upload) {
	die "Can't find entries if list provided!\n" if @action_list;
	@action_list = $Catalog->find_analysis_to_upload(
		age      => $min_age, 
		maxage   => $max_age,
		year     => $year, 
		size     => $min_size,
	);
}
elsif ($list_anal_hide) {
	die "Can't find entries if list provided!\n" if @action_list;
	@action_list = $Catalog->find_analysis_to_hide(
		age      => $min_age, 
		maxage   => $max_age,
		year     => $year, 
		sb       => $include_sb,
		external => $external,
		size     => $min_size,
	);
}
elsif ($list_anal_delete) {
	die "Can't find entries if list provided!\n" if @action_list;
	@action_list = $Catalog->find_analysis_to_delete(
		age      => $min_age, 
		maxage   => $max_age,
		year     => $year, 
		sb       => $include_sb,
		external => $external,
		size     => $min_size,
	);
}
elsif ($list_pi) {
	die "Can't find entries if list provided!\n" if @action_list;
	@action_list = $Catalog->list_projects_for_pi($list_pi);
}




####### Action Functions

if ($scan_size_age) {
	unless (@action_list) {
		die "No list provided to scan filesystem for size and age!\n";
	}
	my $i = 0;
	foreach my $item (@action_list) {
		my ($id, @rest) = split(m/\s+/, $item);
		next unless (defined $id);
		my $Entry = $Catalog->entry($id) or next;
		my $Project = RepoProject->new($Entry->path) or next;
		my ($size, $age) = $Project->get_size_age;
		if ($size) {
			$Entry->size($size);
		}
		if ($age) {
			$Entry->youngest_age($age);
		}
		$i++;
	}
	print " Collected and updated size and age stats for $i entries\n";
}


if ($import_sizes) {
	unless (@action_list) {
		die "No list provided to import sizes!\n";
	}
	foreach my $item (@action_list) {
		my ($id, $size, $previous_size) = split(m/\s+/, $item);
		next unless (defined $id);
		my $Entry = $Catalog->entry($id) or next;
		if (defined $size and defined $previous_size) {
			$Entry->size($previous_size); # this one first
			$Entry->size($size); # then the current one
		}
		elsif (defined $size) {
			$Entry->size($size);
		}
	}
}

if (defined $update_scan_date and $update_scan_date =~ /(\d\d\d\d)(\d\d)(\d\d)/) {
	my $time = timelocal(0, 0, 12, $3, $2 - 1, $1);
	print " Setting scan time ($update_scan_date) to $time\n";
	unless (@action_list) {
		die "No list provided to update scan times!\n";
	}
	foreach my $item (@action_list) {
		my ($id, @rest) = split(m/\s+/, $item);
		next unless (defined $id);
		my $Entry = $Catalog->entry($id) or next;
		$Entry->scan_datestamp($time);
	}
}

if (defined $update_upload_date and $update_upload_date =~ /(\d\d\d\d)(\d\d)(\d\d)/) {
	my $time = timelocal(0, 0, 12, $3, $2 - 1, $1);
	print " Setting upload time ($update_upload_date) to $time\n";
	unless (@action_list) {
		die "No list provided to update upload times!\n";
	}
	foreach my $item (@action_list) {
		my ($id, @rest) = split(m/\s+/, $item);
		next unless (defined $id);
		my $Entry = $Catalog->entry($id) or next;
		$Entry->upload_datestamp($time);
	}
}

if (defined $update_hide_date and $update_hide_date =~ /(\d\d\d\d)(\d\d)(\d\d)/) {
	my $time = timelocal(0, 0, 12, $3, $2 - 1, $1);
	print " Setting hide time ($update_hide_date) to $time\n";
	unless (@action_list) {
		die "No list provided to update hide times!\n";
	}
	foreach my $item (@action_list) {
		my ($id, @rest) = split(m/\s+/, $item);
		next unless (defined $id);
		my $Entry = $Catalog->entry($id) or next;
		$Entry->hidden_datestamp($time);
	}
}

if (defined $update_delete_date and $update_delete_date =~ /(\d\d\d\d)(\d\d)(\d\d)/) {
	my $time = timelocal(0, 0, 12, $3, $2 - 1, $1);
	print " Setting delete time ($update_delete_date) to $time\n";
	unless (@action_list) {
		die "No list provided to update delete times!\n";
	}
	foreach my $item (@action_list) {
		my ($id, @rest) = split(m/\s+/, $item);
		next unless (defined $id);
		my $Entry = $Catalog->entry($id) or next;
		$Entry->deleted_datestamp($time);
	}
}

if (defined $update_email_date and $update_email_date =~ /(\d\d\d\d)(\d\d)(\d\d)/) {
	my $time = timelocal(0, 0, 12, $3, $2 - 1, $1);
	print " Setting email time ($update_email_date) to $time\n";
	unless (@action_list) {
		die "No list provided to update email times!\n";
	}
	foreach my $item (@action_list) {
		my ($id, @rest) = split(m/\s+/, $item);
		next unless (defined $id);
		my $Entry = $Catalog->entry($id) or next;
		$Entry->emailed_datestamp($time);
	}
}

if (defined $update_division) {
	print " Setting SB division name to $update_division\n";
	unless (@action_list) {
		die "No list provided to update division name!\n";
	}
	if ($update_division eq 'none') {
		$update_division = '';
	}
	foreach my $item (@action_list) {
		my ($id, @rest) = split(m/\s+/, $item);
		next unless (defined $id);
		my $Entry = $Catalog->entry($id) or next;
		$Entry->division($update_division);
	}
}




####### Printing Functions

if ($show_status) {
	unless (@action_list) {
		die "No list provided to show status!\n";
	}
	printf "%-6s\t%-6s\t%-5s\t%-10s\t%-10s\t%-10s\t%-10s\t%-10s\n", qw(ID Size Age Scan Upload Hide Delete Division);
	foreach my $item (@action_list) {
		my ($id, @rest) = split(m/\s+/, $item);
		next unless (defined $id);
		my $Entry = $Catalog->entry($id) or next;
		
		# size - work with decimal instead of binary for simplicity
		my $size = $Entry->size || 0;
		if ($size > 1000000000) {
			$size = sprintf("%.1fG", $size / 1000000000);
		}
		elsif ($size > 1000000) {
			$size = sprintf("%.1fM", $size / 1000000);
		}
		elsif ($size > 1000) {
			$size = sprintf("%.1fK", $size / 1000);
		}
		else {
			$size = sprintf("%dB", $size);
		}
		
		# the datetime stamps are converted from epoch to YYYY-MM-DD
		# if not set, just prints blanks
		# since I'm using local time and not gm time, funny things happen regarding 
		# the year when I try to set it to epoch time (0). I get back either year 1969 or 
		# 1970, so I need to check both.
		
		# scan day
		my @scan   = localtime($Entry->scan_datestamp || 0);
		my $scan_day = ($scan[5] == 69 or $scan[5] == 70) ? '          ' : 
			sprintf("%04d-%02d-%02d", $scan[5] + 1900, $scan[4] + 1, $scan[3]);
		# upload day
		my @upload = localtime($Entry->upload_datestamp || 0);
		my $up_day = ($upload[5] == 69 or $upload[5] == 70) ? '          ' : 
			sprintf("%04d-%02d-%02d", $upload[5] + 1900, $upload[4] + 1, $upload[3]);
		# hide day
		my @hide   = localtime($Entry->hidden_datestamp || 0);
		my $hide_day = ($hide[5] == 69 or $hide[5] == 70) ? '          ' : 
			sprintf("%04d-%02d-%02d", $hide[5] + 1900, $hide[4] + 1, $hide[3]);
		# delete day
		my @delete = localtime($Entry->deleted_datestamp || 0);
		my $delete_day = ($delete[5] == 69 or $delete[5] == 70) ? '          ' : 
			sprintf("%04d-%02d-%02d", $delete[5] + 1900, $delete[4] + 1, $delete[3]);
		
		# print
		printf "%-6s\t%-7s\t%-5s\t%s\t%s\t%s\t%s\t%s\n", $id, $size, $Entry->age || '', 
			$scan_day, $up_day, $hide_day, $delete_day, 
			$Entry->external eq 'Y' ? 'external' : $Entry->division || 'none';
	}
}
elsif ($show_info) {
	unless (@action_list) {
		die "No list provided to show status!\n";
	}
	printf "%-6s\t%-10s\t%-16s\t%-16s\t%s\n", qw(ID Date UserLastName LabLastName Name);
	foreach my $item (@action_list) {
		my ($id, @rest) = split(m/\s+/, $item);
		next unless (defined $id);
		my $Entry = $Catalog->entry($id) or next;
		
		printf "%-6s\t%-10s\t%-16s\t%-16s\t%s\n", $id, $Entry->date, $Entry->user_last,
			$Entry->lab_last, $Entry->name;
	}
}
elsif ($show_path) {
	unless (@action_list) {
		die "No list provided to show status!\n";
	}
	foreach my $item (@action_list) {
		my ($id, @rest) = split(m/\s+/, $item);
		next unless (defined $id);
		my $Entry = $Catalog->entry($id) or next;
		
		printf "%s\n", $Entry->path;
	}
}
elsif ($print_info) {
	unless (@action_list) {
		die "No list provided to show status!\n";
	}
	printf "ID\tPath\tName\tDate\tGroup\tUserEmail\tUserFirst\tUserLast\tLabFirst\tLabLast\tPIEmail\tDivision\tURL\tExternal\tStatus\tApplication\tOrganism\tGenome\tSize\tLastSize\tAge\tScan\tUpload\tHidden\tDeleted\tEmailed\n";
	foreach my $item (@action_list) {
		my ($id, @rest) = split(m/\s+/, $item);
		next unless (defined $id);
		my $Entry = $Catalog->entry($id) or next;
		print($Entry->print_string($transform));
	}
	
}
elsif (
	# check to see if we have a list of IDs that were from a search
	@action_list and
	($list_req_upload or $list_req_hide or $list_req_delete or
	 $list_anal_upload or $list_anal_hide or $list_anal_delete or 
	 $list_pi or $list_all)
) {
	# just print them
	printf "%s\n", join("\n", @action_list);
}



####### More Catalog functions

if ($dump_file) {
	my $s = $Catalog->export_to_file($dump_file, $transform);
	if ($s) {
		print " Exported to file '$dump_file'\n";
	}
	else {
		print " Export to file '$dump_file' failed!\n";
	}
}

if ($run_optimize) {
	print " Optimizing...\n";
	$Catalog->optimize;
}








