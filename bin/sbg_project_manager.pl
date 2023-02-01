#!/usr/bin/perl

use warnings;
use strict;
use English qw(-no_match_vars);
use Getopt::Long qw(:config no_ignore_case);
use IO::File;
use List::Util qw(max);
use Net::SB;
use Net::SB::File;
use Net::SB::Folder;

our $VERSION = 1;


######## Documentation
my $doc = <<END;
An application to manage Seven Bridges Genomics projects.

Version: $VERSION

Example Usage:
    sbg_project_manager.pl [options]  -d big-shot-pi  -p experiment
    
    sbg_project_manager.pl [options] big-shot-pi/experiment[/folder1...]


Main function: Pick one only
    -P --listprojects           List all visible projects in division
                                    default if no options and no project given
    -l --list                   Recursively list all the files in project
                                    default if project given and no options
    -u --url                    Print signed download URLs for all found files
    -V --listvolumes            List available attached volumes
    -x --export                 Export files to the attached external volume
    --delete                    DELETE all (selected) files from the project!!!
                                   

Required:
    -d --division   <text>      Indicate the name of the division. Required.
    -p --project    <text>      Name of the SBG project. Required. If absent,
                                   automatically enables --listprojects

Options for file filtering:
    -r --dir        <text>      Name of the starting parent folder (optional)
    -F --filelist   <file>      Existing text file of files IDs to work on
                                    best to use saved output from this app
    -f --filter     <regex>     Perl Regular Expression for selecting files
                                May be used in with --filelist. Examples:
                                   '(bam|bai|bw)',
                                   '\\.fastq\\.gz\$'
                                   '(?:b|cr)a[mi](?:\\.(?:b|cr)ai)?\$'
    -D --printdir               Print folders in the list
 
Options for download URLs:
    --aria                      Format download URLs as an aria2c input file

Options for volume export:
    --volume        <text>      Name of the attached external volume
    --prefix        <text>      Prefix used for new file path on external volume
                                  default is project name
    --copy                      Copy files only, don't move and link (default)
    --overwrite                 Overwrite pre-existing files (default false)
    --wait          <int>       Wait time between status checks (30 seconds)

General:
    --cred          <file>      Path to SBG credentials file 
                                  default ~/.sevenbridges/credentials
    -v --verbose                Print all https processing commands

END





######## Process command line options
my $list_projects  = 0;
my $list_files     = 0;
my $list_volumes   = 0;
my $download_links = 0;
my $export_files   = 0;
my $delete_files   = 0;
my $division_name;
my $project_name;
my $remote_dir_name;
my $filelist_name;
my $file_filter;
my $print_folders;
my $aria_formatting;
my $volume_name;
my $vol_prefix;
my $vol_copy = 0;
my $vol_overwrite = 0;
my $vol_connection_file;
my $wait_time = 30;
my $credentials_file;
my $verbose;
my $help;

if (scalar(@ARGV) > 0) {
	GetOptions(
		'P|listprojects!'   => \$list_projects,
		'l|list!'           => \$list_files,
		'V|listvolumes!'    => \$list_volumes,
		'u|url!'            => \$download_links,
		'x|export!'         => \$export_files,
		'delete!'           => \$delete_files,
		'd|division=s'      => \$division_name,
		'p|project=s'       => \$project_name,
		'r|dir=s'           => \$remote_dir_name,
		'F|filelist=s'      => \$filelist_name,
		'f|filter=s'        => \$file_filter,
		'D|printdir!'       => \$print_folders,
		'aria!'             => \$aria_formatting,
		'volume=s'          => \$volume_name,
		'prefix=s'          => \$vol_prefix,
		'copy!'             => \$vol_copy,
		'overwrite!'        => \$vol_overwrite,
		'wait=i'            => \$wait_time,
		'connection=s'      => \$vol_connection_file,
		'cred=s'            => \$credentials_file,
		'v|verbose!'        => \$verbose,
		'h|help!'           => \$help,
	) or die "please recheck your options!! Run with --help to display options\n";
}
else {
	print $doc;
	exit 0;
}




######## Check options

if ($help) {
	print $doc;
	exit 0;
}
check_options();
my $list_header = 'Type ID                         Size  Status         FilePath';


######## Initialize SBG object
my $Sb = Net::SB->new(
	div        => $division_name,
	cred       => $credentials_file,
	verbose    => $verbose,
	sleepvalue => $wait_time,
) or die " unable to initialize SB object!\n";

my $Project;
if ($project_name) {
	$Project = $Sb->get_project($project_name) or
		die " unable to find project '$project_name'!";
}
elsif ($list_projects or $list_volumes) {
	# no project needed
}
else {
	unless ($list_projects) {
		die " no project name given!\n";
	}
}




######## Main functions

if ($list_projects) {
	print_project_list();
	exit 0;
}
elsif ($list_volumes) {
	print_volume_list();
	exit 0;
}
elsif ($list_files) {
	print_project_file_list();
	exit 0;
}
elsif ($download_links) {
	print_download_file_links();
	exit 0;
}
elsif ($export_files) {
	export_files_to_volume();
	exit 0;
}
elsif ($delete_files) {
	delete_platform_files();
	exit 0;
}
else {
	die "no function requested!?";
}





######## Functions

sub check_options {
	if (scalar @ARGV == 1 and not $division_name and not $project_name) {
		my $a = shift @ARGV;
		($division_name, $project_name, $remote_dir_name) = split m{/}, $a, 3;
	}
	unless ($division_name) {
		die " SBG division name is required!\n";
	}
	my $check = $list_files + $download_links + $export_files + $delete_files
		+ $list_projects + $list_volumes;
	if ($project_name and $check == 0) {
		$list_files = 1;
		$check = 1;
	}
	elsif (not $project_name and $check == 0) {
		$list_projects = 1;
		$check = 1;
	}
	if ($check == 0) {
		die " Must pick a single function: see help!\n";
	}
	elsif ($check > 1) {
		die " Can only pick one function: see help!\n";
	}
	$vol_prefix = $project_name;
}


sub print_project_list {
	my $projects = $Sb->list_projects;
	if (@{$projects}) {
		my $len = max( map {length} map {$_->id} @{$projects} );
		my $formatter = '%-' . $len . 's %s' . "\n";
		printf $formatter, 'ID', 'Name';
		foreach my $p (@{$projects}) {
			printf $formatter, $p->id, $p->name;
		}
	}
	else {
		print " No projects to list!\n";
	}
}

sub print_volume_list {
	my $volumes = $Sb->list_volumes;
	if ($volumes and scalar @{$volumes}) {
		my $len = max( map {length} map {$_->id} @{$volumes} );
		my $formatter = '%-' . $len . 's %s' . "\n";
		printf $formatter, 'ID', 'Name';
		foreach my $p (@{$volumes}) {
			printf $formatter, $p->id, $p->name;
		}
	}
	else {
		print " No volumes to list!\n";
	}
}

sub collect_files {
	my $files;
	die " Can't connect to Project!" unless $Project;
	if ($filelist_name) {
		return load_files_from_file();
	}
	if ($remote_dir_name) {
		my $folder = $Project->get_file_by_name($remote_dir_name) or
			die " unable to find remote folder '$remote_dir_name'!\n";
		$files = $folder->recursive_list($file_filter);
	}
	else {
		$files = $Project->recursive_list($file_filter);
	}
	return $files;
}

sub load_files_from_file {
	my $fh = IO::File->new($filelist_name) or
		die "unable to load file '$filelist_name'! $OS_ERROR\n";
	my @objects;
	my $fail = 0;
	while (my $line = $fh->getline) {
		chomp $line;
		my $type = substr($line,0,4);
		if ($type eq 'File'){
			# File formatter: 'File %s %6s  %-13s  %s'
			if ($line =~ 
				/File\ ([a-z0-9]{24}) \s+ ([\d\.KMG]+) \s+ [\w\-\.:]+ \ \ (.+) $/x
			) {
				my $id   = $1;
				my $size = $2;
				my $path = $3;
				
				## no critic - don't need x
				if ($file_filter and $path =~ /$file_filter/) {
					next;
				}
				## use critic
				
				if ($size =~ /^ (\d+\.\d) G $/x ) {
					$size = $1 * 1073741824;
				}
				elsif ($size =~ /^ (\d+\.\d) M $/x ) {
					$size = $1 * 1048576;
				}
				elsif ($size =~ /^ (\d+\.\d) K $/x ) {
					$size = $1 * 1024;
				}
				my @bits     = split m|/|, $path;
				my $filename = pop @bits;
				if (@bits) {
					$path = join '/', @bits;
				}
				else {
					$path = q();
				}
				
				my $file = {
					id      => $id,
					size    => $size,
					name    => $filename,
					path    => $path,
					project => $project_name,
					href    => sprintf("%s/files/%s", $Sb->endpoint, $id),
				};
				push @objects, Net::SB::File->new( $Project, $file );
			}
			else {
				$fail++;
			}
		}
		elsif ($type eq 'Dir ') {
			# Directory formatter: 'Dir  %s      0  Platform       %s'
			if ($line =~ 
				/^Dir \ \ ([a-z0-9]{24}) \ {6} 0 \ \ Platform \ {7} (.+) $/x
			) {
				my $id   = $1;
				my $path = $2;
				
				## no critic - don't need x
				if ($file_filter and $path =~ /$file_filter/) {
					next;
				}
				## use critic
				
				my @bits = split m|/|, $path;
				my $name = pop @bits;				
				my $folder = {
					id      => $id,
					name    => $name,
					path    => $path,
					project => $project_name,
					href    => sprintf("%s/files/%s", $Sb->endpoint, $id),
				};
				push @objects, Net::SB::Folder->new( $Project, $folder );
			}
			else {
				$fail++;
			}
		}
		elsif ($line eq $list_header) {
			# we are good, correct format
		}
		else {
			$fail++;
		}
	}
	$fh->close;
	if ($fail) {
		print STDERR " there were $fail unrecognizeable lines skipped\n";
	}
	return \@objects;
}

sub print_project_file_list {
	# collect file list from the project
	my $files = collect_files();
	unless (@{$files}) {
		print " No files to list!\n";
		return;
	}

	# bulk collect details for file status and size
	$Sb->bulk_get_file_details($files);

	# print the file names
	# the file IDs is always 24 characters long
	# Status and path will be variable
	printf "%s\n", $list_header;
	my @f = map {$_->[1]}
			sort {$a->[0] cmp $b->[0]}
			map { [ $_->type eq 'file' ? $_->pathname : $_->path, $_ ] } @{$files};
	my $total = 0;
	foreach (@f) {
		if ($_->type eq 'folder') {
			next unless $print_folders;
			printf "Dir  %s      0  Platform       %s\n", $_->id, $_->path;
		}
		else {
			my $size = $_->size || 0;
			printf "File %s %6s  %-13s  %s\n", $_->id, format_human_size($size),
				substr($_->file_status,0,14), $_->pathname;
			$total += $size;
		}
	}
	# print summary
	if ($file_filter) {
		printf "\nTotal size of selected files in %s is %s\n",
			join('/', $division_name, $project_name, $remote_dir_name || q()),
			format_human_size($total);
	}
	else {
		printf "\nTotal size for %s is %s\n",
			join('/', $division_name, $project_name, $remote_dir_name || q()),
			format_human_size($total);
	}
	return;
}


sub print_download_file_links {
	# collect file list from the project
	my $files = collect_files();
	# generate download links
	my @links;
	my @nolinks;
	foreach my $f (@{$files}) {
		next if $f->type eq 'folder';
		my $url = $f->download_link;
		if ($url) {
			push @links, [$f->pathname, $url];
		}
		else {
			push @nolinks, $f->pathname;
		}
	}
	# print the links
	if ($aria_formatting) {
		foreach my $l (@links) {
			printf "%s\n  out=%s\n", $l->[1], $l->[0];
		}
	}
	else {
		foreach my $l (@links) {
			printf "%s\n", $l->[1];
		}
	}
	# print error for those without links
	if (@nolinks) {
		foreach my $f (@nolinks) {
			print STDERR " # no link for $f\n";
		}
	}
	return;
}

sub export_files_to_volume {
	# check volume
	my $Volume = $Sb->get_volume($volume_name);
	if ($Volume and ref($Volume) eq 'Net::SB::Volume' ) {
		# external volume is already attached
		# make sure it is active
		unless ($Volume->active) {
			$Volume->activate;
		}
	}
	else {
# 		if ($vol_connection_file) {
# 			print STDERR "  volume not found, attempting to mount\n";
# 			my $fh = IO::File->new($vol_connection_file) or
# 				die "unable to open file '$vol_connection_file'! $OS_ERROR\n";
# 			my %options = (
# 				name  => $volume_name
# 			);
# 			while (my $line = $fh->getline) {
# 				chomp $line;
# 				next if substr($line, 0, 1) eq '#';
# 				next unless $line =~ /\w+/;
# 				my ($key, $value) = split /\s+=?\s*/, $line;
# 				$options{$key} = $value;
# 			}
# 			$fh->close;
# 			$Volume = $Sb->attach_volume(%options) or
# 				die "unable to attach requested volume!\n";
# 		}
# 		else {
# 			die " requested volume '$volume_name' is not attached!\n";
# 		}
		print 
" Requested volume '$volume_name' is not attached! Double check or attach\n";
		exit 1;
	}

	# collect file list from the project
	my $files = collect_files();
	printf STDERR " exporting %d files to $volume_name/$vol_prefix\n", scalar @{$files};

	# export files
	my @options = (
		files     => $files,
		overwrite => $vol_overwrite,
		copy      => $vol_copy,
		prefix    => $vol_prefix,
	);
	my $results = $Volume->export_files(@options);

	# print results
	# file ID is 24 characters, export ID is 32 characters
	print "FileID                   ExportID                         Status     File\n";
	my @ids = map {$_->[1]}
			  sort {$a->[0] cmp $b->[0]}
			  map { [ $results->{$_}{source}, $_ ] }
			  keys %{$results};
	foreach my $id (@ids) {
		if ($results->{$id}{status} eq 'FAILED') {
			# there was an error, state why if possible
			# I don't have all the error codes here
			my $error;
			if (exists $results->{$id}{error}) {
				if ($results->{$id}{error} == 9107) {
					$error = sprintf "%s already exists", $results->{$id}{destination};
				}
				elsif ($results->{$id}{error} == 9006) {
					$error = sprintf "%s cannot be exported", $results->{$id}{source};
				}
				else {
					$error = sprintf "Error %d for %s", $results->{$id}{error},
						$results->{$id}{source};
				}
			}
			else {
				$error = sprintf "unknown error for %s", $results->{$id}{source};
			}
			printf "%s                                 %s %-10s %s\n", $id, $results->{$id}{transfer_id},
				$results->{$id}{status}, $error;
		}
		else {
			printf "%s %s %-10s %s\n", $id, $results->{$id}{transfer_id},
				$results->{$id}{status}, $results->{$id}{destination};
		}
	}
}

sub delete_platform_files {
	# collect file list from the project
	my $files = collect_files();
	printf " !! Deleting %d files !!\n", scalar @{$files};
	my $results = $Sb->bulk_delete($files);
	foreach my $r (@{$results}) {
		print "$r\n";
	}
}

sub format_human_size {
	my $size = shift;
	if ($size > 1000000000) {
		return sprintf "%.1fG", ($size / 1073741824);
	}
	elsif ($size > 1000000) {
		return sprintf "%.1fM", ($size / 1048576);
	}
	elsif ($size > 1000) {
		return sprintf "%.1fK", ($size / 1024);
	}
	else {
		return sprintf "%d", $size;
	}
}
