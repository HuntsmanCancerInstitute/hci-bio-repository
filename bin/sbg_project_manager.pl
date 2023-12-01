#!/usr/bin/env perl

use warnings;
use strict;
use English qw(-no_match_vars);
use Getopt::Long qw(:config no_ignore_case);
use IO::File;
use IO::Handle;
use List::Util qw(max);
use Net::SB;
use Net::SB::File;
use Net::SB::Folder;

our $VERSION = 1.7;


######## Documentation
my $doc = <<END;

An application to manage Seven Bridges Genomics projects and their files.

It can generate a recursive list of files and directories in a project, 
including file IDs, size, location, and pathname. Importantly, recursive 
lists can be generated starting from nested folder if the path is already
known. File lists can filtered by providing a regular expression. Furthermore, 
file list output may be redirected to a text file, manipulated as necessary,
and provided as an input file to this program.

NOTE: Recursive listing is default behavior. Use --limit option to restrict
recursive depth.

File manipulation includes generating download URLs, copying files to 
another project, moving files to a folder within the same project, exporting 
the files to a mounted volume such as an AWS bucket (either copy-and-link or
move), or file deletion.

This requires a Seven Bridges Developer token, usually stored in the file
~/.sevenbridges/credentials. It assumes the profile name is the same as the
lab division name (Enterprise account) or user account (personal), but will
use "default" if it is not found.

Version: $VERSION

Usage:
    sbg_project_manager.pl [options]  -d big-shot-pi  -p experiment
    
    sbg_project_manager.pl [options] big-shot-pi/experiment[/folder1...]
    
Example Usage:

    # generate list
    sbg_project_manager.pl big-shot-pi/experiment -o list.txt

    # manually edit
    nano list.txt

    # perform file functions
    sbg_project_manager big-shot-pi -F list.txt --copy --dest project2
    sbg_project_manager big-shot-pi -F list.txt --url --aria -o downloads.txt
    sbg_project_manager big-shot-pi -F list.txt --delete
    
   


Main function: Pick one only
    -P --listprojects           List all visible projects in division
                                    default if no options and no project given
    -l --list                   Recursively list all the files in project
                                    default if project given and no options
                                    recursive list starts at provided folder
    -y --summary                Print a file count and size summary only
    -u --url                    Print signed download URLs for all found files
    -V --listvolumes            List available attached volumes
    -x --export                 Export files to the attached external volume
    -c --copy                   Copy the files to the indicated project
    -m --move                   Move the files to the indicated folder
                                    (Copy/Move does not currently preserve folders!)
    --delete                    DELETE all (selected) files from the project!!!
    --deleteproject             DELETE the project!!!!
                                   

Required:
    -d --division   <text>      Indicate the name of the division. Required.
    -p --project    <text>      Name of the SBG project. Required. If absent,
                                   automatically enables --listprojects

Options for file filtering:
    -r --dir        <text>      Name of the starting parent folder (optional)
    -F --filelist   <file>      Existing text file of files IDs to work on
                                    best to use saved output from this app
    -f --filter     <regex>     Perl Regular Expression for selecting file names
                                    May be used with --filelist. Examples:
                                     '(bam|bai|bw)',
                                     '\\.fastq\\.gz\$'
                                     '(?:b|cr)a[mi](?:\\.(?:b|cr)ai)?\$'
    -t --task       <text>      Select files originated from analysis task id
                                    Will recurse into child tasks, but not folders.
                                    Specify folder if files are moved.
    --location      <regex>     Perl Regular Expression for selecting for location
                                    Examples include:
                                    'platform'
                                    'us\\-east\\-\\d'
                                    'cb\\-big\\-shot\\-.+'
    -D --printdir               Print folders in the list
    --limit --depth <integer>   Limit recursive depth to specified depth
 
Options for download URLs:
    --aria                      Format download URLs as an aria2c input file
                                    necessary to preserve folder structure
    --batch         <int>       Split into batches of indicated size (GB)
                                    must use --out option

Options for bulk volume export:
    --volume        <text>      Name of the attached external volume
    --prefix        <text>      Prefix used for new file path on external volume
                                  Default is project name. The entire file path and 
                                  folder structure is preserved.
    --volcopy                   Copy only to volume, do not move and link (the default)
    --overwrite                 Overwrite pre-existing files (default false)
    --wait          <int>       Wait time between status checks (20 seconds)

Options for file copy/move:
    --destination   <text>      The [project]/[folder] destination.
                                    Copy should be a project/[folder]
                                    Move should be a folder within same project
    --new                       Indicate that the destination should be created

General:
    -o --out        <file>      Output filename, default standard output
    --cred          <file>      Path to SBG credentials file 
                                  default ~/.sevenbridges/credentials
    -v --verbose                Print all https processing commands

END





######## Process command line options
my $list_projects  = 0;
my $list_files     = 0;
my $sum_files      = 0;
my $list_volumes   = 0;
my $download_links = 0;
my $export_files   = 0;
my $copy_files     = 0;
my $move_files     = 0;
my $delete_files   = 0;
my $delete_project = 0;
my $division_name;
my $project_name;
my $remote_dir_name;
my $filelist_name;
my $file_filter;
my $task_id;
my $location_filter;
my $print_folders;
my $recurse_limit  = 0;
my $aria_formatting;
my $batch_size = 0;
my $volume_name;
my $vol_prefix;
my $vol_copy = 0;
my $vol_overwrite = 0;
my $vol_connection_file;
my $wait_time = 20;
my $destination;
my $new_destination;
my $output_file;
my $credentials_file;
my $verbose;
my $help;

if (scalar(@ARGV) > 0) {
	GetOptions(
		'P|listprojects!'   => \$list_projects,
		'l|list!'           => \$list_files,
		'y|summary!'        => \$sum_files,
		'V|listvolumes!'    => \$list_volumes,
		'u|url!'            => \$download_links,
		'x|export!'         => \$export_files,
		'c|copy!'           => \$copy_files,
		'm|move!'           => \$move_files,
		'delete!'           => \$delete_files,
		'deleteproject!'    => \$delete_project,
		'd|division=s'      => \$division_name,
		'p|project=s'       => \$project_name,
		'r|dir=s'           => \$remote_dir_name,
		'F|filelist=s'      => \$filelist_name,
		'f|filter=s'        => \$file_filter,
		't|task=s'          => \$task_id,
		'location=s'        => \$location_filter,
		'D|printdir!'       => \$print_folders,
		'limit|depth=i'     => \$recurse_limit,
		'aria!'             => \$aria_formatting,
		'batch=i'           => \$batch_size,
		'volume=s'          => \$volume_name,
		'prefix=s'          => \$vol_prefix,
		'volcopy!'          => \$vol_copy,
		'overwrite!'        => \$vol_overwrite,
		'wait=i'            => \$wait_time,
		'connection=s'      => \$vol_connection_file,
		'destination=s'     => \$destination,
		'new!'              => \$new_destination,
		'o|out=s'           => \$output_file,
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

#### Output file
my $OUT;
if ($output_file) {

	# open indicated output file
	$OUT = IO::File->new( $output_file, 'w' )
		or die "unable to open output file '$output_file'! $OS_ERROR\n";
}
else {

	# assign STDOUT to output variable for consistency
	$OUT = IO::Handle->new;
	$OUT->fdopen( fileno(STDOUT), 'w' );
}


######## Main functions
my $got_bulk_details = 0;   # so we don't do this more than once
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
elsif ($sum_files) {
	print_project_file_summary();
	exit 0;
}
elsif ($export_files) {
	export_files_to_volume();
	exit 0;
}
elsif ($copy_files) {
	copy_files_to_project();
	exit 0;
}
elsif ($move_files) {
	move_files_to_folder();
	exit 0;
}
elsif ($delete_files) {
	delete_platform_files();
	exit 0;
}
elsif ($delete_project) {
	delete_the_project();
	exit 0;
}
else {
	die "no function requested!?";
}





######## Functions

sub check_options {
	if (scalar @ARGV > 1) {
		die " Only one SB division/project parameter allowed!\n";
	}
	if (scalar @ARGV == 1 and not $division_name and not $project_name) {
		my $a = shift @ARGV;
		($division_name, $project_name, $remote_dir_name) = split m{/}, $a, 3;
	}
	unless ($division_name) {
		die " SBG division name is required!\n";
	}
	my $check = $list_files + $download_links + $export_files + $delete_files
		+ $list_projects + $list_volumes + $sum_files + $copy_files + $move_files
		+ $delete_project;
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
	if ($recurse_limit > 0) {
		$print_folders = 1;
	}
	if ($copy_files or $move_files) {
		unless ($destination) {
			die " Must set a destination with copy or move functions!\n";
		}
	}
	## no critic - doesn't like xx
	if ( $task_id and $task_id !~ /^ [ \d \- a-z ]+ $/xx ) {
		die " task id must consist of numbers, letters, and dashes only!\n";
	}
	## use critic
	$vol_prefix ||= $project_name;

	if ($download_links and $batch_size) {
		unless ($output_file) {
			die " Must define an output file with --out when chunking!\n";
		}
	}
	if ($batch_size) {
		$batch_size *= 0.95;            # leave a 5% buffer
		$batch_size *= 1_073_741_824;   # convert GB to base-2 bytes
		$batch_size = int $batch_size;  # drop decimals
	}
}


sub print_project_list {
	my $projects = $Sb->list_projects;
	if (@{$projects}) {
		my $len = max( map {length} map {$_->id} @{$projects} );
		my $formatter = '%-' . $len . 's %s' . "\n";
		$OUT->printf( $formatter, 'ID', 'Name' );
		foreach my $p (@{$projects}) {
			$OUT->printf( $formatter, $p->id, $p->name );
		}
	}
	else {
		print STDERR " No projects to list!\n";
	}
}

sub print_volume_list {
	my $volumes = $Sb->list_volumes;
	if ($volumes and scalar @{$volumes}) {
		my $len = max( map {length} map {$_->id} @{$volumes} );
		my $formatter = '%-' . $len . 's %s' . "\n";
		$OUT->printf( $formatter, 'ID', 'Name' );
		foreach my $p (@{$volumes}) {
			$OUT->printf( $formatter, $p->id, $p->name );
		}
	}
	else {
		print STDERR " No volumes to list!\n";
	}
}

sub collect_files {
	my $files;
	die " Can't connect to Project!" unless $Project;
	if ($filelist_name) {
		return load_files_from_file();
	}
	if ($task_id) {
		if ($remote_dir_name) {
			$files = $Project->list_files_by_task($task_id, $remote_dir_name);
		}
		else {
			$files = $Project->list_files_by_task($task_id);
		}
	}
	elsif ($remote_dir_name) {
		my $folder = $Project->get_file_by_name($remote_dir_name) or
			die " unable to find remote folder '$remote_dir_name'!\n";
		if ($folder and $folder->type eq 'file') {
			# it's actually a file!!!!
			$files = [ $folder ];
		}
		elsif ($folder and $folder->type eq 'folder') {
			$files = $folder->recursive_list($file_filter, $recurse_limit);
		}
		else {
			printf STDERR " returned unrecognized object %s\n", ref($folder);
			$files = [];
		}
	}
	else {
		$files = $Project->recursive_list($file_filter, $recurse_limit);
	}

	# filter on location if requested
	if ($location_filter) {
		
		# location only available via advanced details
		# imported file lists have location truncated, so not reliable
		unless ($got_bulk_details) {
			$Sb->bulk_get_file_details($files);
			$got_bulk_details = 1;
		}

		# filter
		my @keep;
		foreach my $f ( @{ $files} ) {
			my $location;
			if ( $f->type eq 'folder' ) {
				$location = 'platform';
			}
			else {
				$location = $f->file_status;
			}
			if ( $location =~ /$location_filter/xi ) {
				push @keep, $f;
			}
		}
		$files = \@keep;
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
		next unless $line =~ /\w+/;
		my $type = substr($line,0,3);
		if ($type eq 'Fil'){
			# File formatter: 'File %s %6s  %-13s  %s'
			if ($line =~ 
				/File \s+ ([a-z0-9]{24}) \s+ (\d+ (?:\.\d[KMG])?) \s+ [\w\-\.:]+ \s+ (.+) $/x
			) {
				my $id   = $1;
				my $size = $2;
				my $path = $3;
				
				## no critic - don't need x
				if ($file_filter and $path !~ /$file_filter/) {
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
		elsif ($type eq 'Dir') {
			if ($line =~ 
				/^Dir \s+ ([a-z0-9]{24}) \s+ 0 \s+ Platform \s+ (.+) $/x
			) {
				my $id   = $1;
				my $path = $2;
				
				## no critic - don't need x
				if ($file_filter and $path !~ /$file_filter/) {
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
		elsif ($type eq 'Tot') {
			next;   # total line
		}
		elsif ($line =~ /^Type \s+ ID \s+ Size \s+ (?:Location|Status) \s+ FilePath/x) {
			next;   # we are good, correct format
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
	unless ($files and scalar @{$files}) {
		print STDERR " No files to list!\n";
		return;
	}

	# bulk collect details for file status and size
	unless ($got_bulk_details) {
		$Sb->bulk_get_file_details($files);
		$got_bulk_details = 1;
	}

	# print the file names
	# the file IDs is always 24 characters long
	# Location and path will be variable
	my $formatter;
	if ($output_file) {
		# use tab delimiters with an output file
		$formatter = "%s\t%s\t%s\t%s\t%s\n";
	}
	else {
		# use original spaces with stdout
		$formatter = "%-4s %-24s %6s  %-15s  %s\n";
	}
	$OUT->printf( $formatter, qw(Type ID Size Location FilePath) );
	my @f = map {$_->[1]}
			sort {$a->[0] cmp $b->[0]}
			map { [ $_->type eq 'file' ? $_->pathname : $_->path, $_ ] } @{$files};
	my $count = 0;
	my $total = 0;
	foreach (@f) {
		if ($_->type eq 'folder') {
			next unless $print_folders;
			$OUT->printf( $formatter, 'Dir', $_->id, 0, 'Platform', $_->path );
		}
		else {
			my $size = $_->size || 0;
			my $location = $_->file_status;
			if ($location =~ m|/(\w+)$|) {
				$location = $1;
			}
			$OUT->printf( $formatter, 'File', $_->id, format_human_size($size),
				$output_file ? $location : substr( $location, 0, 15 ), $_->pathname );
			$total += $size;
			$count++;
		}
	}
	# print summary
	if ($file_filter or $recurse_limit or $remote_dir_name) {
		$OUT->printf( "\nTotal size of selected files in %s is %s for %d files\n",
			join('/', $division_name, $project_name, $remote_dir_name || q()),
			format_human_size($total), $count );
	}
	else {
		$OUT->printf( "\nTotal size for %s/%s is %s for %d files\n",
			$division_name, $project_name, format_human_size($total), $count );
	}
	return;
}

sub print_project_file_summary {
	# collect file list from the project
	my $files = collect_files();
	unless (@{$files}) {
		print STDERR " No files to list!\n";
		return;
	}

	# bulk collect details for file status and size
	unless ($filelist_name) {
		# we only need the size here, we should have obtained that from an input file
		# if it was provided
		unless ($got_bulk_details) {
			$Sb->bulk_get_file_details($files);
			$got_bulk_details = 1;
		}
	}

	# summarize
	my $count = 0;
	my $total = 0;
	foreach my $f ( @{$files} ) {
		if ($f->type eq 'file') {
			$count++;
			$total += $f->size || 0; # just in case?
		}
	}

	# print summary
	$OUT->printf( "\n%s is %s for %d files\n",
		join('/', $division_name, $project_name, $remote_dir_name || q()),
		format_human_size($total), $count );
	return;
}

sub print_download_file_links {
	# collect file list from the project
	my $files = collect_files();
	if ($batch_size) {
		unless ($filelist_name) {
			# we only need the size here, we should have obtained that from an input file
			# if it was provided
			unless ($got_bulk_details) {
				$Sb->bulk_get_file_details($files);
				$got_bulk_details = 1;
			}
		}
	}

	# generate download links
	my @links;
	my @nolinks;
	foreach my $f (@{$files}) {
		next if $f->type eq 'folder';
		my $url = $f->download_link;
		if ($url) {
			push @links, [$f, $f->pathname, $url];
		}
		else {
			push @nolinks, $f->pathname;
		}
	}
	# print the links
	if ( $aria_formatting and $batch_size ) {
		my $running_size = 0;
		my $number = 0;
		foreach my $l (@links) {
			if ( $l->[0]->size > $batch_size ) {
				printf STDERR " !!! %s is %s, bigger than batch size - skipping\n",
					$l->[1], format_human_size( $l->[0]->size );
				next;
			}
			elsif ( ( $running_size + $l->[0]->size ) < $batch_size ) {
				$OUT->printf( "%s\n  out=%s\n", $l->[2], $l->[1] );
			}
			else {
				# need to write new file
				$OUT->close;
				undef $OUT;
				$running_size = 0;
				$number++;
				my $newfile = $output_file;
				$newfile =~ s/(\.\w+)$/.$number$1/;
				$OUT = IO::File->new($newfile, 'w')
					or die "unable to write file '$newfile'! $OS_ERROR";
				$OUT->printf( "%s\n  out=%s\n", $l->[2], $l->[1] );
			}
			$running_size += $l->[0]->size;
		}
	}
	elsif ( $aria_formatting and not $batch_size ) {
		foreach my $l (@links) {
			$OUT->printf( "%s\n  out=%s\n", $l->[2], $l->[1] );
		}
	}
	elsif ( not $aria_formatting and $batch_size ) {
		my $running_size = 0;
		my $number = 0;
		foreach my $l (@links) {
			if ( $l->[0]->size > $batch_size ) {
				printf STDERR " !!! %s is %s, bigger than batch size - skipping\n",
					$l->[1], format_human_size( $l->[0]->size );
				next;
			}
			elsif ( ( $running_size + $l->[0]->size ) < $batch_size ) {
				$OUT->printf( "%s\n", $l->[2] );
			}
			else {
				# need to write new file
				$OUT->close;
				undef $OUT;
				$running_size = 0;
				$number++;
				my $newfile = $output_file;
				$newfile =~ s/(\.\w+)$/.$number$1/;
				$OUT = IO::File->new($newfile)
					or die "unable to write file '$newfile'! $OS_ERROR";
				$OUT->printf( "%s\n", $l->[2] );
			}
			$running_size += $l->[0]->size;
		}
	}
	elsif ( not $aria_formatting and not $batch_size ) {
		foreach my $l (@links) {
			$OUT->printf( "%s\n", $l->[2] );
		}
	}
	else {
		die "programming error printing download file links!";
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
		print STDERR 
" Requested volume '$volume_name' is not attached! Double check or attach\n";
		exit 1;
	}

	# collect file list from the project
	my $items = collect_files();
	my @files = grep { $_->type eq 'file' } @{ $items };
	printf STDERR " exporting %d files to $volume_name/$vol_prefix\n", scalar(@files);

	# export files
	my @options = (
		files     => \@files,
		overwrite => $vol_overwrite,
		copy      => $vol_copy,
		prefix    => $vol_prefix,
	);
	my $results = $Volume->export_files(@options);

	# print results
	# file ID is 24 characters, export ID is 32 characters
	$OUT->print(
		"FileID                   ExportID                         Status     File\n" );
	my @ids = map {$_->[1]}
			  sort {$a->[0] cmp $b->[0]}
			  map { [ $results->{$_}{source}, $_ ] }
			  keys %{$results};
	foreach my $id (@ids) {
		# default assumption is the copy succeeded, otherwise there would be an error 
		if ($results->{$id}{status} eq 'FAILED') {
			# there was an error, state why if possible
			# not all the error codes here are listed here, just the most obvious
			my $error;
			if (exists $results->{$id}{error} ) {
				my $e = $results->{$id}{error} || 0;
				if ($e == 9107) {
					$error = sprintf "%s already exists", $results->{$id}{destination};
				}
				elsif ($e == 9006) {
					$error = sprintf "%s cannot be exported", $results->{$id}{source};
				}
				else {
					$error = sprintf "Error %d for %s", $e, $results->{$id}{source};
				}
			}
			else {
				$error = sprintf "unknown error for %s", $results->{$id}{source};
			}
			$OUT->printf( "%s %s %-10s %s\n", $id,
				$results->{$id}{transfer_id}, $results->{$id}{status}, $error );
		}
		else {
			$OUT->printf( "%s %s %-10s %s\n", $id, 
				$results->{$id}{transfer_id} || q(                                ),
				$results->{$id}{status}, $results->{$id}{destination} );
		}
	}
}

sub copy_files_to_project {

	# first check destination
	if ( $destination =~ m|/| ) {
		my @bits = split m|/|, $destination;
		if ( scalar(@bits) == 2 and $bits[0] eq $division_name ) {
			# division is ok, we ignore it
			$destination = $bits[1];
		}
		else {
			print STDERR <<END;
 ERROR: Destination has a / character. The destination for the copy function must be
 another project in the same lab division. The destination must not include a folder
 or be in a different lab division, and therefore cannot include a / character."
END
		}
	}

	# open Destination project
	my $Dest;
	if ($new_destination) {
		$Dest = $Sb->create_project( name => $destination );
		if ($Dest and ref $Dest eq 'Net::SB::Project') {
			$OUT->printf( " Created new project $destination\n" );
		}
	}
	else {
		$Dest = $Sb->get_project($destination);
	}
	unless ($Dest and ref $Dest eq 'Net::SB::Project') {
		die " Destination project cannot be opened or created!";
	}
	
	# collect file list from the project
	my $files = collect_files();
	my @copied;
	my @errors;
	foreach my $file ( @{ $files} ) {
		next unless $file->type eq 'file';
		my $result = $file->copy_to_project($Dest);
		if ($result and ref $result eq 'Net::SB::File') {
			push @copied, $result;
		}
		else {
			push @errors, sprintf("%s failed to copy", $file->pathname);
		}
	}
	
	# print results
	$OUT->printf( " Copied %d files to $destination\n Updated files:\n",
		scalar(@copied) );
	foreach (@copied) {
		$OUT->printf( " File %s %s\n", $_->id, $_->pathname );
	}
	if (@errors) {
		$OUT->printf( "\n\n The following %d errors occurred in copying files:\n", 
			scalar(@errors) );
		foreach (@errors) {
			$OUT->print( "$_\n" );
		}
	}
}

sub move_files_to_folder {
	# first check destination
	if ( $destination =~ m|/| ) {
		my @bits = split m|/|, $destination;
		if ( $bits[0] eq $division_name ) {
			shift @bits;
		}
		if ($bits[0] eq $project_name) {
			shift @bits;
		}
		$destination = join '/', @bits;
	}

	# open Destination project
	my $Dest;
	if ($new_destination) {
		$Dest = $Project->create_folder( $destination );
	}
	else {
		$Dest = $Project->get_file_by_name( $destination );
	}
	unless ($Dest and ref $Dest eq 'Net::SB::Folder') {
		die " Destination folder cannot be opened or created!";
	}
	
	# collect file list from the project
	my $files = collect_files();
	my @copied;
	my @errors;
	foreach my $file ( @{ $files} ) {
		next unless $file->type eq 'file';
		my $result = $file->move_to_folder($Dest);
		if ($result and ref $result eq 'Net::SB::File') {
			push @copied, $result;
		}
		else {
			push @errors, sprintf("%s failed to move", $file->pathname);
		}
	}
	
	# print results
	$OUT->printf( " Moved %d files to $destination\n Updated files:\n", scalar(@copied) );
	foreach (@copied) {
		$OUT->printf( " File %s %s\n", $_->id, $_->pathname );
	}
	if (@errors) {
		$OUT->printf( "\n\n The following %d errors occurred in moving files:\n", 
			scalar(@errors) );
		foreach (@errors) {
			$OUT->print( "$_\n" );
		}
	}
}

sub delete_platform_files {
	# collect file list from the project
	my $files = collect_files();
	$OUT->printf( " !! Bulk DELETING %d files!!\n", scalar @{$files} );
	$OUT->print( " You have 10 seconds to cancel this...\n\n");
	sleep 10;
	my $results = $Sb->bulk_delete($files);
	foreach my $r (@{$results}) {
		$OUT->print( " $r\n" );
	}
	if ( $remote_dir_name and not $file_filter and not $filelist_name ) {
		my $folder = $Project->get_file_by_name($remote_dir_name); 
		my $s = $folder->delete;
		if ($s) {
			$OUT->printf( " Deleted parent folder %s\n", $remote_dir_name );
		}
		else {
			$OUT->printf( " Could not delete parent folder %s\n", $remote_dir_name );
		}
	}
}

sub delete_the_project {
	$OUT->printf( "\n !! DELETING project %s !!\n\n", $Project->id );
	$OUT->print( " You have 10 seconds to cancel this...\n\n");
	sleep 10;
	my $s = $Project->delete;
	if ($s) {
		$OUT->printf( " Deleted project %s!\n", $Project->id);
	}
	else {
		$OUT->printf( " Could not delete project %s!\n", $remote_dir_name );
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
