#!/usr/bin/env perl

use warnings;
use strict;
use English qw(-no_match_vars);
use Getopt::Long qw(:config no_ignore_case);
use IO::File;
use Net::SB;

our $VERSION = 1.1;

my $doc = <<END;

An application to copy files/folders between Seven Bridges Genomics projects.

This script will copy multiple source files or folders from one project into 
another (new) project. For example, split a very large unmanageable project 
into two or more smaller, more manageable projects by moving the files. 

This uses the the SBG asynchronous API method calls, allowing a job to be 
submitted and run in the background. Furthermore, it allows folders to be 
specified and the entire hierarchical structure will be copied/moved 
recursively. 

This script will wait and periodically watch the job until completion. 
Alternatively, the script can exit immediately, and the result checked at
a later time point. 

Due to SBG permissions and restrictions, both source and destination 
projects _must_ be in the same lab division, even if you have permission to 
both. 

Version: $VERSION

Example Usage:
    
  sbg_async_folder_copy.pl -s big-shot-pi/project1  -d big-shot-pi/project2  file...

Required:
    -s --source       <text>    Name of the source SBG division/project
    -d --destination  <text>    Name of the destination SBG division/project[/folder]
                                   If a folder or path is specified, it will be
                                   created as necessary.

Sources:
    -f --file         <text>    Source folder/file to copy in the source project
                                Folders are recursed. Repeat as necessary.
    -l --list         <file>    Local file with list of selected files to copy
                                   Use output from sbg_project_manager.pl 

Options:
    -c --check        <text>    Check the result of a previous job, enter 
                                  the ID number of the job. Source is not
                                  required. The move flag must be set
                                  as appropriate.
    --move                      Move the files instead of copying them
    --new                       Indicate that the destination project is new
    --name            <text>    Optional long name for the new project
    --wait         <integer>    Time to sleep between check cycles (30 sec)
                                  set to 0 to exit immediately
General:
    --cred            <file>    Path to SBG credentials file 
                                  default ~/.sevenbridges/credentials
    -v --verbose                Print all https processing commands
    -h --help                   Print this help
END


# Command line variables
my $destination_name;
my $result_id;
my $source_name;
my $list_file;
my @source_files;
my $new_project = 0;
my $long_name;
my $do_move = 0;
my $credentials_file;
my $wait_time = 30;
my $verbose;
my $help;

# other global variables
my $division_name;
my $source_project_name;
my $destination_project_name;
my $destination_folder;


## Process command line options
if (scalar(@ARGV) > 0) {
	GetOptions(
		'c|check=i'         => \$result_id,
		'd|destination=s'   => \$destination_name,
		's|source=s'        => \$source_name,
		'f|file=s'          => \@source_files,
		'l|list=s'          => \$list_file,
		'new!'              => \$new_project,
		'name=s'            => \$long_name,
		'move!'             => \$do_move,
		'cred=s'            => \$credentials_file,
		'wait=i'            => \$wait_time,
		'v|verbose!'        => \$verbose,
		'h|help!'           => \$help,
	) or die "please recheck your options!! Run with --help to display options\n";
}
else {
	print $doc;
	exit 0;
}

# sources provided on command line
if (@ARGV) {
	push @source_files, @ARGV;
}



# Check options
if ($help) {
	print $doc;
	exit 0;
}
check_options();



# Initialize Projects
my ($Sb, $Destination_project, $Source_project, $Destination);
initialize_projects();


# Run 
if ($result_id) {
	# generate a fake result
	my $result = {
		id    => $result_id,
		type  => $do_move ? 'MOVE' : 'COPY',
	};
	report_result($result);
}
else {
	if ($list_file) {
		collect_sources_from_file();
	}
	my $result = submit_job();
	report_result($result);
}


# Finish
print "\n Finished\n";
exit 0;









######## Functions

sub check_options {
	unless ($destination_name) {
		print " SBG destination project name is required!\n";
		exit 1;
	}
	if (not $result_id) {
		unless ($source_name) {
			print " A source project name is required!\n";
			exit 1;
		}
		unless (@source_files or $list_file) {
			print " One or more source files or folders are required!\n";
			exit 1;
		}
	}
	if ($source_name) {
		(my $div1, $destination_project_name, $destination_folder) = split m|/|,
			$destination_name, 3;
		(my $div2, $source_project_name, undef) = split m|/|, $source_name, 3;
		if ($div1 eq $div2) {
			$division_name = $div1;
		}
		else {
			print " Source and destination division names are not the same!\n";
			exit 1;
		}
	}
	else {
		($division_name, $destination_project_name, $destination_folder) = split m|/|,
			$destination_name, 3;
	}
	
}


sub initialize_projects {

	# Division object
	$Sb = Net::SB->new(
		div        => $division_name,
		verbose    => $verbose,
		cred       => $credentials_file,
		sleepvalue => $wait_time,
	) or die "can't initialize division!!!";

	# Source project
	if ($source_project_name) {
		$Source_project = $Sb->get_project($source_project_name)
			or die " Can't initialize source project $source_project_name!\n";
	}

	# Destination project
	$Destination_project = $Sb->get_project($destination_project_name);
	unless ($Destination_project) {
		if ($new_project) {
			my $alt_name = $destination_project_name;
			$alt_name =~ s/\-/ /;  # replace dashes with spaces per API
			$Destination_project = $Sb->create_project(
				name    => $alt_name,
			) or die " Cannot create new destination project '$alt_name'!\n";
			if ($long_name) {
				$Destination_project->update(
					'name'  => $long_name,
				);
			}
		}
		else {
			die 
" Destination project '$destination_project_name' does not exist! use --new option\n";
		}
	}
	
	# Actual Destination object
	if ($destination_folder) {
		my $f = $Destination_project->get_file_by_name($destination_folder);
		if ($f) {
			if ($f->type eq 'folder') {
				$Destination = $f;
			}
			elsif ($f->type eq 'file') {
				print " Destination is a file! Must be a folder\n";
				exit 1;
			}
			else {
				die "something is wrong with returned folder object!";
			}
		}
		else {
			$f = $Destination_project->create_folder($destination_folder);
			if ($f) {
				$Destination = $f;
			}
			else {
				print " Unable to create destination folder $destination_folder!\n";
				exit 1;
			}
		}
	}
	else {
		# we will deposit in the root of the destination project
		$Destination = $Destination_project;
	}
}

sub report_result {
	my $result = shift;

	# status
	my $final  = $Sb->get_async_job_result($result);
	printf " Async %s job %s is %s\n", $final->{type}, $final->{id}, $final->{'state'};

	# finished information
	if ($final->{'state'} eq 'FINISHED') {
		printf " The job finished on %s\n", $final->{'finished_on'};
		printf " There were %d completed files and %d failed files\n",
			$final->{'completed_files'}, $final->{'failed_files'};

		# print list of files
		my $files = $Sb->get_async_job_files($final, $Destination);
		if ( @{$files} ) {
			print " The results are\n";
			foreach my $f ( @{ $files } ) {
				if ($f->type eq 'folder') {
					printf "Dir  %s  %s\n", $f->id, $f->path;
				}
				else {
					printf "File %s  %s\n", $f->id, $f->pathname;
				}
			}
		}
	}
}


sub collect_sources_from_file {
	my $fh = IO::File->new($list_file) or
		die "unable to load file '$list_file'! $OS_ERROR\n";
	my $fail = 0;
	while (my $line = $fh->getline) {
		chomp $line;
		next unless length $line;
		my $type = substr($line,0,3);
		if ($type eq 'Fil'){
			# File formatter: 'File %s %6s  %-13s  %s'
			if ($line =~ 
				/File \s+ ([a-z0-9]{24}) \s+ ([\d\.KMG]+) \s+ [\w\-\.:]+ \s+ (.+) $/x
			) {
				my $id   = $1;
				my $size = $2;
				my $path = $3;
				
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
					project => sprintf("%s/%s", $division_name, $source_project_name),
					href    => sprintf("%s/files/%s", $Sb->endpoint, $id),
				};
				push @source_files, Net::SB::File->new( $Source_project, $file );
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
				
				my @bits = split m|/|, $path;
				my $name = pop @bits;				
				my $folder = {
					id      => $id,
					name    => $name,
					path    => $path,
					project => sprintf("%s/%s", $division_name, $source_project_name),
					href    => sprintf("%s/files/%s", $Sb->endpoint, $id),
				};
				push @source_files, Net::SB::Folder->new( $Source_project, $folder );
			}
			else {
				$fail++;
			}
		}
		elsif ($line =~ /^Type \s+ ID \s+ Size \s+ Location \s+ FilePath$/x) {
			# expected header
		}
		elsif ($line =~ /^Total \s size/x) {
			# summary footer
		}
		else {
			$fail++;
		}
	}
	$fh->close;
	if ($fail) {
		print STDERR " there were $fail unrecognizeable lines skipped\n";
	}
}

sub submit_job {
	
	# generate list of source destination pairs
	my @list;
	foreach my $f (@source_files) {
		# get folder or file object
		my $Source = $Source_project->get_file_by_name($f);
		if ($Source) {
			push @list, [ $Source, $Destination ];
		}
		else {
			print " ! Unable to find source '$f'! Skipping\n";
		}
	}

	# submit the async job
	my $submit;
	if ($do_move) {
		$submit = $Sb->submit_multiple_file_move(\@list);
	}
	else {
		$submit = $Sb->submit_multiple_file_copy(\@list);
	}
	if ($submit and ref($submit) eq 'HASH') {
		printf "\n Submitted async %s job %s\n", $submit->{type}, $submit->{id};
	}
	else {
		die " Something went wrong with submission\n";
	}
	
	
	# wait and watch or finish
	if ($wait_time) {
		printf
"\n Watching job. Status reports should print every %d seconds until complete.\n You may cancel anytime\n\n",
			$wait_time;
		return $Sb->watch_async_job($submit);
	}
	else {
		return $submit;
	}
}


