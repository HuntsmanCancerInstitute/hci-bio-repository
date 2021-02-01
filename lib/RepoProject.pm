package RepoProject;
our $VERSION = 5.1;

=head1 NAME 

ProjectTools - Common functions for HCI-Bio-Repository projects

=head1 DESCRIPTION

Common Perl functions for the scripts in the HCI-Bio-Repository package. 
All functions are exported by default.

=head1 FUNCTIONS

=head2 Initialize

=over 4 

=item new

Initialize a Repository object. Must pass the full path to a directory. 
Optionally pass a 1 or 0 for verbosity. The project identifier will be 
the last directory in the path. All variables will be derived from this 
path and identifier.

    my $path    = '/Repository/MicroarrayData/2019/1234R';
    my $verbose = 1;
    my $Project = RepoProject->new($path, $verbose);

=back

=head2 Project Parameters

These are all read-only functions.

=over 4 

=item given_dir

Returns the given directory when initializing the object.
Example: F</Repository/MicroarrayData/2019/1234R>.

=item parent_dir

Returns parent directory. Example: F</Repository/MicroarrayData/2019>.

=item id

Returns project identifier. Example: C<1234R>.

=item manifest_file

Returns name of the manifest file. Example: F<1234R_MANIFEST.csv>.

=item remove_file

Returns name of the remove list file. Example: F<1234R_REMOVE_LIST.txt>

=item zip_file

Returns name of the zip archive file. Example: F<1234R_ARCHIVE.zip>.

=item ziplist_file

Returns name of the zip archive list file. Example F<1234R_ARCHIVE_LIST.txt>.

=item notice_file

Returns name of the notice file. Example F<where_are_my_files.txt>

=item alt_remove_file

Returns path and name of alternate or hidden remove list file. Example: 
F</Repository/MicroarrayData/2019/1234R_REMOVE_LIST.txt>

=item zip_folder

Returns path to the hidden zip folder where files are moved to after 
being added into the zip archive. Example: 
F</Repository/MicroarrayData/2019/1234R_ZIPPED_FILES>.

=item delete_folder

Returns path to the hidden delete folder where files are moved to after 
being hidden. Example: 
F</Repository/MicroarrayData/2019/1234R_DELETED_FILES>.

=item notice_source_file

Returns the path and name to the original notice text file that is 
linked to the project folder. A notice file is kept at the root of 
both Repository volumes, F<MicroarrayData> and F<AnalysisData>. 
Example: F</Repository/MicroarrayData/missing_file_notice.txt>.

=item verbose

Returns the verbosity value.

=back

=head2 Utility functions

General purpose utility functions that are not necessarily linked to 
the project object.

=over 4

=item get_file_list

Loads a list text file, such as the zip list or remove list, from disk 
into memory. Returns array or array reference. Pass the file path.

    my @filelist = $Project->get_file_list($Project->ziplist_file);

=item calculate_file_checksum

Calculates the MD5 checksum on a file. Pass the file path.

    my $md5 = $Project->calculate_file_checksum($file);

=item clean_empty_directories

Executes an external C<find> command in a sub shell to recursively 
search for and delete empty subdirectories. Pass the directory to 
search. Returns the return status from the C<find> command.

=back

=head2 Repository functions

These are functions to act on the files within a repository. 
In all cases, the repository file must be scanned and list 
files generated before any of these functions can be applied. 
See the accompanying scripts for scanning Analysis and Request 
projects. 

When L<verbose> is set to true, file names are printed to
standard out as they are being moved or deleted. When failures
occur, file names are printed to standard out. 

=over 4

=item hide_deleted_files

Moves the files listed in the remove list from the project folder
into the hidden deleted files folder. Prints the files moved if
verbose is true. Returns an integer for the number of failures; 0
is success. Empty directories are removed after moving files.

=item hide_zipped_files

Moves the files listed in the archive list from the project
folder into the hidden zipped files folder. Prints the files
moved if verbose is true. Returns an integer for the number of
failures; 0 is success. Empty directories are removed after
moving files.

=item unhide_deleted_files

Moves the files listed in the remove list from the hidden 
deleted files folder back into the project folder. Prints the 
files moved if verbose is true. Returns an integer for the 
number of failures; 0 is success.

=item unhide_zip_files

Moves the files listed in the archive list from the hidden 
zipped files folder back into the project folder. Prints the 
files moved if verbose is true. Returns an integer for the 
number of failures; 0 is success.

=item delete_zipped_files_folder

Deletes the contents of the hidden zipped file folder based 
on the archive list file. Returns an integer for the number 
of failures; 0 is success. Empty directories are trimmed 
after deletion.

=item delete_hidden_deleted_files

Deletes the contents of the hidden deleted files folder based 
on the remove list file. Returns an integer for the number 
of failures; 0 is success. Empty directories are trimmed 
after deletion.

=item delete_project_files

Deletes the files listed in the remove list file or alternate 
remove list file from the project folder. Returns an integer 
for the number of failures to remove files; 0 is success. 

=item delete_zipped_files

Deletes the files listed in the archive list file from the
project folder. Returns an integer for the number of failures to
remove files; 0 is success. 

=item add_notice_file

Inserts a symbolic link from the notice source file to the 
notice file in the project folder.

=back

=cut

use strict;
use Carp;
use IO::File;
use File::Spec;
use File::Copy;
use File::Path qw(make_path);
use File::Find;
use Digest::MD5;

1;

### Initialize

# Initialize reusable checksum object
my $Digest = Digest::MD5->new;

# Initialize global find variables
my $current_project = undef;
my $project_age     = 0;
my $project_size    = 0;
my $day             = 86400; # 60 seconds * 60 minutes * 24 hours

sub new {
	my ($class, $path, $verbose) = @_;
	$verbose ||= 0;
		
	# check directory
	unless ($path =~ /^\//) {
		carp "given path does not begin with / Must use absolute paths!";
		return;
	}
	unless (-e $path) {
		carp "given path $path does not exist!";
		return;
	}

	# extract the project ID
	my $project;
	if ($path =~ m/(A\d{1,5}|\d{3,5}R)\/?$/) {
		# look for A prefix or R suffix project identifiers
		# this ignores Request digit suffixes such as 1234R1, 
		# when clients submitted replacement samples
		$project = $1;
	}
	elsif ($path =~ m/(\d{2,4})\/?$/) {
		# old style naming convention without an A prefix or R suffix
		$project = $1;
	}
	else {
		# non-canonical path, take the last given directory
		my @dir = File::Spec->splitdir($path);
		$project = @dir[-1];
	}

	# check directory 
	my $parent_dir = './';
	if ($path =~ m/^(\/Repository\/(?:MicroarrayData|AnalysisData)\/\d{4})\/?/) {
		$parent_dir = $1;
	}
	elsif ($path =~ m/^(.+)$project\/?$/) {
		$parent_dir = $1;
	}
	
	# initiate project
	my $self = {
		given_dir   => $path,
		parent_dir  => $parent_dir,
		project     => $project,
		verbose     => $verbose,
	};
	
	# project files
	$self->{manifest} = $project . "_MANIFEST.csv";
	$self->{remove}   = $project . "_REMOVE_LIST.txt";
	$self->{ziplist}  = $project . "_ARCHIVE_LIST.txt";
	$self->{zip}      = $project . "_ARCHIVE.zip";
	$self->{notice}   = "where_are_my_files.txt";

	# hidden file names in parent directory
	$self->{alt_remove}    = File::Spec->catfile($parent_dir, $project . "_REMOVE_LIST.txt");
	$self->{alt_zip}       = File::Spec->catfile($parent_dir, $project . "_ARCHIVE.zip");
	$self->{alt_ziplist}   = File::Spec->catfile($parent_dir, $project . "_ARCHIVE_LIST.txt");
	$self->{zipfolder}     = File::Spec->catfile($parent_dir, $project . "_ZIPPED_FILES");
	$self->{delfolder}     = File::Spec->catfile($parent_dir, $project . "_DELETED_FILES");
	
	# notification file
	if ($parent_dir =~ /MicroarrayData/) {
		$self->{notice_source} = "/Repository/MicroarrayData/missing_file_notice.txt";
	}
	elsif ($parent_dir =~ /AnalysisData/) {
		$self->{notice_source} = "/Repository/AnalysisData/missing_file_notice.txt";
	}
	else {
		# primarily for testing purposes
		$self->{notice_source} = "~/missing_file_notice.txt";
	}

	return bless $self, $class;
}



### Path variables

sub given_dir {
	return shift->{given_dir};
}

sub parent_dir {
	return shift->{parent_dir};
}

sub id {
	# old method name for id
	return shift->{project};
}

sub project {
	return shift->{project};
}

sub manifest_file {
	return shift->{manifest};
}

sub remove_file {
	return shift->{remove};
}

sub zip_file {
	return shift->{zip};
}

sub ziplist_file {
	return shift->{ziplist};
}

sub notice_file {
	return shift->{notice};
}

sub alt_remove_file {
	return shift->{alt_remove};
}

sub alt_zip_file {
	return shift->{alt_zip};
}

sub zip_folder {
	return shift->{zipfolder};
}

sub delete_folder {
	return shift->{delfolder};
}

sub notice_source_file {
	return shift->{notice_source};
}

sub verbose {
	return shift->{verbose};
}




### Utility functions

sub get_file_list {
	my $self = shift;
	my $file = shift;
	return unless $file;
	
	my $fh = IO::File->new($file, 'r');
	unless ($fh) {
		carp "can't read $file! $!\n";
		return;
	} 
	
	# process
	my @list;
	while (my $line = $fh->getline) {
		chomp($line);
		push @list, $line;
	}
	$fh->close;
	return wantarray ? @list : \@list;
}


sub calculate_file_checksum {
	# calculate the md5 checksum on a given file
	my $self = shift;
	my $file = shift;
	return unless $file;
	
	open (my $fh, '<', $file) or return 1;
		# if we can't open the file, just skip it and return a dummy value
		# we'll likely have more issues with this file later
	binmode ($fh);
	my $md5 = $Digest->addfile($fh)->hexdigest;
	close $fh;
	return $md5;
}





### Project functions

sub hide_deleted_files {
	my $self = shift;
	
	# check file list
	unless (-e $self->alt_remove_file) {
		carp "  ! no alternate file remove list!";
		return 1;
	}
	
	# move the deleted files
	my $filelist = $self->get_file_list($self->alt_remove_file);
	chdir $self->given_dir; # just in case
	mkdir $self->delete_folder;
	my $failure_count = $self->_move_directory_files('./', 
		$self->delete_folder, $filelist);
	
	# move the hidden deletion list file into project
	move($self->alt_remove_file, $self->remove_file) or do {
		printf "   failed to move %s! $!\n", $self->alt_remove_file;
		$failure_count++;
	};
	
	# clean up empty directories
	$failure_count += $self->clean_empty_directories($self->given_dir);
	
	# put in notice
	$failure_count += $self->add_notice_file;
	
	return $failure_count;
}

sub hide_zipped_files {
	my $self = shift;
	chdir $self->given_dir; # just in case
	if (not -e $self->ziplist_file) {
		print "  ! no zip list file exists! Nothing to move\n" ;
		return 1;
	}
	if (not -e $self->zip_file) {
		print "  ! no zip archive exists! Best not move!\n" ;
		return 1;
	}
	
	# move the zipped files
	my $filelist = $self->get_file_list($self->ziplist_file);
	mkdir $self->zip_folder;
	my $failure_count = $self->_move_directory_files('./', $self->zip_folder, $filelist);
	
	# clean up empty directories
	$failure_count += $self->clean_empty_directories($self->given_dir);
	
	return $failure_count;
}


sub unhide_zip_files {
	my $self = shift;
	if (-e $self->ziplist_file and -e $self->zip_folder) {
		my $filelist = $self->get_file_list($self->ziplist_file);
		chdir $self->zip_folder;
		my $fc = $self->_move_directory_files('./', $self->given_dir, $filelist);
		chdir $self->given_dir; # go back
		$fc += $self->clean_empty_directories($self->zip_folder);
	
		return $fc;
	}
	else {
		print "  ! No zip list file available!\n";
		return 1;
	}
}

sub unhide_deleted_files {
	my $self = shift;
	my $failure_count;
	
	if (-e $self->delete_folder) {
		# we have remove and and deletion folder
		
		# collect from delete list
		my $filelist;
		if (-e $self->remove_file) {
			$filelist = $self->get_file_list($self->remove_file);
		}
		elsif (-e $self->alt_remove_file) {
			print "   wierd! no delete list file, but have alternate remove file! trying with that\n";
			$filelist = $self->get_file_list($self->alt_remove_file);
		}
		else {
			printf "  ! No deleted file list file available!\n";
			return 1;
		}
		
		chdir $self->delete_folder; 
		$failure_count = $self->_move_directory_files('./', $self->given_dir, $filelist);
		chdir $self->given_dir; # move back
		
		# clean up empty directories
		$failure_count += $self->clean_empty_directories($self->delete_folder);
		
		# hide remove list
		move($self->remove_file, $self->alt_remove_file) or do {
			printf "   failed to hide $%s! $!\n", $self->remove_file;
			$failure_count++;
		};
		
		# remove notice file
		if (-e $self->notice_file) {
			print "  > deleting notice file\n" if $self->verbose;
			unlink $self->notice_file;
		}
	}
	else {
		print "  ! No deleted files folder! Nothing to unhide\n";
		$failure_count += 1;
	}
	
	return $failure_count;
}


sub delete_zipped_files_folder {
	my $self = shift;
	if (-e $self->zip_folder and -e $self->ziplist_file) {
		my $filelist = $self->get_file_list($self->ziplist_file);
		my $fc = $self->_delete_directory_files($self->zip_folder, $filelist);
		$fc += $self->clean_empty_directories($self->zip_folder);
		return $fc;
	}
	else {
		print "  ! No hidden zip folder and/or zip list file!\n";
		return 1;
	}
}

sub delete_hidden_deleted_files {
	my $self = shift;
	if (-e $self->delete_folder and -e $self->remove_file) {
		my $filelist = $self->get_file_list($self->remove_file);
		my $fc = $self->_delete_directory_files($self->delete_folder, $filelist);
		$fc += $self->clean_empty_directories($self->delete_folder);
		return $fc;
	}
	else {
		print "  ! No hidden delete folder and/or remove list file!\n";
		return 1;
	}
}

sub delete_project_files {
	my $self = shift;
	my $failure_count;
	chdir $self->given_dir; # just in case
	
	if (-e $self->alt_remove_file) {
		# normal situation
		my $filelist = $self->get_file_list($self->alt_remove_file);
		$failure_count = $self->_delete_directory_files('./', $filelist);
		
		# move the deleted list file back into project
		move($self->alt_remove_file, $self->remove_file) or do {
			printf "   failed to move file %s! $!\n", $self->alt_remove_file;
			$failure_count++;
		};
		
		$failure_count += $self->clean_empty_directories($self->given_dir);
	}
	elsif (-e $self->remove_file) {
		# unusual situation
		# may be an already processed folder, or from an earlier management script????
		my $filelist = $self->get_file_list($self->remove_file);
		$failure_count = $self->_delete_directory_files('./', $filelist);
		$failure_count += $self->clean_empty_directories($self->given_dir);
	}
	else {
		print "  No remove file list found to delete files!\n";
		return 1;
	}
	
	# put in notice
	$failure_count += $self->add_notice_file;
	
	return $failure_count;
}

sub delete_zipped_files {
	my $self = shift;
	chdir $self->given_dir; # just in case
	if (-e $self->ziplist_file) {
		my $filelist = $self->get_file_list($self->ziplist_file);
		my $fc = $self->_delete_directory_files('./', $filelist);
		$fc += $self->clean_empty_directories($self->given_dir);
	}
	else {
		print "  No zip list file found to delete files!\n";
		return 1;
	}
}

sub add_notice_file {
	my $self = shift;
	
	if (not -e $self->notice_file) {
		if (not -e $self->notice_source_file) {
			print "   Notice file not present!\n";
			return 1;
		}
		
		my $command = sprintf("ln -s %s %s", $self->notice_source_file, $self->notice_file);
		if (system($command)) {
			print "    failed to link notice file! $!\n" ;
			return 1;
		}
		else {
			return 0;
		}
	}
	return 0;
}

sub clean_empty_directories {
	my $self = shift;
	my $directory = shift;
	
	my $command = sprintf("find %s -type d -empty -delete", $directory);
	print "  > executing: $command\n" if $self->verbose;
	if (system($command)) {
		print "   ! find command '$command' failed! $!\n";
		return 1;
	}
	return 0;
}

sub get_size_age {
	my $self = shift;
	
	# set global values, because File::Find sucks and can't take private data
	$current_project = $self;
	$project_size    = 0;
	$project_age     = 0;
	
	# collect data for given directory
	find( {
			follow => 0, # do not follow symlinks
			wanted => \&_age_callback,
		  }, $self->given_dir
	);
	
	# add hidden directories too
	if (-e $self->zip_folder) {
		find( {
				follow => 0, # do not follow symlinks
				wanted => \&_age_callback,
			  }, $self->zip_folder
		);
	}
	if (-e $self->delete_folder) {
		find( {
				follow => 0, # do not follow symlinks
				wanted => \&_age_callback,
			  }, $self->delete_folder
		);
	}
	
	# return size in bytes and oldest posix age (youngest file)
	return ($project_size, $project_age);
}



#### Internal functions

sub _move_directory_files {
	my ($self, $source, $destination, $filelist) = @_; # could be either zipped or deleted
	my $failure_count = 0;
	
	unless (scalar @$filelist) {
		print "   no files in list! Nothing moving\n";
		return 0;
	}
	
	# process the removelist
	foreach my $file (@$filelist) {
		my (undef, $dir, $basefile) = File::Spec->splitpath($file);
		
		# source file
		my $sourcefile = $self->_check_file($source, $dir, $basefile);
		unless ($sourcefile) {
			print "   Missing $file\n" if $self->verbose;
			next;
		}
		# destination
		my $destinationdir = File::Spec->catdir($destination, $dir);
		make_path($destinationdir); 
			# this should safely skip existing directories
			# permissions and ownership inherit from user, not from source
			# return value is number of directories made, which in some cases could be 0!
		print "   moving $sourcefile to $destinationdir\n" if $self->verbose;
		move($sourcefile, $destinationdir) or do {
			print "   failed to move $sourcefile! $!\n";
			$failure_count++;
		};
	}
	
	return $failure_count;
}

sub _delete_directory_files {
	my ($self, $target_dir, $filelist) = @_;
		
	# process the removelist
	my $failure_count = 0;
	foreach my $file (@$filelist) {
		my (undef, $dir, $basefile) = File::Spec->splitpath($file);
		my $targetfile = $self->_check_file($target_dir, $dir, $basefile);
		unless ($targetfile) {
			print "   Missing $file\n" if $self->verbose;
			next;
		}
		print "   DELETING $targetfile\n" if $self->verbose;
		unlink($targetfile) or do {
			print "   failed to remove $targetfile! $!\n";
			$failure_count++;
		};
	}
	
	return $failure_count;
}

sub _check_file {
	my ($self, $target_dir, $dir, $basefile) = @_;
	return undef unless ($target_dir and $basefile);
	my $targetfile = File::Spec->catfile($target_dir, $dir, $basefile);
	if (-e $targetfile) {
		# check file exist and not something else like a link
		return $targetfile if -f _ ;
		return undef;
	}
	else {
		# older versions may record the project folder in the list file name, so let's 
		# try removing that
		my $p = $self->id;
		$dir =~ s/^$p\///;
		$targetfile = File::Spec->catfile($target_dir, $dir, $basefile);
		return $targetfile if -e $targetfile and -f _ ;
	}
	return undef;	
}

sub _age_callback {
	my $file = $_;
	
	# skip specific files, including SB preparation files
	return if -d $file;
	return if -l $file;
	return if substr($file,0,1) eq '.'; # dot files are usually hidden, backup, or OS stuff
	return if $file eq $current_project->manifest_file;  
	return if $file eq $current_project->zip_file;  
	return if $file eq $current_project->ziplist_file;  
	return if $file eq $current_project->remove_file;  
	
	# get file size and time
	my ($size, $age) = (stat($file))[7,9];
	
	# check age
	if ($project_age == 0) {
		# first file! seed with current data
		$project_age = $age;
	}
	elsif ($age > $project_age) {
		# file is younger, so take this time
		$project_age = $age;
	}
	
	# add to running total of file sizes
	$project_size += $size;
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


