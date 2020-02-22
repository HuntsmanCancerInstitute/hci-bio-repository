package RepoProject;
our $VERSION = 4.0;

=head1 NAME 

ProjectTools - Common functions for HCI-Bio-Repository projects

=head1 DESCRIPTION

Common Perl functions for the scripts in the HCI-Bio-Repository package. 
All functions are exported by default.

=head1 FUNCTIONS

to be written....

=cut

use strict;
use IO::File;
use File::Spec;
use File::Copy;
use File::Path qw(make_path);
use Digest::MD5;

1;

### Initialize

# Initialize reusable checksum object
my $Digest = Digest::MD5->new;

sub new {
	my ($class, $path, $verbose) = @_;
	$verbose ||= 0;
		
	# check directory
	unless ($path =~ /^\//) {
		die "given path does not begin with / Must use absolute paths!\n";
	}
	unless (-e $path) {
		die "given path $path does not exist!\n";
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
	
	my $fh = IO::File->new($file, 'r') or 
		die "can't read $file! $!\n";
	
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
		my $sourcefile = $self->_check_file(
			File::Spec->catfile($source, $dir, $basefile));
		next unless $sourcefile;
		(undef, $dir, $basefile) = File::Spec->splitpath($sourcefile); 
			# regenerate these in case they've been changed
		
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
		my $targetfile = $self->_check_file(
			File::Spec->catfile($target_dir, $dir, $basefile));
		next unless $targetfile;
		print "   DELETING $targetfile\n" if $self->verbose;
		unlink($targetfile) or do {
			print "   failed to remove $targetfile! $!\n";
			$failure_count++;
		};
	}
	
	return $failure_count;
}

sub _check_file {
	my ($self, $file) = @_;
	# check file exist and not a link
	return undef unless $file;
	return undef if -l $file;
	return $file if -f $file;
	# older versions may record the project folder in the name, so let's 
	# try removing that
	my $p = $self->project;
	$file =~ s/^$p\///;
	return $file if -f $file;
	return undef;	
}


__END__

=head1 AUTHOR

 Timothy J. Parnell, PhD
 Dept of Oncological Sciences
 Huntsman Cancer Institute
 University of Utah
 Salt Lake City, UT, 84112

This package is free software; you can redistribute it and/or modify
it under the terms of the Artistic License 2.0.  


