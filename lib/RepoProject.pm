package RepoProject;


use strict;
use English qw(-no_match_vars);
use Carp;
use IO::File;
use Cwd;
use File::Spec;
use File::Copy;
use File::Path qw(make_path);
use File::Find;
use Digest::MD5;
use POSIX qw(strftime);

our $VERSION = 'v9.0.0';

### Initialize

# Initialize reusable checksum object
my $Digest = Digest::MD5->new;

# Initialize global find variables
my $current_project = undef;
my %ignore_files    = ();
my $project_age     = 0;
my $project_size    = 0;
my $autoanal_age    = 0;
my @months = qw(Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec);

sub new {
	my ($class, $path, $verbose) = @_;
	$verbose ||= 0;
		
	# check directory
	unless (-e $path) {
		carp "given path $path does not exist!";
		return;
	}
	if (substr($path,-1,1) eq '/') {
		# removing trailing slash, just in case
		$path = substr($path,0,-1);
	}
	unless (substr($path,0,1) eq '/') {
		# given path isn't rooted, try to fix
		$path = File::Spec->catdir($ENV{'PWD'}, $path);
	}

	# extract the project ID
	my $project;
	my $parent_dir;
	my @dirs = File::Spec->splitdir($path);
		# if path is full from root, the first element will be null
	if (
		$dirs[1] eq 'Repository' and 
		($dirs[2] eq 'MicroarrayData' or $dirs[2] eq 'AnalysisData')
	) {
		# full path representing our Repository file system
		if ($dirs[-1] =~ m/^( A \d{1,5} )$/x) {
			# Analysis project
			$project = $1;
		}
		elsif ($dirs[-1] =~ m/^( \d{3,5} R ) \d? $/x) {
			# Request project
			# this ignores Request digit suffixes such as 1234R1, 
			# when clients submitted replacement samples
			$project = $1;
		}
		else {
			# huh? just take last directory then
			$project = $dirs[-1];
		}
		$parent_dir = File::Spec->catdir(@dirs[0..$#dirs-1]);
	}
	else {
		# non-canonical path
		# set the project to the last given directory
		# set the parent directory to the same directory if parent is not writable
		$project = @dirs[-1];
		$parent_dir = File::Spec->catdir(@dirs[0..$#dirs-1]);
		unless ( -w $parent_dir ) {
			undef $parent_dir;
		}
	}

	# initiate project
	my $self = {
		given_dir   => $path,
		parent_dir  => $parent_dir,
		project     => $project,
		verbose     => $verbose,
	};
	
	# project files
	$self->{manifest}     = $project . '_MANIFEST.csv';
	$self->{prevmanifest} = $project . '_PREVIOUS_MANIFEST.csv';
	$self->{remove}       = $project . '_REMOVE_LIST.txt';
	$self->{prevremove}   = $project . '_PREVIOUS_REMOVE_LIST.txt';
	$self->{ziplist}      = $project . '_ARCHIVE_LIST.txt';
	$self->{prevziplist}  = $project . '_PREVIOUS_ARCHIVE_LIST.txt';
	$self->{zip}          = $project . '_ARCHIVE.zip';
	$self->{notice}       = 'where_are_my_files.txt';

	# hidden file names in parent directory
	if ($parent_dir) {
		$self->{alt_remove}  = File::Spec->catfile($parent_dir, $project . '_REMOVE_LIST.txt');
		$self->{alt_zip}     = File::Spec->catfile($parent_dir, $project . '_ARCHIVE.zip');
		$self->{alt_ziplist} = File::Spec->catfile($parent_dir, $project . '_ARCHIVE_LIST.txt');
		$self->{zipfolder}   = File::Spec->catfile($parent_dir, $project . '_ZIPPED_FILES');
		$self->{delfolder}   = File::Spec->catfile($parent_dir, $project . '_DELETED_FILES');
	}
	else {
		$self->{alt_remove}  = $self->{remove};
		$self->{alt_zip}     = $self->{zip};
		$self->{alt_ziplist} = $self->{ziplist};
		$self->{zipfolder}   = undef;
		$self->{delfolder}   = undef;
	}

	# notification file
	if ($parent_dir =~ /MicroarrayData/) {
		$self->{notice_source} = '/Repository/MicroarrayData/missing_file_notice.txt';
	}
	elsif ($parent_dir =~ /AnalysisData/) {
		$self->{notice_source} = '/Repository/AnalysisData/missing_file_notice.txt';
	}
	else {
		# primarily for testing purposes
		$self->{notice_source} = File::Spec->catfile( $ENV{HOME}, 'missing_file_notice.txt' );
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

sub previous_manifest_file {
	return shift->{prevmanifest};
}

sub remove_file {
	return shift->{remove};
}

sub previous_remove_file {
	return shift->{prevremove};
}

sub zip_file {
	return shift->{zip};
}

sub ziplist_file {
	return shift->{ziplist};
}

sub alt_ziplist_file {
	return shift->{alt_ziplist};
}

sub previous_ziplist_file {
	return shift->{prevziplist};
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
		carp "can't read $file! $OS_ERROR\n";
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
	
	my $fh = IO::File->new( $file) or return 1;
		# if we can't open the file, just skip it and return a dummy value
		# we'll likely have more issues with this file later
	$fh->binmode;
	my $md5 = $Digest->addfile($fh)->hexdigest;
	$fh->close;
	return $md5;
}





### Project functions

sub hide_deleted_files {
	my $self = shift;
	
	# check file list
	unless (-e $self->alt_remove_file) {
		printf "  ! no file remove list %s\n", $self->alt_remove_file;
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
		printf "   failed to move %s! $OS_ERROR\n", $self->alt_remove_file;
		$failure_count++;
	};
	
	# clean up empty directories
	$failure_count += $self->clean_empty_directories($self->given_dir);
	
	# we don't add a notice file here anymore because notice files are custom
	# and we need to know whether it was uploaded or not, best done by caller
	
	return $failure_count;
}

sub zip_archive_files {
	my $self = shift;

	# check zip list
	unless ( -e $self->alt_ziplist_file ) {
		printf "  ! %s has no alternate zip list file %s\n", $self->project || q(),
			$self->alt_ziplist_file;
		# sometimes this happens, there was nothing to zip, not necessarily an error
		return;
	}

	# An extensive internet search reveals no parallelized zip archiver, despite
	# there being a parallelized version of gzip, when both use the common DEFLATE 
	# algorithm. So we just run plain old zip.
	my $zipper = `which zip`;
	chomp $zipper;
	unless ($zipper) {
		print " ! no zip compression utility\n";
		return 1;
	}
	
	# first we move the zip list file
	chdir $self->given_dir; # just in case
	move( $self->alt_ziplist_file, $self->ziplist_file ) or do {
		printf " ! failed to move zip list file %s\n", $self->alt_ziplist_file;
		return 1;
	};
	
	# Zip the files
	my $command = sprintf("cat %s | $zipper --names-stdin %s", $self->ziplist_file, 
		$self->zip_file);
	print "  > executing: $command\n";
	my $result = system($command);
	if ($result) {
		print "     failed!\n";
		return 1;
	}
	elsif (not $result and -e $self->zip_file) {
		# zip appears successful
		# need to add these files to the manifest file
		my @zl_st  = stat($self->ziplist_file);
		my @zip_st = stat($self->zip_file);
		my $zl_md5 = $self->calculate_file_checksum( $self->ziplist_file );
		my $zip_md = $self->calculate_file_checksum( $self->zip_file );
		my $fh = IO::File->new( $self->manifest_file, '>>' ) or do {
			printf " ! failed to update manifest file %s: $OS_ERROR\n",
				$self->manifest_file;
			return 1;
		};
		$fh->printf( "%s\n%s\n", 
			join(',', ( sprintf(qq("%s"), $self->ziplist_file), 'Text', 'N', $zl_st[7], 
				sprintf(qq("%s"), strftime("%B %d, %Y %H:%M:%S", localtime($zl_st[9]) ) ),
				$zl_md5, q(), q(), q(), q() ) ),
			join(',', ( sprintf(qq("%s"), $self->zip_file), 'Archive', 'N', $zip_st[7], 
				sprintf(qq("%s"), strftime("%B %d, %Y %H:%M:%S", localtime($zl_st[9]) ) ),
				$zip_md, q(), q(), q(), q() ) ),
		);
		$fh->close;

		# update the remove list
		$fh = IO::File->new( $self->alt_remove_file, '>>' ) or do {
			printf " ! failed to update remove list file %s: $OS_ERROR\n",
				$self->alt_remove_file;
			return 1;
		};
		$fh->printf("%s\n", $self->zip_file);
		$fh->close;
		
		# finish up by hiding the zipped files
		print "  > moving zipped files\n";
		return $self->hide_zipped_files;
	}
	return;
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
		if (-e $self->remove_file) {
			move($self->remove_file, $self->alt_remove_file) or do {
				printf "   failed to unhide %s! $OS_ERROR\n", $self->remove_file;
				$failure_count++;
			};
		}
		
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
			printf "   failed to move file %s! $OS_ERROR\n", $self->alt_remove_file;
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
	
	# we don't add a notice file here anymore because notice files are custom
	# and we need to know whether it was uploaded or not, best done by caller
	
	return $failure_count;
}

sub delete_zipped_files {
	my $self = shift;
	chdir $self->given_dir; # just in case
	if (-e $self->ziplist_file) {
		my $filelist = $self->get_file_list($self->ziplist_file);
		my $fc = $self->_delete_directory_files('./', $filelist);
		$fc += $self->clean_empty_directories($self->given_dir);
		return $fc;
	}
	else {
		print "  No zip list file found to delete files!\n";
		return 1;
	}
}

sub add_notice_file {
	my $self = shift;
	carp " add_notice_file() is deprecated!";
	return 1;
}

sub write_deleted_files_notice {
	my $self  = shift;
	my $Entry = shift;
	unless ( $Entry and ref($Entry) eq 'RepoEntry' ) {
		carp ' ! Must pass a Catalog RepoEntry object!';
		return 1;
	}
	chdir $self->given_dir; # just in case
	
	# delete the old one, this might be a symbolic link
	if ( -e $self->notice_file ) {
		unlink $self->notice_file;
	}
	
	# generate text
	my $text1 = <<~DOC;
	######## Where are my files? ###########
	
	The project %s, "%s", was removed on %s %02d, %d due to space limitations.
	
	The owners were notified of this on %s %02d, %d.
	
	See the policy on data storage at
	https://uofuhealth.utah.edu/huntsman/shared-resources/gcb/cbi/data-access-storage
	
	Several files may be retained here. These include the following:
	
	1. %s
	
	This is a comma-separated-value text file listing the files that were present
	in this project, including the file name, date, size in bytes, and MD5 checksum.
	
	2. %s
	
	This is a list of the filenames that were removed from this project.
	
	DOC
	
	my $text2;
	if ( $Entry->is_request ) {
		$text2 = <<~DOC;
		3. QC Files
		
		Certain Quality Control files, for example Sample QC and Library QC, are
		always retained.
		
		DOC
	}
	else {
		$text2 = <<~DOC;
		3. Certain analysis files
		
		Certain indexed analysis files may be retained for the convenience of viewing
		them distributed through GNomEx to genome browsers.
		
		DOC
	}
	
	my $text3 = <<~DOC;
	
	####### Questions
	
	If you have any questions, please submit a ticket to the Cancer Bioinformatics
	Shared Resource through our web page at
	
	https://uofuhealth.utah.edu/huntsman/shared-resources/gcb/cbi

	DOC
	
	my $fh = IO::File->new( $self->notice_file, '>' );
	unless ($fh) {
		printf " ! unable to write notice file %s! %s\n", $self->notice_file, $OS_ERROR;
		return 1;
	}
	my @hide  = localtime( $Entry->hidden_datestamp );
	my @email = localtime( $Entry->emailed_datestamp );
	$fh->printf( $text1, $Entry->id, $Entry->name, $months[ $hide[4] ], $hide[3],
		$hide[5] + 1900, $months[ $email[4] ], $email[3], $email[5] + 1900,
		$self->manifest_file, $self->remove_file,  );
	$fh->print($text2);
	$fh->print($text3);
	$fh->close;
	return 0;
}

sub write_uploaded_files_notice {

	my $self  = shift;
	my $Entry = shift;
	unless ( $Entry and ref($Entry) eq 'RepoEntry' ) {
		carp ' ! Must pass a Catalog RepoEntry object!';
		return 1;
	}
	chdir $self->given_dir; # just in case
	
	# delete the old one, this might be a symbolic link
	if ( -e $self->notice_file ) {
		unlink $self->notice_file;
	}
	
	# generate text
	my $text1 = <<~DOC;
	######## Where are my files? ###########
	
	The project %s, "%s", was removed on %s %02d, %d due to space limitations.
	
	The files have been uploaded to the AWS account "%s" in the bucket "%s".
	
	You may view these files in CORE Browser using the link below:
	
	%s
	
	Several files may be retained here. These include the following:
	
	1. %s
	
	This is a comma-separated-value text file listing the files that were present
	in this project, including the file name, date, size in bytes, and MD5 checksum.
	
	2. %s
	
	This is a list of the filenames that were removed from this project.
	
	DOC
	
	my $text2;
	if ( $Entry->is_request ) {
		$text2 = <<~DOC;
		3. QC Files
		
		Certain Quality Control files, for example Sample QC and Library QC, are
		always retained.
		
		DOC
	}
	
	my $text3;
	if ( $Entry->is_request and $Entry->autoanal_folder ) {
		$text3 = <<~DOC;
		4. %s
		
		This is a text file containing the list of file names included in the Zip Archive file "%s".
		This is a convenience file to identify the contents without opening the file.
		
		DOC
	}
	elsif ( not $Entry->is_request ) {
		$text3 = <<~DOC;
		3. %s
		
		This is a text file containing the list of file names included in the Zip Archive file "%s".
		This is a convenience file to identify the contents without opening the file.
		
		4. Certain analysis files
		
		Certain indexed analysis files may be retained for the convenience of viewing
		them distributed through GNomEx to genome browsers.
		
		DOC
	}
	
	my $text4 = <<~DOC;
	
	####### Questions
	
	If you have any questions, please submit a ticket to the Cancer Bioinformatics
	Shared Resource through our web page at
	
	https://uofuhealth.utah.edu/huntsman/shared-resources/gcb/cbi

	DOC
	
	my $fh = IO::File->new( $self->notice_file, '>' );
	unless ($fh) {
		printf " ! unable to write notice file %s! %s\n", $self->notice_file, $OS_ERROR;
		return 1;
	}
	my @hide  = localtime( $Entry->hidden_datestamp );
	$fh->printf( $text1, $Entry->id, $Entry->name, $months[ $hide[4] ], $hide[3],
		$hide[5] + 1900, $Entry->core_lab, $Entry->bucket, $Entry->project_core_url,
		$self->manifest_file, $self->remove_file );
	if ($text2) {
		$fh->print($text2);
	}
	if ($text3) {
		$fh->printf( $text3, $self->ziplist_file, $self->zip_file );
	}
	$fh->print($text4);
	$fh->close;
	return 0;
}

sub clean_empty_directories {
	my $self = shift;
	my $directory = shift;
	
	my $command = sprintf("find %s -type d -empty -delete", $directory);
	print "  > executing: $command\n" if $self->verbose;
	if (system($command)) {
		print "   ! find command '$command' failed! $OS_ERROR\n";
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
	$autoanal_age    = 0;
	%ignore_files    = map { $_ => 1 } (
		$self->manifest_file,
		$self->zip_file,
		$self->ziplist_file,
		$self->remove_file,
		$self->previous_manifest_file,
		$self->previous_remove_file,
		$self->previous_ziplist_file,
		$self->notice_file,
	);
	
	# collect data for given directory
	find( {
			follow => 0, # do not follow symlinks
			wanted => \&_age_callback,
		  }, $self->given_dir
	);
	
	# return size in bytes and oldest posix age (youngest file)
	return ($project_size, $project_age, $autoanal_age);
}

sub get_autoanal_folder {
	my $self = shift;
	return unless ($self->project =~ /^\d+R$/);
	my $curdir = getcwd();
	chdir $self->given_dir;
	my @results = glob("AutoAnalysis_*");
	my $aapath = q();
	if ( scalar @results == 1 and $results[0] ) {
		$aapath = $results[0];
	}
	elsif ( scalar @results > 1 ) {
		printf "   ! more than one AutoAnalysis folders found for %s\n", $self->project;
		$aapath = join(',', @results);
	}
	chdir $curdir;
	return $aapath;
}

sub has_fastq {
	my $self = shift;
	if ( $self->project =~ /^ (\d+) R $/x) {
		my $dir = sprintf "%s/Fastq", $self->given_dir;

		# fastq files may be in Fastq folder or a sample subdirectory therein
		# they may have .gz or .ora or other extension depending on compression
		return 0 unless ( -e $dir);
		my @results = glob( sprintf("%s/*.fastq.* %s/%sX*/*.fastq.*", $dir, $dir, $1 ) );
		return scalar(@results);
	}
	else {
		return 0;
	}
}

sub reset_list_files {
	my $self = shift;
	my $curdir = getcwd();
	chdir $self->given_dir;
	if ( -e $self->previous_manifest_file ) {
		printf "  ! project %s already reset, unable to continue\n", $self->project;
		chdir $curdir;
		return;
	}
	if ( -e $self->manifest_file ) {
		move( $self->manifest_file, $self->previous_manifest_file ) or do {
			printf "  ! Failed to move %s: %s\n", $self->manifest_file, $OS_ERROR;
		}	
	}
	else {
		printf "  ! project %s not scanned, unable to reset\n", $self->project;
		chdir $curdir;
		return;
	}
	if ( -e $self->remove_file ) {
		move( $self->remove_file, $self->previous_remove_file ) or do {
			printf "  ! Failed to move %s: %s\n", $self->remove_file, $OS_ERROR;
		}	
	}
	if ( -e $self->ziplist_file ) {
		move( $self->ziplist_file, $self->previous_ziplist_file ) or do {
			printf "  ! Failed to move %s: %s\n", $self->ziplist_file, $OS_ERROR;
		}	
	}
	chdir $curdir;
	return 1;
}


#### Internal functions

sub _move_directory_files {
	my ($self, $source, $destination, $filelist) = @_; # could be either zipped or deleted
	my $failure_count = 0;
	
	unless (scalar @{$filelist}) {
		print "   no files in list! Nothing moving\n";
		return 0;
	}
	
	# process the removelist
	foreach my $file (@{$filelist}) {
		my (undef, $dir, $basefile) = File::Spec->splitpath($file);
		
		# source file
		my $sourcefile = $self->_check_file($source, $dir, $basefile);
		unless ($sourcefile) {
			printf "   ! Missing or non-file to move: %s\n", $file;
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
			printf "   ! Failed to move %s: %s\n", $sourcefile, $OS_ERROR;
			$failure_count++;
		};
	}
	
	return $failure_count;
}

sub _delete_directory_files {
	my ($self, $target_dir, $filelist) = @_;
		
	unless (scalar @{$filelist}) {
		print "   no files in list! Nothing deleted\n";
		return 0;
	}
	
	# process the removelist
	my $failure_count = 0;
	foreach my $file (@{$filelist}) {
		my (undef, $dir, $basefile) = File::Spec->splitpath($file);
		my $targetfile = $self->_check_file($target_dir, $dir, $basefile);
		unless ($targetfile) {
			printf "   ! Missing or non-file to delete: %s\n", $file;
			next;
		}
		print "   DELETING $targetfile\n" if $self->verbose;
		unlink($targetfile) or do {
			printf "   ! Failed to remove %s: %s\n", $targetfile, $OS_ERROR;
			$failure_count++;
		};
	}
	
	return $failure_count;
}

sub _check_file {
	# this checks whether the file exists, handling links and broken links and
	# odd cases where the file has been moved to a special directory
	# or really old file lists that record the project ID in the path
	# directories and special files shouldn't be given, the will return undefined
	# as will missing files
	
	my ($self, $target_dir, $dir, $basefile) = @_;
	return undef unless ($target_dir and $basefile);
	my $targetfile = File::Spec->catfile($target_dir, $dir, $basefile);
	# need to check whether it is a symlink first, which forces an lstat
	# other file tests simply follow the symlink to the referent and runs stat 
	# which is problematic if the referent was zipped up and removed, failing the test
	if ( -l $targetfile ) {
		return $targetfile;
	}
	elsif ( -f $targetfile ) {
		return $targetfile;
	}
	else {
		# older versions may record the project folder in the list file name, so let's 
		# try removing that prefix
		my $p = $self->id;
		$dir =~ s/^$p\///;
		$targetfile = File::Spec->catfile($target_dir, $dir, $basefile);
		if ( -l $targetfile ) {
			return $targetfile;
		}
		elsif ( -f $targetfile ) {
			return $targetfile;
		}		
	}
	return undef;	
}

sub _age_callback {
	my $file = $_;
	
	# skip specific files, including project manifest files
	return if -d $file;
	return if -l $file;
	return if exists $ignore_files{$file};
	return if $file =~ m/md5/i;   # too small and variable to make a difference

	# for Request projects, don't calculate age if file appears to be a QC folder
	# these are considered supplementary and can be updated anytime
	# we keep Autoanalysis file age separately
	# all other files we keep the age by default
	my $include_age = 1;
	if ( $File::Find::name =~
		m/ \d+R \/ (?: Sample.?QC | Library.?QC | Sequence.?QC | Cell.Prep.QC ) \/ /x
	) {
		$include_age = 0;
	}
	elsif ( $File::Find::name =~ m/ \d+R \/ AutoAnalysis_\w+ \/ /x ) {
		$include_age = 2;
	}
	
	# get file size and time date stamp
	my ($size, $age) = (stat($file))[7,9];
	
	# add to running total of file sizes, even for files which we don't keep age
	$project_size += $size;
	
	# check age
	if ( $include_age == 1 and $age > $project_age ) {
		$project_age = $age;
	}
	elsif ( $include_age == 2 and $age > $autoanal_age ) {
		$autoanal_age = $age;
	}
}

1;

__END__

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

=item alt_ziplist_file

Returns path and name of alternate or zip archive list file. Example: 
F</Repository/MicroarrayData/2019/1234R_ARCHIVE_LIST.txt>

=item previous_manifest_file

Returns name of the previous manifest file. Example: F<1234R_PREVIOUS_MANIFEST.csv>.

=item previous_remove_file

Returns name of the previous_remove list file.
Example: F<1234R_PREVIOUS_REMOVE_LIST.txt>

=item previous_ziplist_file

Returns name of the previous zip archive list file.
Example: F<1234R_PREVIOUS_ARCHIVE_LIST.txt>.

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

=back

=head2 Project information

These functions gather information about the project folder.

=over 4

=item get_size_age

Returns three values: the total size in bytes, the highest posix age
(youngest file) for the Project, and the highest posix age (youngest
file) for an AutoAnalysis folder. The recursive search includes the 
primary folder. For Request projects, the file age is not considered
for files in various QC folders since these are considered secondary,
subject to change, and not considered for project expiration. However,
we return Request AutoAnalysis folder file age separately for
informational purposes. 

=item get_autoanal_folder

Searches for and returns the name of a Request project's AutoAnalysis
folder. These have a date appended to the name, and therefore
inconsistently named from project to project. Does not include the 
full path.

=item has_fastq

Returns true (number of found files) or false (0) if a Request project
contains identifiable fastq files in a C<Fastq> folder.

=back

=head2 Repository file functions

These are functions to act on the files within a repository. 
In all cases, the repository file must be scanned and list 
files generated before any of these functions can be applied. 
See the accompanying scripts for scanning Analysis and Request 
projects. 

When L<verbose> is set to true, file names are printed to
standard out as they are being moved or deleted. When failures
occur, file names are printed to standard out. 

B<IMPORTANT> These functions return a failure count. A return
of zero is success. 

B<IMPORTANT> These functions will change into the directory to
perform the indicated action, but they do not change back to
the previous directory. You should handle this as appropriate.

=over 4

=item zip_archive_files

Generates a Zip archive of the project files using the external
C<zip> utility, which must be in the environment C<PATH>. Only
the files listed in the alternate zip list file will be included
in the archive. The zip list is moved into the project folder.
Both the zip list and zip archive file metadata are added to the
manifest files. The Zip archive file is added to the alternate
remove list file. If successful, the L<hide_zipped_files> 
function is automatically subsequently run.

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

=item write_deleted_files_notice($Entry)

Write a custom notice file for deleted projects. A L<RepoEntry>
object must be passed.

=item write_uploaded_files_notice($Entry)

Write a custom notice file for uploaded projects. Include information
for archived files and direct link to CORE Browser. A L<RepoEntry>
object must be passed.

=item clean_empty_directories

Executes an external C<find> command in a sub shell to recursively 
search for and delete empty subdirectories. Pass the directory to 
search. Returns the return status from the C<find> command.

=item reset_list_files

Renames existing manifest, remove list, and archive list files
to new names with "_PREVIOUS" prefix. These are ignored when
scanning and effectively resets a project while maintaining a
history.

=back

=head1 AUTHOR

 Timothy J. Parnell, PhD
 Bioinformatics Shared Resource
 Huntsman Cancer Institute
 University of Utah
 Salt Lake City, UT, 84112

This package is free software; you can redistribute it and/or modify
it under the terms of the Artistic License 2.0.  


