#!/usr/bin/perl


use strict;
use File::Spec;
use File::Find;
use IO::File;
use File::Copy;
use POSIX qw(strftime);

# shortcut variable name to use in the find callback
use vars qw(*fname);
*fname = *File::Find::name;

# other predefined variables
use constant {
	SIXMOS   => 60 * 60 * 24 * 180,
	THREEMOS => 60 * 60 * 24 * 90,
	TYPE     => [qw(Other Analysis Alignment Fastq QC)],
};

my $VERSION = 3;

# boolean value to hide the files
# this will appease David and not tip off users by sudden appearance of additional files
my $hidden = 1;

# Documentation
unless (@ARGV) {
	print <<DOC;
  A script to prepare Repository project directories for uploading to Amazon via 
  Seven Bridges.
  
  It will generate the following text files
    - ID_MANIFEST.txt
    - ID_ARCHIVE_LIST.txt
    - ID_UPLOAD_LIST.txt
    - ID_REMOVE_LIST.txt
  
  Execute under GNU parallel to speed up the process. 
  Usage:  
    $0 /Repository/MicroarrayData/2010/1234R
    $0 /Repository/AnalysisData/2010/A5678
DOC
	exit;
}



# global arrays for storing file names
# these are needed since the File::Find callback doesn't accept pass through variables
my @manifest;
my %manifest2file; # hash for processing existing manifest
my @ziplist;
my @uploadlist;
my @removelist;
my $youngest_age = 0;
my $project;



# main loop
while (my $given_dir = shift @ARGV) {
	
	my $start_time = time;
	
	# extract the project ID
	if ($given_dir =~ m/(A\d{1,5}|\d{3,5}R)\/?$/) {
		# look for A prefix or R suffix project identifiers
		# this ignores Request digit suffixes such as 1234R1, 
		# when clients submitted replacement samples
		$project = $1;
		print " working on $project at $given_dir\n";
	}
	elsif ($given_dir =~ m/(\d{2,4})\/?$/) {
		# old style naming convention without an A prefix or R suffix
		$project = $1;
		print " working on $project at $given_dir\n";
	}
	else {
		warn "unable to identify project ID for $given_dir!\n";
		next;
	}
	
	# check directory and move into the parent directory if we're not there
	my $start_dir = './';
	if ($given_dir =~ s/^(\/Repository\/(?:MicroarrayData|AnalysisData)\/\d{4})\///) {
		$start_dir = $1;
		# print " changing to $start_dir\n";
		chdir $start_dir or die "can not change to $start_dir!\n";
	}
	
	# prepare file names
	my $manifest_file = File::Spec->catfile($given_dir, $project . "_MANIFEST.txt");
	my $ziplist_file  = File::Spec->catfile($given_dir, $project . "_ARCHIVE_LIST.txt");
	my $upload_file   = File::Spec->catfile($given_dir, $project . "_UPLOAD_LIST.txt");
	my $remove_file   = File::Spec->catfile($given_dir, $project . "_REMOVE_LIST.txt");
	my $zip_file      = File::Spec->catfile($given_dir, $project . "_ARCHIVE.zip");
	my $alt_manifest  = File::Spec->catfile($start_dir, $project . "_MANIFEST.txt");
	
	# find existing manifest file
	if (-e $manifest_file) {
		# we have a manifest file from a previous run
		load_manifest($manifest_file);
	}
	elsif (-e $alt_manifest) {
		# we have a hidden manifest file from a previous run
		load_manifest($alt_manifest);
	}
	
	# search directory using File::Find 
	find(\&callback, $given_dir);
	
	
	# fill out the removelist based on age
	if (time - $youngest_age < THREEMOS) {
		# keep everything here if date is less than three months
	}
	elsif (time - $youngest_age < SIXMOS) {
		# keep analysis and other files if date is less than six months
		foreach my $m (@manifest) {
			my @fields = split("\t", $m);
			next if ($fields[3] eq 'Alignment' or $fields[3] eq 'Fastq' or $fields[3] eq 'QC');
			push @removelist, $fields[4];
		}
	}
	else {
		# otherwise remove everything
		foreach my $m (@manifest) {
			my @fields = split("\t", $m);
			next if ($fields[3] eq 'Analysis' or $fields[3] eq 'QC');
			push @removelist, $fields[4];
		}
	}
	
	# write files
	push @uploadlist, $ziplist_file if @ziplist;
	push @uploadlist, $zip_file if @ziplist;
	push @uploadlist, $manifest_file if @uploadlist;
	write_file($manifest_file, \@manifest);
	write_file($ziplist_file, \@ziplist) if @ziplist;
	write_file($remove_file, \@removelist) if @removelist;
	write_file($upload_file, \@uploadlist) if @uploadlist;
	
	
	# temporarily move files out of directory
	if ($hidden) {
		move($manifest_file, $start_dir);
		move($ziplist_file, $start_dir);
		move($upload_file, $start_dir);
		move($remove_file, $start_dir);
	}
	
	# finished
	printf " finished with $project in %.1f minutes\n", (time - $start_time)/60;
	$youngest_age = 0;
	undef $project;
}



sub callback {
	my $file = $_;
	
	# ignore certain files
	return unless -f $file; # skip directories and symlinks!
	if ($file =~ /\.sra$/i) {
		# what the hell are SRA files doing in here!!!????
		push @removelist, $file;
		return;
	}
	elsif ($file =~ /libsnappyjava|fdt\.jar/) {
		# devil java spawn, delete!!!!
		push @removelist, $file;
		return;
	}
	elsif ($file eq '.DS_Store' or $file eq 'Thumbs.db') {
		# Windows and Mac file browser devil spawn, just ignore these, or maybe delete?
		return;
	}
	
	# stats on the file
	my @st = stat($file);
	my $date = strftime("%B %d, %Y %H:%M:%S", localtime($st[9]));
	my $size = $st[7];
	my $md5;
	
	# check for existing file information from previous manifest file
	if (exists $manifest2file{$fname}) {
		$md5 = $manifest2file{$fname};
	}
	else {
		# we will have to calculate the md5 checksum
		if (-e "$file\.md5") {
			# looks like we have pre-calculated md5 file, so please use it
			my $fh = IO::File->new("$file\.md5");
			my $line = $fh->getline;
			($md5, undef) = split(/\s+/, $line);
			$fh->close;
		}
		else {
			# calculate the md5 with external utility
			my $checksum = `md5sum \"$file\"`; # quote file, because we have files with ;
			($md5, undef) = split(/\s+/, $checksum);
		}
	}
	
	
	# check age - we are comparing the time in seconds from epoch which is easier
	$youngest_age = $st[9] if ($st[9] > $youngest_age);
	
	# check file type
	my $keeper = 0;
	if ($file =~ /\.(?:bw|bigwig|bb|bigbed|useq)$/i) {
		# an indexed analysis file
		$keeper = 1;
	}
	elsif ($file =~ /\.vcf\.gz$/i) {
		# a vcf file, check to see if there is a corresponding tabix index
		my $i = $file . '.tbi';
		$keeper = 1 if -e $i;
	}
	elsif ($file =~ /\.tbi$/i) {
		# a tabix index file, presumably alongside a vcf file
		my $v = $file;
		$v =~ s/\.tbi$//i;
		$keeper = 1 if -e $v;
	}
	elsif ($file =~ /\.(?:bam|bai|cram|crai|csi|sam\.gz)$/i) {
		# an alignment file
		$keeper = 2;
	}
	elsif (
		$file =~ m/^.*\d{4,5}[xX]\d+_.+_(?:sequence|sorted)\.txt\.gz(?:\.md5)?$/ or 
		$file =~ m/^\d{4,5}[xX]\d+_.+\.txt\.gz(?:\.md5)?$/ or 
		$file =~ m/\.(?:fastq|fq)(?:\.gz)?$/i
	) {
		# looks like a fastq file
		$keeper = 3;
	}
	elsif ($fname =~ /^$project\/(?:bioanalysis|Sample QC|Library QC)\//) {
		# these are QC samples in a bioanalysis or Sample of Library QC folder
		# directly under the main project 
		# hope there aren't any of these folders in Analysis!!!!!
		$keeper = 4;
	}
	
	# record the manifest information
	push @manifest, join("\t", $md5, $size, $date, TYPE->[$keeper], $fname);
	
	
	# record in appropriate lists
	if (int($size) < 100_000_000 and not $keeper) {
		push @ziplist, $fname;
# 		print "   archiving and removing $fname\n";
	}
	else {
		# everything else except for sample quality files
		push @uploadlist, $fname unless $keeper == 4;
# 		print "   uploading $fname\n";
	}
	
}


sub write_file {
	my ($file, $data) = @_;
	
	# open file
	my $fh = IO::File->new($file, 'w') or 
		die "can't write to $file! $!\n";
	
	# write manifest header
	if ($file =~ /MANIFEST/) {
		$fh->print("MD5\tSize\tModification_Time\tType\tName\n");
	}
	
	# print out the data
	while (my $d = shift @$data) {
		$fh->print("$d\n");
	}
	$fh->close;
	
	# change permissions to rw-rw-rw- 
	chmod 0666, $file;
}


sub load_manifest {
	my $file = shift;
	
	# open file
	my $fh = IO::File->new($file, 'r') or 
		die "can't read $file! $!\n";
	
	# process
	while (my $line = $fh->getline) {
		chomp($line);
		my @bits = split('\t', $line);
		$manifest2file{$bits[4]} = $bits[0];
	}
	$fh->close;
}


