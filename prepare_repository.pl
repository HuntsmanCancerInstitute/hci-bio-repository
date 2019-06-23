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
	TYPE     => [qw(Other Analysis Alignment Fastq)],
};


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
my @ziplist;
my @uploadlist;
my @removelist;
my $youngest_age = 0;



# main loop
while (my $given_dir = shift @ARGV) {
	
	my $start_time = time;
	
	# extract the project ID
	my $project;
	if ($given_dir =~ m/(A\d{1,5}|\d{3,5}R)\/?$/) {
		$project = $1;
		print " working on $project at $given_dir\n";
	}
	elsif ($given_dir =~ m/(\d{2,4})\/?$/) {
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
	push @uploadlist, $manifest_file, $ziplist_file, $zip_file;
	
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
			push @removelist, $fields[4] if ($fields[3] eq 'Alignment' or $fields[3] eq 'Fastq');
		}
	}
	else {
		# otherwise remove everything
		foreach my $m (@manifest) {
			my @fields = split("\t", $m);
			push @removelist, $fields[4] unless ($fields[3] eq 'Analysis');
		}
	}
	
	# write files
	write_file($manifest_file, \@manifest);
	write_file($ziplist_file, \@ziplist);
	write_file($upload_file, \@uploadlist);
	write_file($remove_file, \@removelist);
	
	# move files out of directory
	# this will appease David and not tip off users by sudden appearance of additional files
	move($manifest_file, $start_dir);
	move($ziplist_file, $start_dir);
	move($upload_file, $start_dir);
	move($remove_file, $start_dir);
	
	# finished
	printf " finished with $project in %.1f minutes\n", (time - $start_time)/60;
	$youngest_age = 0;
}



sub callback {
	my $file = $_;
	
	# ignore certain files
	return if -d $file; # skip directories!
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
	
	# stats on the file
	my @st = stat($file);
	my $md5;
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
	
	# check age
	$youngest_age = $st[9] if ($st[9] > $youngest_age);
	
	# check file type
	my $keeper;
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
	elsif ($file =~ /\.(?:bam|bai|cram|crai|sam\.gz)$/i) {
		# an alignment file
		$keeper = 2;
	}
	elsif (
		$file =~ m/^.*\d{4,5}[xX]\d+_.+_(?:sequence|sorted)\.txt\.gz(?:\.md5)?$/ or 
		$file =~ m/^\d{4,5}X\d+_.+\.txt\.gz(?:\.md5)?$/ or 
		$file =~ m/\.(?:fastq|fq)(?:\.gz)?$/i
	) {
		# looks like a fastq file
		$keeper = 3;
	}
	
	# record the manifest information
	push @manifest, join("\t", $md5, $st[7], 
		strftime("%B %d, %Y %H:%M:%S", localtime($st[9])), TYPE->[$keeper], $fname);
	
	
	# record in appropriate lists
	if (int($st[7]) < 100_000_000 and not $keeper) {
		push @ziplist, $fname;
# 		print "   archiving and removing $fname\n";
	}
	else {
		# everything else
		push @uploadlist, $fname;
# 		print "   uploading $fname\n";
	}
	
# 	elsif ($keeper == 1) {
# 		# we will archive these to amazon, but also keep locally for users, so don't remove
# 		push @uploadlist, $fname;
# 		print "   uploading $fname\n";
# 	}
# 	elsif ($keeper >= 2) {
# 		# these will get archived and then removed locally
# 		push @uploadlist, $fname;
# 		# push @removelist, $fname;
# 		print "   uploading and removing $fname\n";
# 	}
# 	else {
# 		# everything else
# 		push @uploadlist, $fname;
# 		# push @removelist, $fname;
# 		print "   uploading and removing $fname\n";
# 	}
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





