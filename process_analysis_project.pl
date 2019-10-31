#!/usr/bin/perl


use strict;
use IO::File;
use File::Spec;
use File::Find;
use File::Copy;
use File::Path qw(make_path);
use POSIX qw(strftime);
use Getopt::Long;
use FindBin qw($Bin);
use lib $Bin;
use SB;

my $version = 2.1;

# shortcut variable name to use in the find callback
use vars qw(*fname);
*fname = *File::Find::name;



######## Documentation
my $doc = <<END;

A script to process GNomEx Analysis project folders for 
Seven Bridges upload.

It will inventory files, classify them based on known file 
type extensions, and calculate basic metadata, including size 
in bytes, date, and MD5 checksum. It will write a CSV manifest 
file. It will also a write a list of all files targeted for 
deletion at a later date. The deletion list file is written in 
the parent directory.

Options to compress and/or archive small files are available. 
Known files that exceed a minimum specified size may be gzip 
compressed. All files, except known indexed Analysis files, 
below a specified maximum file size may be compressed into a 
single Zip archive. Files that are zip archived are moved into 
a hidden folder in the parent directory.

This requires collecting data from the GNomEx LIMS database and 
passing certain values on to this script for inclusion as metadata 
in the metadata Manifest CSV file. However, the current bulk 
uploader does not simultaneously handle CSV metadata and recursive 
paths. Therefore metadata is not set upon uploading to Seven Bridges.

A Seven Bridges Project is automatically generated when uploading,
using the project identifier as the name. A Markdown description is
generated for the Project using the GNomEx metadata, including user
name, title, group name, and genome version. 

Version: $version

Usage:
    process_analysis_project.pl [options] /Repository/AnalysisData/2019/A5678

Options:

 Mode
    --scan              Scan the project folder and generate the manifest
    --gz                GZip compress known text files while scanning
    --zip               Zip archive small files when scanning
    --upload            Run the sbg-uploader program to upload
    --hide              Hide the deleted files in hidden folder

 Metadata
    --first <text>      User first name for the owner of the project
    --last <text>       User last name for the owner of the project
    --title "text"      GNomEx Request Name or title. This is a long text 
                        field, so must be protected by quoting. It will 
                        become the name of SB project.
    --group "text"      GNomEx Request group or ProjectName. This is a long 
                        text field, so must be protected by quoting.
    --species "text"    Specify the species, such as Homo sapiens or 
                        Mus musculus. Also accepts: human, 
                        mouse, rat, yeast, zebrafish, worm.
    --genome <text>     Genome build version, such as hg38 or mm10. 
                        Recognized forms will be simplified by regex.

 Options
    --all               Mark everything, including Analysis files, for deletion
    --desc "text"       Description for new SB project when uploading. 
                        Can be Markdown text.
    --mingz <integer>   Minimum size in bytes to gzip text files (10 KB)
    --maxzip <integer>  Maximum size in bytes to avoid zip archiving (100 MB)
    --verbose           Tell me everything!
 
 Seven Bridges
    --division <text>   The Seven Bridges division name. Should be name of 
                        section in the credentials file. Default 'default'.

 Paths
    --sb <path>         Path to the Seven Bridges command-line api utility sb
    --sbup <path>       Path to the Seven Bridges Java uploader start script,
                        sbg-uploader.sh
    --cred <path>       Path to the Seven Bridges credentials file. 
                        Default is ~/.sevenbridges/credentials. Each profile 
                        should be named after the SB division.

END



######## Process command line options
my $given_dir;
my $scan;
my $zip;
my $gzip;
my $hide_files;
my $upload;
my $userfirst           = q();
my $userlast            = q();
my $title               = q();
my $group               = q();
my $species             = q();
my $genome              = q();
my $description         = q();
my $min_gz_size         = 10000;
my $max_zip_size        = 100000000;
my $sb_division         = q(default);
my $sb_path             = q();
my $sbupload_path       = q();
my $cred_path           = q();
my $everything;
my $verbose;

if (scalar(@ARGV) > 1) {
	GetOptions(
		'scan!'         => \$scan,
		'zip!'          => \$zip,
		'gz!'           => \$gzip,
		'hide!'         => \$hide_files,
		'upload!'       => \$upload,
		'first=s'       => \$userfirst,
		'last=s'        => \$userlast,
		'title=s'       => \$title,
		'group=s'       => \$group,
		'species=s'     => \$species,
		'genome=s'      => \$genome,
		'desc=s'        => \$description,
		'mingz=i'       => \$min_gz_size,
		'all!'          => \$everything,
		'division=s'    => \$sb_division,
		'sb=s'          => \$sb_path,
		'sbup=s'        => \$sbupload_path,
		'cred=s'        => \$cred_path,
		'verbose!'      => \$verbose,
	) or die "please recheck your options!\n\n";
}
else {
	print $doc;
	exit;
}
$given_dir = shift @ARGV;


# Start up message for log tracking
print " > working on $given_dir\n";




######## Check options
if ($scan) {
	die "must provide user first name to scan!\n" unless $userfirst;
	die "must provide user last name to scan!\n" unless $userlast;
}
if ($upload) {
	die "must provide a SB division name!\n" unless $sb_division;
	die "must provide a title for SB project!\n" unless $title;
}




######## Global variables
# these are needed since the File::Find callback doesn't accept pass through variables
my $start_time = time;
my @removelist;
my @ziplist;
my $project;
my %filedata;


# external commands
my ($gzipper, $zipper);
if ($gzip) {
	$gzipper = `which pigz`;
	chomp $gzipper;
	if ($gzipper) {
		$gzipper .= ' -p 4'; # run with four cores
	}
	else {
		$gzipper = 'gzip';
	}
	if ($verbose) {
		print " => gzip compression: $gzipper\n";
	}
}
if ($zip) {
	$zipper = `which zip`;
	chomp $zipper;
	unless ($zipper) {
		print " ! no zip compression utility, disabling\n";
		$zip = 0;
	}
	if ($verbose) {
		print " => zip archiving utility: $zipper\n";
	}
}


### Species
# the SB metadata expects simple values, so define this by matching with regex
my $sb_species;
if ($species =~ /human|sapiens/i) {
	$sb_species = 'Homo sapiens';
}
elsif ($species =~ /mouse|musculus/i) {
	$sb_species = 'Mus musculus';
}
elsif ($species =~ /zebrafish/i) {
	$sb_species = 'Danio rerio';
}
elsif ($species =~ /rat/i) {
	$sb_species = 'Rattus norvegicus';
}
elsif ($species =~ /fly|melanogaster/i) {
	$sb_species = 'Drosophila melanogaster';
}
elsif ($species =~ /yeast|cerevisiae/i) {
	$sb_species = 'Saccharomyces cerevisiae';
}
elsif ($species =~ /worm|elegans/i) {
	$sb_species = 'Caenorhabditis elegans';
}
else {
	# default is take what they give us and hope for the best
	$sb_species = $species;
}

### Genome version
# generally defaulting to the UCSC short hand names for the most common
my $sb_genome;
# human
if ($genome =~ /(hg\d+)/i) {
	$sb_genome = $1;
}
elsif ($genome =~ /(grch\d+)/i) {
	$sb_genome = $1;
}
elsif ($genome =~ /B37/) {
	$sb_genome = 'GRCh37';
}
elsif ($genome =~ /B38/) {
	$sb_genome = 'GRCh38';
}
# mouse
elsif ($genome =~ /(mm\d+)/i) {
	$sb_genome = $1;
}
elsif ($genome =~ /(grcm\d+)/i) {
	$sb_genome = $1;
}
# zebrafish
elsif ($genome =~ /(zv\d+)/i) {
	$sb_genome = $1;
}
# yeast
elsif ($genome =~ /(saccer\d)/i) {
	$sb_genome = $1;
}
# fly
elsif ($genome =~ /(dm\d+)/i) {
	$sb_genome = $1;
}
# worm
elsif ($genome =~ /(ce\d+)/i) {
	$sb_genome = $1;
}
# what else?
else {
	$sb_genome = $genome;
}


####### Check directories

# empirical directories
if ($verbose) {
	print " => SB path: $sb_path\n" if $sb_path;
	print " => SB uploader path: $sbupload_path\n" if $sbupload_path;
	print " => SB credentials path: $cred_path\n" if $cred_path;
}

# check directory
unless ($given_dir =~ /^\//) {
	die "given path does not begin with / Must use absolute paths!\n";
}
unless (-e $given_dir) {
	die "given path $given_dir does not exist!\n";
}

# extract the project ID
if ($given_dir =~ m/(A\d{1,5}|\d{3,5}R)\/?$/) {
	# look for A prefix or R suffix project identifiers
	# this ignores Request digit suffixes such as 1234R1, 
	# when clients submitted replacement samples
	$project = $1;
}
elsif ($given_dir =~ m/(\d{2,4})\/?$/) {
	# old style naming convention without an A prefix or R suffix
	$project = $1;
}
else {
	# non-canonical path, take the last given directory
	my @dir = File::Spec->splitdir($given_dir);
	$project = @dir[-1];
}


# check directory and move into the parent directory if we're not there
my $parent_dir = './';
if ($given_dir =~ m/^(\/Repository\/(?:MicroarrayData|AnalysisData)\/\d{4})\/?/) {
	$parent_dir = $1;
}
elsif ($given_dir =~ m/^(.+)$project\/?$/) {
	$parent_dir = $1;
}
print "   using parent directory $parent_dir\n" if $verbose;

# change to the given directory
print " > changing to $given_dir\n" if $verbose;
chdir $given_dir or die "cannot change to $given_dir!\n";







####### Prepare file names

# file names in project directory
my $manifest_file = $project . "_MANIFEST.csv";
my $remove_file   = $project . "_REMOVE_LIST.txt";
my $ziplist_file  = $project . "_ARCHIVE_LIST.txt";
my $zip_file      = $project . "_ARCHIVE.zip";
my $notice_file   = "where_are_my_files.txt";

# hidden file names in parent directory
my $alt_remove    = File::Spec->catfile($parent_dir, $project . "_REMOVE_LIST.txt");
my $alt_zip       = File::Spec->catfile($parent_dir, $project . "_ARCHIVE.zip");
my $alt_ziplist   = File::Spec->catfile($parent_dir, $project . "_ARCHIVE_LIST.txt");

if ($verbose) {
	print " =>  manifest file: $manifest_file\n";
	print " =>    remove file: $remove_file or $alt_remove\n";
	print " =>  zip list file: $ziplist_file or $alt_ziplist\n";
	print " =>       zip file: $zip_file or $alt_zip\n";
}


# zipped file hidden folder
my $zipped_folder = File::Spec->catfile($parent_dir, $project . "_ZIPPED_FILES");
print " =>  zipped folder: $zipped_folder\n" if $verbose;
if (-e $zipped_folder) {
	if ($scan) {
		print " ! cannot re-scan if zipped files hidden folder exists!\n";
		$scan = 0;
	}
	if ($zip) {
		print " ! zipped files hidden folder already exists! Will not zip\n";
		$zip = 0; # do not want to rezip
	}
}


# removed file hidden folder
my $deleted_folder = File::Spec->catfile($parent_dir, $project . "_DELETED_FILES");
print " => deleted folder: $deleted_folder\n" if $verbose;
if (-e $deleted_folder) {
	if ($hide_files) {
		print " ! deleted files hidden folder already exists! Will not move deleted files\n";
		$hide_files = 0; # do not want to move zipped files
	} 
	if ($scan) {
		print "! cannot re-scan if deleted files hidden folder exists!\n";
		$scan = 0;
	}
}

# notification file
my $notice_source_file;
if ($given_dir =~ /MicroarrayData/) {
	$notice_source_file = "/Repository/MicroarrayData/missing_file_notice.txt";
}
elsif ($given_dir =~ /AnalysisData/) {
	$notice_source_file = "/Repository/AnalysisData/missing_file_notice.txt";
}
else {
	# primarily for testing purposes
	$notice_source_file = "~/missing_file_notice.txt";
}






######## Main functions

# scan the directory
if ($scan) {
	# this will also run the zip function
	print " > scanning project $project in directory $parent_dir\n";
	scan_directory();
}


# upload files to Seven Bridges
if ($upload) {
	if (-e $manifest_file) {
		print " > uploading project $project files to $sb_division\n";
		upload_files();
	}
	else {
		print " ! No manifest file! Cannot upload files\n";
	}
}


# hide files
if ($hide_files) {
	if (-e $alt_remove) {
		print " > moving project $project files to $deleted_folder\n";
		hide_deleted_files();
	}
	else {
		print " ! No deleted files to hide\n";
	}
}



######## Finished
printf " > finished with $project in %.1f minutes\n\n", (time - $start_time)/60;









####### Functions ##################

sub scan_directory {
	
	### search directory recursively using File::Find 
	# remember that we are in the project directory, so we search current directory ./
	# results from the recursive search are processed with callback() function and 
	# written to global variables - the callback doesn't support passed data 
	
	# search
	find( {
			follow => 0, # do not follow symlinks
			wanted => \&callback,
		  }, '.'
	);
	
	# confirm
	if (not scalar keys %filedata) {
		print "  > nothing found!\n";
		return;
	}
	else {
		printf "  > processed %d files\n", scalar keys %filedata;
	}
	
	### Generate manifest file

	# compile lists
	my @manifest;
	push @manifest, join(',', qw(File Type species reference_genome UserFirstName UserLastName Size Date MD5));
	foreach my $f (sort {$a cmp $b} keys %filedata) {
		
		# set genome information for files that need it
		my ($file_species, $file_genome) = ('','');
		my $type = $filedata{$f}{type};
		if (
			$type eq 'IndexedAnalysis' or 
			$type eq 'Alignment' or 
			$type eq 'Analysis' or 
			$type eq 'Wiggle'
		) {
			$file_species = $sb_species;
			$file_genome  = $sb_genome;
		}
		
		# manifest list
		push @manifest, join(',', 
			sprintf("\"%s\"", $filedata{$f}{clean}),
			$filedata{$f}{type},
			sprintf("\"%s\"", $file_species),
			sprintf("\"%s\"", $file_genome),
			sprintf("\"%s\"", $userfirst),
			sprintf("\"%s\"", $userlast),
			$filedata{$f}{size},
			sprintf("\"%s\"", $filedata{$f}{date}),
			$filedata{$f}{md5},
		);
		
		# zip list
		if ($filedata{$f}{zip} == 1) {
			push @ziplist, $filedata{$f}{clean};
		}
		
		# remove list
		if ($filedata{$f}{type} eq 'IndexedAnalysis') {
			# we are keeping the bigWig and USeq files around, for good or bad,
			# out of benefit to users who happen to use GNomEx as a track hub
			# only delete if specifying everything must go
			push @removelist, $filedata{$f}{clean} if $everything;
		}
		else {
			# we delete everything else
			push @removelist, $filedata{$f}{clean};
		}
	}	
	
	
	### Zip the files
	if (@ziplist and $zip) {
		
		## write zip list file
		my $fh = IO::File->new($ziplist_file, 'w') or 
			die "unable to write zip list file $ziplist_file: $!\n";
		foreach (@ziplist) {
			$fh->print("$_\n");
		}
		$fh->close;
		
		
		## Zip the files
		# we will zip zip with fastest compression for speed
		# use file sync option to add, update, and/or delete members in zip archive
		# regardless if it's present or new
		print "  > zipping files\n";
		my $command = sprintf("cat %s | $zipper -1 --symlinks -FS -@ %s", $ziplist_file, $zip_file);
		print "  > executing: $command\n";
		my $result = system($command);
		if ($result) {
			print "     failed!\n" ;
		}
		elsif (not $result and -e $zip_file) {
			# zip appears successful
			
			# add zip archive data to lists
			my ($date, $size) = get_file_stats($zip_file);
			my $md5 = calculate_checksum($zip_file);
			push @manifest, join(",", 
				$zip_file, 
				'Archive', 
				q(""), 
				q(""), 
				sprintf("\"%s\"", $userfirst),
				sprintf("\"%s\"", $userlast),
				$size,
				sprintf("\"%s\"", $date),
				$md5,
			);
			push @removelist, $zip_file;
			
			# move the the files
			print "  > moving zipped files\n";
			mkdir $zipped_folder;
			foreach my $file (@ziplist) {
				my (undef, $dir, $basefile) = File::Spec->splitpath($file);
				my $targetdir = File::Spec->catdir($zipped_folder, $dir);
				make_path($targetdir); 
					# this should safely skip existing directories
					# permissions and ownership inherit from user, not from source
					# return value is number of directories made, which in some cases could be 0!
				print "     moving $file\n" if $verbose;
				move($file, $targetdir) or print "     ! failed to move $file! $!\n";
			}
	
			# clean up empty directories
			my $command = sprintf("find %s -type d -empty -delete", $given_dir);
			print "  > executing: $command\n";
			print "    ! failed $!\n" if (system($command));
		
			# clean up broken symlinks
			# sometimes they refer to a zipped file, and thus breaks the sbg_uploader
			$command = sprintf("find %s -xtype l -delete", $given_dir);
			print "  > executing: $command\n";
			print "    ! failed $!\n" if (system($command));
		}
	}
	
	
	### Write files
	# manifest
	my $fh = IO::File->new($manifest_file, 'w') or 
		die "unable to write manifest file $manifest_file: $!\n";
	foreach (@manifest) {
		$fh->print("$_\n");
	}
	$fh->close;
	
	# remove list
	$fh = IO::File->new($alt_remove, 'w') or 
		die "unable to write manifest file $alt_remove: $!\n";
	foreach (@removelist) {
		$fh->print("$_\n");
	}
	$fh->close;
	
	return 1;
}


# find callback
sub callback {
	my $file = $_;
	print "  > find callback for $fname\n" if $verbose;

	# generate a clean name for recording
	my $clean_name = $fname;
	$clean_name =~ s|^\./||; # strip the beginning ./ from the name to clean it up
	
	
	### Ignore certain files
	if (-d $file) {
		# skip directories
		print "     directory, skipping\n" if $verbose;
		return;
	}
	elsif (-l $file) {
		print "     symbolic link, skipping\n" if $verbose;
		return;
	}
	elsif ($file eq $remove_file) {
		print "     skipping project metadata file\n" if $verbose;
		return;
	}
	elsif ($file eq $notice_file) {
		print "     skipping project metadata file\n" if $verbose;
		return;
	}
	elsif ($file eq $manifest_file) {
		print "     skipping project metadata file\n" if $verbose;
		return;
	}
	elsif ($file eq $ziplist_file) {
		print "     skipping project metadata file\n" if $verbose;
		return;
	}
	elsif ($file =~ /(?:libsnappyjava|fdt|fdtCommandLine)\.jar/) {
		# devil java spawn, delete!!!!
		print "   > deleting java file\n" if $verbose;
		unlink $file;
		return;
	}
	elsif ($file =~ /\.sra$/i) {
		# what the hell are SRA files doing in here!!!????
		print "   > deleting SRA file\n";
		unlink $file;
		return;
	}
	elsif ($file eq '.DS_Store' or $file eq 'Thumbs.db') {
		# Windows and Mac file browser devil spawn, delete these immediately
		print "   > deleting file browser metadata file\n" if $verbose;
		unlink $file;
		return;
	}
	
	### Stats on the file
	my ($date, $size) = get_file_stats($file);
	
	
	### Possible file types
	my $filetype;
	if ($file =~ /\.(?:bw|bigwig|bb|bigbed|useq)$/i) {
		# an indexed analysis file
		$filetype = 'IndexedAnalysis';
	}
	elsif ($file =~ /\.(?:bam|bai|cram|crai|csi|sam\.gz)$/i) {
		# an alignment file
		$filetype = 'Alignment';
	}
	elsif ($file =~ /\.vcf(?:\.gz)?$/i) {
		# possibly an indexed file, check to see if there is a corresponding tabix index
		my $i = $file . '.tbi';
		if (-e $i) {
			# index present
			# but skip genomic vcf files
			if ($file =~ /[_\.\-] g (?:enomic)? \.? vcf/xi) {
				# looks like a genomic vcf, skip these, just want final vcf files
				# this assumes people are naming their genomic vcfs like above
				$filetype = 'Analysis';
			}
			else {
				# regular vcf with index
				$filetype = 'IndexedAnalysis';
			}
		} 
		else {
			$filetype = 'Analysis';
		}
	}
	elsif ($file =~ /\.vcf\.idx$/i) {
		# a GATK-style index for non-tabix, uncompressed VCF files
		# This index isn't useful for browsing, so do not classify as IndexedAnalysis
		$filetype = 'Analysis';
	}
	elsif ($file =~ /\.tbi$/i) {
		# a tabix index file, presumably alongside a vcf file
		my $f = $file;
		$f =~ s/\.tbi$//i;
		if ($f =~ /\.vcf\.gz$/i and -e $f) {
			# we have an indexed vcf file
			if ($file =~ /[_\.\-] g (?:enomic)? \.? vcf/xi) {
				# looks like a genomic vcf, skip these, just want final vcf files
				# this assumes people are naming their genomic vcfs like above
				$filetype = 'Analysis';
			}
			else {
				# regular vcf with index
				$filetype = 'IndexedAnalysis';
			}
		} 
		else {
			$filetype = 'Analysis';
		}
	}
	elsif ($file =~ /\..*loupe$/i) {
		# 10X genomics loupe file
		$filetype = 'Analysis';
	}
	elsif ($file =~ /\.(?:fa|fq|fasta|fastq|fai)(?:\.gz)?$/i) {
		$filetype = 'Sequence';
	}
	elsif ($file =~ /\.(?:bed|bed\d+|gtf|gff|gff\d|narrowpeak|broadpeak|refflat|genepred|ucsc)(?:\.gz)?$/i) {
		$filetype = 'Annotation';
	}
	elsif ($file =~ /\.(?:txt|tsv|tab|csv|cdt|counts|results|cns|cnr|cnn|md|sam)(?:\.gz)?$/i) {
		# yes, uncompressed sam files get thrown in here as text files!
		$filetype = 'Text';
	}
	elsif ($file =~ /\.(?:wig|bg|bdg|bedgraph)(?:\.gz)?$/i) {
		$filetype = 'Wiggle';
	}
	elsif ($file =~ /\.(?:bar|bar\.zip|swi|egr|ser|mpileup|mpileup\.gz|motif)$/i) {
		$filetype = 'Analysis';
	}
	elsif ($file =~ /\.(?:xls|xlsx|ppt|pptx|doc|docx|pdf|ps|eps|png|jpg|jpeg|gif|tif|tiff|svg|ai|out|rout|rdata|xml|json|json\.gz|html|pzfx)$/i) {
		$filetype = 'Results';
	}
	elsif ($file =~ /\.(?:sh|pl|py|pyc|r|rmd|rscript|awk)$/i) {
		$filetype = 'Script';
	}
	elsif ($file =~ /\.(?:tar|tar\.gz|tar\.bz2|zip)$/i) {
		$filetype = 'Archive';
	}
	else {
		# catchall
		$filetype = 'Other';
	}
	
	# Compress individual files
	if (
		$gzip and 
		($filetype eq 'Text' or $filetype eq 'Annotation' or $filetype eq 'Wiggle' or $filetype eq 'Sequence') and 
		$file !~ /\.(?:gz|fai)$/i and 
		$size > $min_gz_size
	) {
		# We have a known, big, text file that is not compressed and needs to be
		my $command = sprintf "%s \"%s\"", $gzipper, $file;
		if (system($command)) {
			print "   ! gzip command '$command' failed!\n";
		}
		else {
			# succesfull compression! update values
			print "   > gzip compressed $file\n" if $verbose;
			$file .= '.gz';
			$clean_name .= '.gz';
			($date, $size) = get_file_stats($file);
		}
	}
	
	
	# Check files for Zip archive files
	$filedata{$fname}{zip} = 0; # default setting
	if ($zip and ($filetype ne 'IndexedAnalysis' or $filetype ne 'Alignment') ) {
		if ( ($filetype eq 'Text' or $filetype eq 'Annotation' or $filetype eq 'Sequence') and $file !~ /\.gz$/i) {
			# uncompressed text and annotation files always gets zipped
			# it's just the way it is!
			$filedata{$fname}{zip} = 1;
		}
		elsif ($size <= $max_zip_size) {
			# if it's small, it gets zipped, regardless of type
			$filedata{$fname}{zip} = 1;
		}
	}
	
	
	### Record the collected file information
	$filedata{$fname}{clean} = $clean_name;
	$filedata{$fname}{type}  = $filetype;
	$filedata{$fname}{md5}   = calculate_checksum($file) || q();
	$filedata{$fname}{date}  = $date;
	$filedata{$fname}{size}  = $size;
	
	print "     processed $filetype file\n" if $verbose;
}


sub get_file_stats {
	# stats on the file
	my $file = shift;
	my @st = stat($file);
	my $date = strftime("%B %d, %Y %H:%M:%S", localtime($st[9]));
	return ($date, $st[7]); # date, size
}


sub calculate_checksum {
	# calculate the md5 with external utility
	my $file = shift;
	my $checksum = qx(md5sum '$file'); # quote file, because we have files with nasty characters
	my ($md5, undef) = split(/\s+/, $checksum);
	return $md5;
}


sub upload_files {
	
	### Initialize SB wrapper
	my $sb = SB->new(
		div     => $sb_division,
		sb      => $sb_path,
		cred    => $cred_path,
	) or die "unable to initialize SB wrapper module!";
	$sb->verbose(1) if $verbose;
	
	
	### Create the project on Seven Bridges
	my $sbproject;
	
	# check whether it exists
	foreach my $p ($sb->projects) {
		if ($p->name eq $project) {
			# we found it
			$sbproject = $p;
			printf "   > using existing SB project %s\n", $p->id;
			last;
		}
	}
	
	# create the project if it doesn't exist
	if (not defined $sbproject) {
		# generate description in markdown as necessary
		if (not $description) {
			$description = sprintf "# %s\n## %s\n GNomEx project %s is an Analysis project for %s %s",
				$project, $title, $project, $userfirst, $userlast;
			if ($group) {
				$description .= " in the group '$group'. ";
			}
			else {
				$description .= ". ";
			}
			if ($sb_species and $sb_genome) {
				$description .= "Analysis files are for species $sb_species, genome build version $sb_genome. ";
			}
			$description .= sprintf("Details on the experiment may be found in [GNomEx](https://hci-bio-app.hci.utah.edu/gnomex/gnomexFlex.jsp?analysisNumber=%s).\n",
				$project);
		}
		
		# create project
		$sbproject = $sb->create_project(
			name        => $project,
			description => $description,
		);
		if ($sbproject) {
			printf "   > created SB project %s\n", $sbproject->id;
		}
		else {
			print "   ! failed to make SB project!\n";
			return;
		}
	}
	
	
	### Upload the files
	# upload options
	# The bulk uploader currently doesn't handle manifest files while preserving 
	# folders, and the sb command line tool doesn't handle folders either, so I'm 
	# basically screwed here. The best we can do is upload with folders. That's 
	# more important than the small amount of metadata that could be added anyway.
	my @up_options = ('--preserve-folders', '*');
	
	# upload command
	my $path = $sbproject->bulk_upload_path($sbupload_path);
	unless ($path) {
		print "   ! no sbg-upload.sh executable path!\n";
		return;
	};
	my $result = $sbproject->bulk_upload(@up_options);
	print $result;
	if ($result =~ /FAILED/) {
		print "   ! upload failed!\n";
	}
	elsif ($result =~ /Done\.\n$/) {
		print "   > upload successful\n";
	}
	else {
		print "   ! upload error!\n";
	}
	return 1;
}


sub hide_deleted_files {
	
	# move the deleted files
	mkdir $deleted_folder;
	
	unless (@removelist) {
		# load the delete file contents
		my $fh = IO::File->new($alt_remove, 'r') or die "can't read $alt_remove! $!\n";
		while (my $line = $fh->getline) {
			chomp($line);
			push @removelist, $line;
		}
		$fh->close;
	}
	
	# process the removelist
	foreach my $file (@removelist) {
		next unless (-e $file);
		my (undef, $dir, $basefile) = File::Spec->splitpath($file);
		my $targetdir = File::Spec->catdir($deleted_folder, $dir);
		make_path($targetdir); 
			# this should safely skip existing directories
			# permissions and ownership inherit from user, not from source
			# return value is number of directories made, which in some cases could be 0!
		print "   moving $file\n" if $verbose;
		move($file, $targetdir) or print "   failed to move! $!\n";
	}
	
	# clean up empty directories
	my $command = sprintf("find %s -type d -empty -delete", $given_dir);
	print "  > executing: $command\n";
	print "    failed! $!\n" if (system($command));
	
	# move the deleted folder back into archive
	move($alt_remove, $remove_file) or print "   failed to move $alt_remove! $!\n";
	
	# put in notice
	if (not -e $notice_file and $notice_source_file) {
		$command = sprintf("ln -s %s %s", $notice_source_file, $notice_file);
		print "   ! failed to link notice file! $!\n" if (system($command));
	}
}








