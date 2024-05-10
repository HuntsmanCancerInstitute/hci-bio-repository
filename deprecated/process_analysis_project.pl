#!/usr/bin/env perl

use warnings;
use strict;
use English qw(-no_match_vars);
use IO::File;
use File::Find;
use POSIX qw(strftime);
use Getopt::Long;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use RepoProject;
use RepoCatalog;


our $VERSION = 6.0;

# shortcut variable name to use in the find callback
use vars qw(*fname);
*fname = *File::Find::name;



######## Documentation
my $doc = <<END;

A script to process GNomEx Analysis project folders.

It will recursively scan and inventory files, classifying them based on
known file type extensions, and calculate basic metadata, including size in
bytes, date, and MD5 checksum. It will write a CSV manifest file containing
all of this metadata. It will also a write a list of all files targeted for
deletion at a later date in the parent directory.

Options to compress and/or archive small files are available. 
Known file types that exceed a minimum specified size may be gzip 
compressed automatically. All files, except certain known Analysis 
and compressed files, may be stored into a single Zip archive. 
Files that are zip archived are automatically moved into a hidden 
folder in the parent directory.

This requires collecting some metadata from the GNomEx LIMS database 
for inclusion in the metadata Manifest CSV file. If not directly provided,
a catalog database file may be provided to automatically retrieve 
the data given the project identifier.

Version: $VERSION

Example Usage:
    process_analysis_project.pl [options] /Repository/AnalysisData/2019/A5678
    
    process_analysis_project.pl --cat Analysis.db [options] A5678

Options:

 Metadata
    --cat <path>        Provide path to metadata catalog database
    --first <text>      User first name for the owner of the project
    --last <text>       User last name for the owner of the project
    --species "text"    Specify the species, such as Homo sapiens or 
                        Mus musculus. Also accepts: human, 
                        mouse, rat, yeast, zebrafish, worm.
    --genome <text>     Genome build version, such as hg38 or mm10. 
                        Recognized forms will be simplified by regex.

 Options
    --zip               Zip archive small files when scanning
    --all               Mark everything, including Analysis files, for deletion
    --keepzip           Do not hide files that have been zip archived
    --delzip            Immediately delete files that have been zip archived
    --verbose           Tell me everything!

END



######## Process command line options
my $path;
my $scan = 1;
my $zip;
my $cat_file;
my $userfirst           = q();
my $userlast            = q();
my $species             = q();
my $genome              = q();
my $description         = q();
my $max_zip_size        = 200000000; # 200 MB
my $keepzip             = 0;
my $deletezip           = 0;
my $everything;
my $verbose;

if (scalar(@ARGV) > 1) {
	GetOptions(
		'scan!'         => \$scan,
		'zip!'          => \$zip,
		'c|catalog=s'   => \$cat_file,
		'first=s'       => \$userfirst,
		'last=s'        => \$userlast,
		'species=s'     => \$species,
		'genome=s'      => \$genome,
		'desc=s'        => \$description,
		'keepzip!'      => \$keepzip,
		'delzip!'       => \$deletezip,
		'all!'          => \$everything,
		'verbose!'      => \$verbose,
	) or die "please recheck your options!\n\n";
}
else {
	print $doc;
	exit;
}
$path = shift @ARGV;





######## Check options

# grab all parameters from the catalog database if provided
if ($cat_file) {
	
	# first check path
	if ($cat_file !~ m|^/|) {
		# catalog file path is not from root
		$cat_file = File::Spec->catfile( File::Spec->rel2abs(), $cat_file);
	}
	
	# find entry in catalog and collect information
	my $Catalog = RepoCatalog->new($cat_file) or 
		die "Cannot open catalog file '$cat_file'!\n";
	if ($path =~ /(A\d{4,5})/) {
		my $id = $1;
		my $Entry = $Catalog->entry($id) or 
			die "No Catalog entry for $id\n";
		# collect metadata
		if (not $userfirst) {
			$userfirst = $Entry->user_first;
		}
		if (not $userlast) {
			$userlast = $Entry->user_last;
		}
		if (not $species) {
			$species = $Entry->organism;
		}
		if (not $genome) {
			$genome = $Entry->genome;
		}
		$path = $Entry->path;
	}
	else {
		die "unrecognized project identifier '$path'!\n";
	}
}

if ($keepzip and $deletezip) {
	die "must choose one: --keepzip or --delzip   Not both!!!\n";
}



######## Global variables
# these are needed since the File::Find callback doesn't accept pass through variables
my $start_time = time;
my @removelist;
my @ziplist;
my %filedata;
my $failure_count = 0;
my $post_zip_size = 0;
my @ten_x_crap;

# external commands
my ($gzipper, $bgzipper, $zipper);
{
	# preferentially use threaded gzip compression
	$gzipper = `which pigz`;
	chomp $gzipper;
	if ($gzipper) {
		$gzipper .= ' -p 4'; # run with four cores
	}
	else {
		# default to ordinary gzip
		$gzipper = 'gzip';
	}
	
	# bgzip is desirable when we auto compress certain files
	$bgzipper = `which bgzip`;
	chomp $bgzipper;
	if ($bgzipper) {
		$bgzipper .= ' -@ 4'; # run with four cores
	}
	else {
		# default to using standard gzip compression
		$bgzipper = $gzipper;
	}
	
	# An extensive internet search reveals no parallelized zip archiver, despite
	# there being a parallelized version of gzip, when both use the common DEFLATE 
	# algorithm. So we just run plain old zip.
	$zipper = `which zip`;
	chomp $zipper;
	unless ($zipper) {
		print " ! no zip compression utility, disabling\n";
		$zip = 0;
	}
}


### Species
# the SB metadata expects simple values, so define this by matching with regex
my $sb_species;
if ($species =~ /human | sapiens/xi) {
	$sb_species = 'Homo sapiens';
}
elsif ($species =~ /mouse | musculus/xi) {
	$sb_species = 'Mus musculus';
}
elsif ($species =~ /zebrafish/i) {
	$sb_species = 'Danio rerio';
}
elsif ($species =~ /rat/i) {
	$sb_species = 'Rattus norvegicus';
}
elsif ($species =~ /sheep/i) {
	$sb_species = 'Ovis aries';
}
elsif ($species =~ /rabbit/i) {
	$sb_species = 'Oryctolagus cuniculus';
}
elsif ($species =~ /pig/i) {
	$sb_species = 'Sus scrofa';
}
elsif ($species =~ /fly | melanogaster/xi) {
	$sb_species = 'Drosophila melanogaster';
}
elsif ($species =~ /yeast | cerevisiae/xi) {
	$sb_species = 'Saccharomyces cerevisiae';
}
elsif ($species =~ /worm | elegans/xi) {
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
# sheep
elsif ($genome =~ /(ov\d\+)/i) {
	$sb_genome = $1;
}
# rat
elsif ($genome =~ /(rn\d\+)/i) {
	$sb_genome = $1;
}
# what else?
else {
	$sb_genome = $genome;
}





####### Initiate Project

my $Project = RepoProject->new($path, $verbose) or 
	die "unable to initiate Repository Project!\n";

# check project ID
if ($Project->id =~ m/ \d{3,5}R \/? $/x) {
	# looks like a Request project
	die "given path is a Request project! Stopping!\n";
}

printf " > working on %s at %s\n", $Project->id, $Project->given_dir;
printf "   using parent directory %s\n", $Project->parent_dir if $verbose;

# application paths
if ($verbose) {
	print " =>    gzip compression: $gzipper\n";
	print " =>   bgzip compression: $bgzipper\n";
	print " =>         zip utility: $zipper\n";
}

# file paths
if ($verbose) {
	printf " =>  manifest file: %s\n", $Project->manifest_file;
	printf " =>    remove file: %s or %s\n", $Project->remove_file, $Project->alt_remove_file;
	printf " =>  zip list file: %s or %s\n", $Project->ziplist_file;
	printf " =>       zip file: %s or %s\n", $Project->zip_file, $Project->alt_zip_file;
	printf " => deleted folder: %s\n", $Project->delete_folder;
	printf " =>  zipped folder: %s\n", $Project->zip_folder;
}


# zipped file hidden folder
if (-e $Project->zip_folder) {
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
if (-e $Project->delete_folder) {
	if ($scan) {
		print "! cannot re-scan if deleted files hidden folder exists!\n";
		$scan = 0;
	}
}






######## Main functions

# change to the given directory
printf " > changing to %s\n", $Project->given_dir if $verbose;
chdir $Project->given_dir or 
	die sprintf("cannot change to %s!\n", $Project->given_dir);


# scan the directory
if ($scan) {
	# this will also run the zip function
	printf " > scanning project %s in directory %s\n", $Project->id, 
		$Project->parent_dir;
	scan_directory();
	
	# update scan time stamp
	if ($cat_file and -e $cat_file and not $failure_count) {
		my $Catalog = RepoCatalog->new($cat_file);
		if ( $Catalog ) {
			my $Entry = $Catalog->entry($Project->id) ;
			$Entry->scan_datestamp(time);
			print " > updated Catalog scan date stamp\n";
		}
	}
}


######## Finished
if ($failure_count) {
	printf " ! finished with %s with %d failures in %.1f minutes\n\n", $Project->id,
		$failure_count, (time - $start_time)/60;
	
}
else {
	printf " > finished with %s in %.1f minutes\n\n", $Project->id, 
		(time - $start_time)/60;
}

exit;







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
		return 0;
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
		my ($file_species, $file_genome) = (q(),q());
		my $type = $filedata{$f}{type};
		if (
			$type eq 'IndexedAnalysis' or 
			$type eq 'IndexedVariant' or
			$type eq 'Alignment' or 
			$type eq 'Analysis' or 
			$type eq 'Annotation' or
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
		
		# zip and delete lists
		if ($filedata{$f}{zip} == 1) {
			# add to zip list but not delete list, since it'll be removed anyway
			push @ziplist, $filedata{$f}{clean};
		}
		else {
			# remove list
			if ($filedata{$f}{type} =~ /Indexed/) {
				# we are keeping the bigWig and tabixed files around, for good or bad,
				# out of benefit to users who happen to use GNomEx as a track hub
				# only delete if specifying everything must go
				push @removelist, $filedata{$f}{clean} if $everything;
			}
			else {
				# we delete everything else
				push @removelist, $filedata{$f}{clean};
			}
		}
	}	
	
	### 10X temporary files
	if (@ten_x_crap) {
		# we have 10X crap files that need to be targeted for deletion
		# have to do it now while scanning
		# I don't have a specific function to do this, so use a low level function to 
		# manually do this
		# this is basically undoable and permanent
		print "  ! Deleting 10X Genomics temporary analysis files\n";
		$failure_count += $Project->_delete_directory_files('./', \@ten_x_crap);
		
		# clean up empty directories
		$failure_count += $Project->clean_empty_directories('./');
	}
	
	
	### Zip archive the files
	if ($zip and scalar @ziplist) {
		
		## write zip list file
		my $fh = IO::File->new($Project->ziplist_file, 'w') or 
			die sprintf("unable to write zip list file %s: $OS_ERROR\n", $Project->ziplist_file);
		foreach (@ziplist) {
			$fh->print("$_\n");
		}
		$fh->close;
		
		
		## Zip the files
		# we will zip zip with fast compression for speed
		# use file sync option to add, update, and/or delete members in zip archive
		# regardless if it's present or new
		print "  > zipping files\n";
		my $command = sprintf("cat %s | $zipper -FS -@ %s", $Project->ziplist_file, 
			$Project->zip_file);
		print "  > executing: $command\n";
		my $result = system($command);
		if ($result) {
			print "     failed!\n";
			$failure_count++;
		}
		elsif (not $result and -e $Project->zip_file) {
			# zip appears successful
			
			# add zip archive data to lists
			my ($date, $size) = get_file_stats($Project->zip_file);
			my $md5 = $Project->calculate_file_checksum($Project->zip_file);
			push @manifest, join(",", 
				$Project->zip_file, 
				'Archive', 
				q(""), 
				q(""), 
				sprintf("\"%s\"", $userfirst),
				sprintf("\"%s\"", $userlast),
				$size,
				sprintf("\"%s\"", $date),
				$md5,
			);
			push @removelist, $Project->zip_file;
			
			# process the remaining files that were zipped
			if ($deletezip) {
				print "  > deleting zipped files\n";
				$failure_count += $Project->delete_zipped_files;
			}
			elsif ($keepzip == 0) {
				print "  > moving zipped files\n";
				$failure_count += $Project->hide_zipped_files;
			}
		}
	}
	
	
	### Write files
	# manifest
	my $fh = IO::File->new($Project->manifest_file, 'w') or 
		die sprintf("unable to write manifest file %s: $OS_ERROR\n", $Project->manifest_file);
	foreach (@manifest) {
		$fh->print("$_\n");
	}
	$fh->close;
	
	# remove list
	$fh = IO::File->new($Project->alt_remove_file, 'w') or 
		die sprintf("unable to write remove file %s: $OS_ERROR\n", $Project->alt_remove_file);
	foreach (@removelist) {
		$fh->print("$_\n");
	}
	$fh->close;
	
	
	### Finished
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
	if (-l $file) {
		# the sbg uploader can't handle symbolic links safely, so just delete them
		print "   ! deleting symbolic link: $clean_name\n";
		unlink $file;
		return;
	}
	elsif (-d $file) {
		# skip directories
		print "     directory, skipping\n" if $verbose;
		return;
	}
	elsif ($file eq $Project->remove_file) {
		print "     skipping project metadata file\n" if $verbose;
		return;
	}
	elsif ($file eq $Project->notice_file) {
		print "     skipping project metadata file\n" if $verbose;
		return;
	}
	elsif ($file eq $Project->manifest_file) {
		print "     skipping project metadata file\n" if $verbose;
		return;
	}
	elsif ($file eq $Project->ziplist_file) {
		print "     skipping project metadata file\n" if $verbose;
		return;
	}
	elsif ($file =~ /libsnappyjava \.so $/xi) {
		# devil java spawn, delete!!!!
		print "   ! deleting java file $clean_name\n";
		unlink $file;
		return;
	}
	elsif ($file =~ /(?: fdt | fdtCommandLine ) \. jar/x) {
		# fdt files, don't need
		print "   ! deleting java file $clean_name\n";
		unlink $file;
		return;
	}
	elsif ($file =~ /\.sra$/i) {
		# what the hell are SRA files doing in here!!!????
		print "   ! deleting SRA file $clean_name\n";
		unlink $file;
		return;
	}
	elsif ($fname =~ /.+ \/ fork\d \/ .+/x) {
		# these are left over 10X Genomics temporary processing files
		# they are not needed and do not need to be saved
		# add to custom remove list
		print "     marking 10X Genomics temporary file for deletion\n" if $verbose;
		push @ten_x_crap, $clean_name;
		return;
	}
	elsif ($file eq '.DS_Store' or $file eq 'Thumbs.db') {
		# Windows and Mac file browser devil spawn, delete these immediately
		print "   ! deleting file browser metadata file\n" if $verbose;
		unlink $file;
		return;
	}
	elsif ( $file =~ /~$/ ) {
		# files ending in ~ are typically backup copies of an edited text file
		# these can be safely deleted
		print "   ! deleting backup file $clean_name\n";
		unlink $file;
		return;
	}
	
	### Stats on the file
	my ($date, $size) = get_file_stats($file);
	
	
	### Possible file types
	# assign general file category type based on the extension
	# this should catch most files, but there's always weirdos and miscellaneous files
	# this will also dictate zip file status
	
	my $filetype;
	if ($file =~ /\. (?: bw | bigwig | bb | bigbed | hic) $/xi) {
		# an indexed analysis file
		$filetype = 'IndexedAnalysis';
		$filedata{$fname}{zip} = 0;
	}
	elsif ($file =~ /\. (?: bam | bai | cram | crai | csi | sam\.gz ) $/xi) {
		# an alignment file
		$filetype = 'Alignment';
		$filedata{$fname}{zip} = 0;
	}
	elsif ($file =~ /\.sam$/i) {
		# an uncompressed alignment - why do these exist??
		$filetype = 'Alignment';
		if ($size > 1000000) {
			# file bigger than 1 MB, let's compress it separately
			my $command = sprintf "%s \"%s\"", $gzipper, $file;
			if (system($command)) {
				print "   ! failed to automatically compress '$fname': $OS_ERROR\n";
				$filedata{$fname}{zip} = 1; 
			}
			else {
				# succesfull compression! update values
				print "   > automatically gzip compressed $file\n";
				$file  .= '.gz';
				$fname .= '.gz';
				$clean_name .= '.gz';
				$filedata{$fname}{zip} = 0;
				($date, $size) = get_file_stats($file);
			}
		}
		else {
			# otherwise we'll leave it for inclusion in the zip archive
			$filedata{$fname}{zip} = 1;
		}
	}
	elsif ($file =~ /\.vcf.gz$/i) {
		# indexed variant file, check to see if there is a corresponding tabix index
		my $i = $file . '.tbi';
		if (-e $i) {
			# index present
			# 
			# but skip genomic vcf files
			if ($file =~ /[_\.\-] g (?:enomic)? \.? vcf/xi) {
				# looks like a genomic vcf, skip these, just want final vcf files
				# this assumes people are naming their genomic vcfs like above
				$filetype = 'Variant';
				$filedata{$fname}{zip} = 0;
			}
			else {
				# regular vcf with index
				$filetype = 'IndexedVariant';
				$filedata{$fname}{zip} = 0;
			}
		} 
		else {
			$filetype = 'Variant';
			$filedata{$fname}{zip} = 0;
		}
	}
	elsif ($file =~ /\.(?: vcf | maf ) $/xi) {
		# uncompressed variant file, either VCF or MAF
		if ($size > 1000000) {
			# file bigger than 1 MB, let's compress it separately
			my $command = sprintf "%s \"%s\"", $bgzipper, $file;
			if (system($command)) {
				print "   ! failed to automatically compress '$fname': $OS_ERROR\n";
				$filetype = 'Variant';
				$filedata{$fname}{zip} = 1; # we'll store it archive
			}
			else {
				# succesfull compression! update values
				print "   > automatically compressed $file\n";
				$file  .= '.gz';
				$fname .= '.gz';
				$clean_name .= '.gz';
				$filetype = 'Variant';
				$filedata{$fname}{zip} = 0;
				($date, $size) = get_file_stats($file);
			}
		}
		else {
			# otherwise we'll leave it for inclusion in the zip archive
			$filetype = 'Variant';
			$filedata{$fname}{zip} = 1;
		}
	}
	elsif ($file =~ / \.vcf \.idx $/xi) {
		# a GATK-style index for non-tabix, uncompressed VCF files
		# This index isn't useful for browsing or for anything other than GATK
		# and it will auto-recreate anyway, so toss
		unlink $file;
		return;
	}
	elsif ($file =~ /\.tbi$/i) {
		# a tabix index file, presumably alongside a vcf file
		# but possible another file type
		my $f = $file;
		$f =~ s/\.tbi$//i;
		if ($f =~ /\.vcf\.gz$/i and -e $f) {
			# we have an indexed vcf file
			if ($file =~ /[_\.\-] g (?:enomic)? \.? vcf/xi) {
				# looks like a genomic vcf, skip these, just want final vcf files
				# this assumes people are naming their genomic vcfs like above
				$filetype = 'Variant';
				$filedata{$fname}{zip} = 0;
			}
			else {
				# regular vcf with index
				$filetype = 'IndexedVariant';
				$filedata{$fname}{zip} = 0;
			}
		} 
		elsif ($f =~ /\. (?: bed | bed\d+ | gtf | gff | gff\d | narrowpeak | broadpeak | gappedpeak | refflat | genepred | ucsc ) \.gz $/xi) {
			$filetype = 'Annotation';
			$filedata{$fname}{zip} = 0; # don't archive if indexed
		}
		else {
			$filetype = 'Analysis';
			$filedata{$fname}{zip} = 1;
		}
	}
	elsif ($file =~ /\. \w* loupe$/xi) {
		# 10X genomics loupe file
		$filetype = 'Analysis';
		$filedata{$fname}{zip} = 0;
	}
	elsif ($file =~ /\. (?: fq | fastq ) (?: \.gz)? $/xi) {
		# fastq file
		if ($file =~ /^\d+X\d+_/) {
			print "   ! Possible HCI Fastq file detected! $clean_name\n";
		}
		$filetype = 'Sequence';
		if ($file !~ /\.gz$/i) {
			# file not compressed!!!????? let's compress it separately
			my $command = sprintf "%s \"%s\"", $gzipper, $file;
			if (system($command)) {
				print "   ! failed to automatically compress '$fname': $OS_ERROR\n";
				$filedata{$fname}{zip} = 1; 
			}
			else {
				# succesfull compression! update values
				print "   > automatically gzip compressed $fname\n";
				$file  .= '.gz';
				$fname .= '.gz';
				$clean_name .= '.gz';
				($date, $size) = get_file_stats($file);
			}
		}
		
		# check the size
		if ($size > 100000) {
			# Bigger than 1 KB, leave it out
			$filedata{$fname}{zip} = 0;
		}
		else {
			# otherwise we'll include it in the zip archive
			$filedata{$fname}{zip} = 1;
		}
	}
	elsif ($file =~ /^Unmapped \.out \.mate [12] .*$/xi) {
		# unmapped fastq from STAR - seriously, does anyone clean up their droppings?
		$filetype = 'Sequence';
		if ($file !~ /\.gz$/i) {
			# file not compressed!!!????? let's compress it
			my $command = sprintf "%s \"%s\"", $gzipper, $file;
			if (system($command)) {
				print "   ! failed to automatically compress '$fname': $OS_ERROR\n";
				$filedata{$fname}{zip} = 1; 
			}
			else {
				# succesfull compression! update values
				print "   > automatically gzip compressed $fname\n";
				$file  .= '.gz';
				$fname .= '.gz';
				$clean_name .= '.gz';
				($date, $size) = get_file_stats($file);
			}
		}
		
		# check the size
		if ($size > 100000) {
			# Bigger than 1 KB, leave it out
			$filedata{$fname}{zip} = 0;
		}
		else {
			# otherwise we'll include it in the zip archive
			$filedata{$fname}{zip} = 1;
		}
	}
	elsif ($file =~ /\. (?: fa | fasta | fai | ffn | dict ) (?: \.gz )? $/xi) {
		# sequence file of some sort
		$filetype = 'Sequence';
		$filedata{$fname}{zip} = 1;
	}
	elsif ($file =~ /\. (?: bed | bed\d+ | gtf | gff | gff\d | narrowpeak | broadpeak | gappedpeak | refflat | genepred | ucsc) (?:\.gz)? $/xi) {
		$filetype = 'Annotation';
		my $i = $file . '.tbi';
		if ($file =~ /\.gz$/ and -e $i) {
			# it is tabix indexed, do not archive
			$filedata{$fname}{zip} = 0;
		}
		else {
			$filedata{$fname}{zip} = 1;
		}
	}
	elsif ($file =~ /\. (?: sh | pl | py | pyc | r | rmd | rscript | awk | sm | sing ) $/xi) {
		$filetype = 'Script';
		$filedata{$fname}{zip} = 1;
	}
	elsif ($file eq 'cmd.txt') {
		# pysano command script
		$filetype = 'Script';
		$filedata{$fname}{zip} = 1;
	}
	elsif ($file =~ /\. (?: txt | tsv | tab | csv | cdt | counts | results | cns | cnr | cnn | md | log | biotypes ) (?:\.gz)? $/xi) {
		# general analysis text files, may be compressed
		$filetype = 'Text';
		my $i = $file . '.tbi';
		if ($file =~ /\.gz$/ and -e $i) {
			# it is tabix indexed, do not archive
			$filedata{$fname}{zip} = 0;
		}
		else {
			$filedata{$fname}{zip} = 1;
		}
	}
	elsif ($file =~ /\. (?: wig | bg | bdg | bedgraph ) (?:\.gz)? $/xi) {
		$filetype = 'Analysis';
		$filedata{$fname}{zip} = 1;
	}
	elsif ($file =~ /\. mpileup.* \.gz $/xi) {
		# compressed mpileup files ok?, some people stick in text between mpileup and gz
		$filetype = 'Analysis';
		$filedata{$fname}{zip} = 0;
	}
	elsif ($file =~ /\. (?: bar | bar\.zip | useq | swi | swi\.gz | egr | ser | mpileup | motif | cov ) $/xi) {
		$filetype = 'Analysis';
		$filedata{$fname}{zip} = 1;
	}
	elsif ($file =~ /\. (?: xls | ppt | pptx | doc | docx | pdf | ps | eps | png | jpg | jpeg | gif | tif | tiff | svg | ai | out | rout | rda | rdata | rds | rproj | xml | json | json\.gz | html | pzfx | err | mtx | mtx\.gz ) $/xi) {
		$filetype = 'Results';
		$filedata{$fname}{zip} = 1;
	}
	elsif ($file =~ /\. (?: xlsx | h5 | hd5 | hdf5 ) $/xi) {
		# leave out certain result files from zip archive just to be nice
		$filetype = 'Results';
		$filedata{$fname}{zip} = 0;
	}
	elsif ($file =~ /\. (?: tar |tar\.gz | tar\.bz2 | zip ) $/xi) {
		$filetype = 'Archive';
		if ($file =~ /fastqc \.zip $/x) {
			# no need keeping fastqc zip files separate
			$filedata{$fname}{zip} = 1; 
		}
		else {
			# something else
			$filedata{$fname}{zip} = 0; 
		}
	}
	elsif ($file =~ /\. (?: bt2 | amb | ann | bwt | pac | nix | novoindex | index ) $/x) {
		$filetype = 'AlignmentIndex';
		$filedata{$fname}{zip} = 1; # zip I guess?
	}
	else {
		# catchall
		$filetype = 'Other';
		if ($size > $max_zip_size) {
			# it's pretty big
			printf "   ! Large unknown file $clean_name at %.1fG\n", $size / 1000000000;
		}
		$filedata{$fname}{zip} = 1;
	}
	
	
	# Check files for Zip archive files
	if (not $zip) {
		# we're not zipping anything so turn it off
		$filedata{$fname}{zip} = 0; 
	}
	
	
	### Record the collected file information
	$filedata{$fname}{clean} = $clean_name;
	$filedata{$fname}{type}  = $filetype;
	$filedata{$fname}{md5}   = $Project->calculate_file_checksum($file);
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





__END__

=head1 AUTHOR

 Timothy J. Parnell, PhD
 Cancer Bioinformatics Shared Resource
 Huntsman Cancer Institute
 University of Utah
 Salt Lake City, UT, 84112

This package is free software; you can redistribute it and/or modify
it under the terms of the Artistic License 2.0.  


