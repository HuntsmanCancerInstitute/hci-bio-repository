#!/usr/bin/perl


use strict;
use IO::File;
use File::Find;
use POSIX qw(strftime);
use Getopt::Long;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use SB2;
use RepoProject;
use RepoCatalog;
use Emailer;


my $version = 5.1;

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
Known file types that exceed a minimum specified size may be gzip 
compressed automatically. All files, except certain known Analysis 
and compressed files, may be stored into a single Zip archive. 
Files that are zip archived are automatically moved into a hidden 
folder in the parent directory.

This requires collecting data from the GNomEx LIMS database and 
passing certain values on to this script for inclusion as metadata 
in the metadata Manifest CSV file. However, the current bulk 
uploader does not simultaneously handle CSV metadata and recursive 
paths. Therefore metadata is not set upon uploading to Seven Bridges.

A catalog database file may be provided to automatically retrieve 
metadata given a project identifier.

A Seven Bridges Project is automatically generated when uploading,
using the project identifier as the name. A Markdown description is
generated for the Project using the GNomEx metadata, including user
name, title, group name, and genome version. 

Version: $version

Example Usage:
    process_analysis_project.pl [options] /Repository/AnalysisData/2019/A5678
    
    process_analysis_project.pl --cat Analysis.db [options] A5678

Options:

 Main functions - not exclusive
    --scan              Scan the project folder and generate the manifest
                          also zip archives and/or compresses files
    --upload            Run the sbg-uploader program to upload
    --hide              Hide the deleted files in hidden folder

 Metadata
    --cat <path>        Provide path to metadata catalog database
    --first <text>      User first name for the owner of the project
    --last <text>       User last name for the owner of the project
    --email <text>      User email address for notifications
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
    --zip               Zip archive small files when scanning
    --all               Mark everything, including Analysis files, for deletion
    --desc "text"       Description for new SB project when uploading. 
                        Can be Markdown text.
    --keepzip           Do not hide files that have been zip archived
    --delzip            Immediately delete files that have been zip archived
    --notify            Send email to user and PI when uploading
    --verbose           Tell me everything!
 
 Seven Bridges
    --division <text>   The Seven Bridges division name. Should be name of 
                        section in the credentials file. Default 'default'.

 Paths
    --sbup <path>       Path to the Seven Bridges Java uploader start script,
                        sbg-uploader.sh
    --cred <path>       Path to the Seven Bridges credentials file. 
                        Default is ~/.sevenbridges/credentials. Each profile 
                        should be named after the SB division.

END



######## Process command line options
my $path;
my $scan;
my $zip;
my $hide_files;
my $upload;
my $cat_file;
my $userfirst           = q();
my $userlast            = q();
my $email_address       = q();
my $title               = q();
my $group               = q();
my $species             = q();
my $genome              = q();
my $description         = q();
my $max_zip_size        = 200000000; # 200 MB
my $keepzip             = 0;
my $deletezip           = 0;
my $sb_division         = q();
my $sb_path             = q();
my $sbupload_path       = q();
my $cred_path           = q();
my $send_email;
my $everything;
my $verbose;

if (scalar(@ARGV) > 1) {
	GetOptions(
		'scan!'         => \$scan,
		'zip!'          => \$zip,
		'hide!'         => \$hide_files,
		'upload!'       => \$upload,
		'catalog=s'     => \$cat_file,
		'first=s'       => \$userfirst,
		'last=s'        => \$userlast,
		'email=s'       => \$email_address,
		'title=s'       => \$title,
		'group=s'       => \$group,
		'species=s'     => \$species,
		'genome=s'      => \$genome,
		'desc=s'        => \$description,
		'keepzip!'      => \$keepzip,
		'delzip!'       => \$deletezip,
		'notify!'       => \$send_email,
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
		if (not $email_address) {
			$email_address = $Entry->user_email;
		}
		if (not $title) {
			$title = $Entry->name;
		}
		if (not $group) {
			$group = $Entry->group;
		}
		if (not $species) {
			$species = $Entry->organism;
		}
		if (not $genome) {
			$genome = $Entry->genome;
		}
		if (not $sb_division) {
			$sb_division = $Entry->division;
		}
		$path = $Entry->path;
	}
	else {
		die "unrecognized project identifier '$path'!\n";
	}
}

if ($scan) {
	die "must provide user first name or Catalog db file to scan!\n" unless $userfirst;
	die "must provide user last name or Catalog db file to scan!\n" unless $userlast;
}
if ($upload) {
	die "must provide a SB division name or Catalog db file!\n" unless $sb_division;
	die "must provide a title for SB project or Catalog db file!\n" unless $title;
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
my $Digest;
my $failure_count = 0;
my $post_zip_size = 0;

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
	$bgzipper = `which bgzip`;
	
	# bgzip is desirable when we auto compress certain files
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
elsif ($species =~ /sheep/i) {
	$sb_species = 'Ovis aries';
}
elsif ($species =~ /rabbit/i) {
	$sb_species = 'Oryctolagus cuniculus';
}
elsif ($species =~ /pig/i) {
	$sb_species = 'Sus scrofa';
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
if ($Project->id =~ m/\d{3,5}R\/?$/) {
	# looks like a Request project
	die "given path is a Request project! Stopping!\n";
}

printf " > working on %s at %s\n", $Project->id, $Project->given_dir;
printf "   using parent directory %s\n", $Project->parent_dir if $verbose;

# application paths
if ($verbose) {
	print " =>             SB path: $sb_path\n" if $sb_path;
	print " =>    SB uploader path: $sbupload_path\n" if $sbupload_path;
	print " => SB credentials path: $cred_path\n" if $cred_path;
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
	if ($hide_files) {
		print " ! deleted files hidden folder already exists! Will not move deleted files\n";
		$hide_files = 0; # do not want to move zipped files
	} 
	if ($scan) {
		print "! cannot re-scan if deleted files hidden folder exists!\n";
		$scan = 0;
	}
	if ($upload) {
		print "! cannot upload if deleted files hidden folder exists!\n";
		$upload = 0;
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
}


# upload files to Seven Bridges
if ($upload and not $failure_count) {
	if (-e $Project->manifest_file) {
		printf " > uploading project %s files to $sb_division\n", $Project->id;
		upload_files();
		
		# re-calculate the size here for email notification
		# must do it before the directory is cleaned up below
		($post_zip_size, undef) = $Project->get_size_age;
	}
	else {
		print " ! No manifest file! Cannot upload files\n";
		$failure_count++;
	}
}


# hide files
if ($hide_files and not $failure_count) {
	if (-e $Project->alt_remove_file) {
		printf " > moving project %s files to %s\n", $Project->id, 
			$Project->delete_folder;
		$failure_count += $Project->hide_deleted_files;
	}
	else {
		print " ! No deleted files to hide\n";
		$failure_count++;
	}
}



######## Finished
if ($failure_count) {
	printf " ! finished with %s with %d failures in %.1f minutes\n\n", $Project->id,
		$failure_count, (time - $start_time)/60;
	
}
else {
	if ($cat_file) {
		my $Catalog = RepoCatalog->new($cat_file) if -e $cat_file;
		my $Entry = $Catalog->entry($Project->id) if $Catalog;
		if ($Entry) {
			my $t = time;
			if ($scan) {
				$Entry->scan_datestamp($t);
			}
			if ($upload) {
				$Entry->upload_datestamp($t);
				
				# send an upload notification email
				if ($send_email) {
					# use the pre-cleaned but post-zipped file size
					unless ($post_zip_size) {
						# this should not be null, but just in case
						($post_zip_size, undef) = $Project->get_size_age;
					}
					# initialize
					my $Email = Emailer->new();
					if ($Email) {
						my $result = $Email->send_analysis_upload_email(
							$Entry,
							'size' => $post_zip_size
							# force using the post-zip, pre-cleanup size
						);
						if ($result) {
							printf " > Sent Analysis SB upload notification email: %s\n", 
								$result->message;
							$Entry->emailed_datestamp($t);
						}
						else {
							print " ! Failed to send Analysis upload notification email!\n";
						}
					}
					else {
						print " ! Failed to initialize Emailer! unable to send!\n";
					}
				}
			}
			if ($hide_files) {
				$Entry->hidden_datestamp($t);
			}
			print " > updated catalog entry\n";
		}
		else {
			print " ! failed to update catalog entry!\n";
		}
	}
	printf " > finished with %s in %.1f minutes\n\n", $Project->id, 
		(time - $start_time)/60;
}









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
		my ($file_species, $file_genome) = ('','');
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
	
	
	### Zip archive the files
	if ($zip and scalar @ziplist) {
		
		## write zip list file
		my $fh = IO::File->new($Project->ziplist_file, 'w') or 
			die sprintf("unable to write zip list file %s: $!\n", $Project->ziplist_file);
		foreach (@ziplist) {
			$fh->print("$_\n");
		}
		$fh->close;
		
		
		## Zip the files
		# we will zip zip with fast compression for speed
		# use file sync option to add, update, and/or delete members in zip archive
		# regardless if it's present or new
		print "  > zipping files\n";
		my $command = sprintf("cat %s | $zipper -3 -FS -@ %s", $Project->ziplist_file, 
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
		die sprintf("unable to write manifest file %s: $!\n", $Project->manifest_file);
	foreach (@manifest) {
		$fh->print("$_\n");
	}
	$fh->close;
	
	# remove list
	$fh = IO::File->new($Project->alt_remove_file, 'w') or 
		die sprintf("unable to write manifest file : $!\n", $Project->alt_remove_file);
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
	elsif ($file =~ /(?:libsnappyjava)\.so$/i) {
		# devil java spawn, delete!!!!
		print "   ! deleting java file $clean_name\n";
		unlink $file;
		return;
	}
	elsif ($file =~ /(?:fdt|fdtCommandLine)\.jar/) {
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
	elsif ($file eq '.DS_Store' or $file eq 'Thumbs.db') {
		# Windows and Mac file browser devil spawn, delete these immediately
		print "   ! deleting file browser metadata file\n" if $verbose;
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
	if ($file =~ /\.(?:bw|bigwig|bb|bigbed)$/i) {
		# an indexed analysis file
		$filetype = 'IndexedAnalysis';
		$filedata{$fname}{zip} = 0;
	}
	elsif ($file =~ /\.(?:bam|bai|cram|crai|csi|sam\.gz)$/i) {
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
				print "   ! failed to automatically compress '$fname': $!\n";
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
	elsif ($file =~ /\.vcf$/i) {
		# uncompressed variant file
		if ($size > 1000000) {
			# file bigger than 1 MB, let's compress it separately
			my $command = sprintf "%s \"%s\"", $bgzipper, $file;
			if (system($command)) {
				print "   ! failed to automatically compress '$fname': $!\n";
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
	elsif ($file =~ /\.vcf\.idx$/i) {
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
		elsif ($f =~ /\.(?:bed|bed\d+|gtf|gff|gff\d|narrowpeak|broadpeak|gappedpeak|refflat|genepred|ucsc)\.gz$/i) {
			$filetype = 'Annotation';
			$filedata{$fname}{zip} = 0; # don't archive if indexed
		}
		else {
			$filetype = 'Analysis';
			$filedata{$fname}{zip} = 1;
		}
	}
	elsif ($file =~ /\.\w*loupe$/i) {
		# 10X genomics loupe file
		$filetype = 'Analysis';
		$filedata{$fname}{zip} = 0;
	}
	elsif ($file =~ /\.(?:fq|fastq)(?:\.gz)?$/i) {
		# fastq file
		if ($file =~ /^\d+X\d+_/) {
			print "   ! Possible HCI Fastq file detected! $clean_name\n";
		}
		$filetype = 'Sequence';
		if ($file !~ /\.gz$/i and $size > 1000000) {
			# file bigger than 1 MB, let's compress it separately
			my $command = sprintf "%s \"%s\"", $gzipper, $file;
			if (system($command)) {
				print "   ! failed to automatically compress '$fname': $!\n";
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
	elsif ($file =~ /\.(?:fa|fasta|fai|ffn|dict)(?:\.gz)?$/i) {
		# sequence file of some sort
		$filetype = 'Sequence';
		$filedata{$fname}{zip} = 1;
	}
	elsif ($file =~ /\.(?:bed|bed\d+|gtf|gff|gff\d|narrowpeak|broadpeak|gappedpeak|refflat|genepred|ucsc)(?:\.gz)?$/i) {
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
	elsif ($file =~ /\.(?:sh|pl|py|pyc|r|rmd|rscript|awk|sm|sing)$/i) {
		$filetype = 'Script';
		$filedata{$fname}{zip} = 1;
	}
	elsif ($file eq 'cmd.txt') {
		# pysano command script
		$filetype = 'Script';
		$filedata{$fname}{zip} = 1;
	}
	elsif ($file =~ /\.(?:txt|tsv|tab|csv|cdt|counts|results|cns|cnr|cnn|md|log|biotypes)(?:\.gz)?$/i) {
		# general analysis text files, may be compressed
		$filetype = 'Text';
		$filedata{$fname}{zip} = 1;
	}
	elsif ($file =~ /\.(?:wig|bg|bdg|bedgraph)(?:\.gz)?$/i) {
		$filetype = 'Wiggle';
		$filedata{$fname}{zip} = 1;
	}
	elsif ($file =~ /\.mpileup.*\.gz$/) {
		# compressed mpileup files ok?, some people stick in text between mpileup and gz
		$filetype = 'Analysis';
		$filedata{$fname}{zip} = 0;
	}
	elsif ($file =~ /\.(?:bar|bar\.zip|useq|swi|egr|ser|mpileup|motif)$/i) {
		$filetype = 'Analysis';
		$filedata{$fname}{zip} = 1;
	}
	elsif ($file =~ /\.(?:xls|ppt|pptx|doc|docx|pdf|ps|eps|png|jpg|jpeg|gif|tif|tiff|svg|ai|out|rout|rda|rdata|rds|rproj|xml|json|json\.gz|html|pzfx|err|mtx|mtx\.gz)$/i) {
		$filetype = 'Results';
		$filedata{$fname}{zip} = 1;
	}
	elsif ($file =~ /\.(?:xlsx|h5|hd5|hdf5)$/i) {
		# leave out certain result files from zip archive just to be nice
		$filetype = 'Results';
		$filedata{$fname}{zip} = 0;
	}
	elsif ($file =~ /\.(?:tar|tar\.gz|tar\.bz2|zip)$/i) {
		$filetype = 'Archive';
		if ($file =~ /fastqc\.zip$/) {
			# no need keeping fastqc zip files separate
			$filedata{$fname}{zip} = 1; 
		}
		else {
			# something else
			$filedata{$fname}{zip} = 0; 
		}
	}
	elsif ($file =~ /\.(?:bt2|amb|ann|bwt|pac|nix|novoindex|index)$/) {
		$filetype = 'AlignmentIndex';
		$filedata{$fname}{zip} = 1; # zip I guess?
	}
	else {
		# catchall
		$filetype = 'Other';
		if ($size > $max_zip_size) {
			# it's pretty big
			printf "   ! Large unknown file $file at %.1fG\n", $size / 1000000000;
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


sub upload_files {
	
	### Grab missing information
	unless ($userfirst and $userlast) {
		# this can be grabbed from the first data line in the manifest CSV
		my $fh = IO::File->new($Project->manifest_file) or die "unable to read manifest file!";
		my $h = $fh->getline;
		my $l = $fh->getline;
		my @d = split(',', $l);
		$userfirst = $d[4];
		$userlast  = $d[5];
		$fh->close;
	}
	
	### Initialize SB wrapper
	my $sb = SB2->new(
		div     => $sb_division,
		cred    => $cred_path,
	) or die "unable to initialize SB wrapper module!";
	$sb->verbose(1) if $verbose;
	
	
	### Create the project on Seven Bridges
	# first check whether it exists
	# use lower case to mimic the short name that would be used.
	my $sbproject = $sb->get_project(lc($Project->id));
	
	
	# create the project if it doesn't exist
	if (defined $sbproject) {
		# we must be picking up from a previous upload
		printf "   > using existing SB project %s\n", $sbproject->id;
	}
	else {
		# generate description in markdown as necessary
		if (not $description) {
			$description = sprintf "# %s\n## %s\n GNomEx project %s is an Analysis project for %s %s",
				$Project->id, $title, $Project->id, $userfirst, $userlast;
			if ($group) {
				$description .= " in the group '$group'. ";
			}
			else {
				$description .= ". ";
			}
			if ($sb_species and $sb_genome) {
				$description .= "Analysis files are for species $sb_species, genome build version $sb_genome. ";
			}
			$description .= sprintf("Details on the experiment may be found in [GNomEx](https://hci-bio-app.hci.utah.edu/gnomex/?analysisNumber=%s).\n",
				$Project->id);
			$description .= "\n**Warning:** After these files are removed from GNomEx, these may be your only copies. Do not delete!\n";
		}
		
		# create project
		$sbproject = $sb->create_project(
			name        => $Project->id,
			description => $description,
		);
		if ($sbproject and $sbproject->id) {
			printf "   > created SB project %s\n", $sbproject->id;
		}
		else {
			print "   ! failed to make SB project!\n";
			$failure_count++;
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
		$failure_count++;
		return;
	};
	my $result = $sbproject->bulk_upload(@up_options);
	print $result;
	if ($result =~ /FAILED/) {
		$failure_count++;
		print "   ! upload failed!\n";
	}
	elsif ($result =~ /Done\.\n$/) {
		print "   > upload successful\n";
		
		# add the user to the project
		if ($email_address) {
			my $name = add_user_to_sb_project($sb, $sbproject);
			if ($name) {
				print "   > added user $name to SB project\n";
			}
			else {
				printf "   ! SB user for %s %s not found on platform\n", 
					$userfirst, $userlast;
			}
		}
	}
	else {
		$failure_count++;
		print "   ! upload error!\n";
	}
	return 1;
}


sub add_user_to_sb_project {
	my ($division, $sbproject) = @_;
	
	# find division member
	my $divMember;
	foreach my $member ($division->list_members) {
		if (lc($member->email) eq lc($email_address)) {
			# email matches, that was easy!
			$divMember = $member;
			last;
		}
		elsif (
			lc($member->last_name) eq lc($userlast) and 
			lc($member->first_name) eq lc($userfirst)
		) {
			# first and last names match
			$divMember = $member;
			last;
		}
	}
	return unless ($divMember);
	
	# check if user is already a member of project
	foreach my $member ($sbproject->list_members) {
		if ($member->username eq $divMember->username) {
			# user is already a member of this project!
			# nothing more needs to be done
			return $member->username;
		}
	}
	
	# otherwise add member with full permissions
	my @permissions = (
		'read'      => 'true',
		'copy'      => 'true',
		'write'     => 'true',
		'execute'   => 'true',
		'admin'     => 'true'
	);
	my $pMember = $sbproject->add_member($divMember, @permissions);
	if ($pMember) {
		return $pMember->username;
	}
	
	return;
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


