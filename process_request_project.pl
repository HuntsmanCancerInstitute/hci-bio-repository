#!/usr/bin/perl


use strict;
use IO::File;
use File::Spec;
use File::Find;
use File::Copy;
use File::Path qw(make_path);
use POSIX qw(strftime);
use Getopt::Long;

my $version = 1.2;

# shortcut variable name to use in the find callback
use vars qw(*fname);
*fname = *File::Find::name;



######## Documentation
my $doc = <<END;

A script to process GNomEx Experiment Request project folders for 
Seven Bridges upload.

This will generate a manifest CSV file from the fastq files, and 
optionally upload the files using SB tools, and optionally then hide 
the Fastq files in a hidden directory in preparation for eventual 
deletion from the server.

This requires collecting data from the GNomEx LIMS database and 
passing certain values on to this script for inclusion as metadata 
in the metadata Manifest CSV file. 

A Markdown description is generated for the Seven Bridges Project 
using the GNomEx metadata, including user name, strategy, title, 
and group name.

Usage:
    process_request_project.pl [options] /Repository/MicroarrayData/2019/1234R

Options:

 Mode
    --scan              Scan the project folder and generate the manifest
    --upload            Run the sbg-uploader program to upload
    --hide              Hide the Fastq files in hidden deletion folder

 Metadata
    --first <text>      User first name for the owner of the project
    --last <text>       User last name for the owner of the project
    --strategy "text"   GNomEx database application value. This is a long 
                        and varied text field, so must be protected by 
                        quoting. It will be distilled into a single-word 
                        value based on regular expression of keywords.
    --title "text"      GNomEx Request Name or title. This is a long text 
                        field, so must be protected by quoting.
    --group "text"      GNomEx Request group or ProjectName. This is a long 
                        text field, so must be protected by quoting.

 Seven Bridges
    --division <text>   The Seven Bridges division name

 Paths
    --sb <path>         Path to the Seven Bridges command-line api utility sb
    --sbup <path>       Path to the Seven Bridges Java uploader start script,
                        sbg-uploader.sh
    --cred <path>       Path to the Seven Bridges credentials file. 
                        Default is ~/.sevenbridges/credentials. Each profile 
                        should be named after the SB division.
    --verbose           Tell me everything!

END



######## Process command line options
my $given_dir;
my $scan;
my $hide_files;
my $upload;
my $userfirst;
my $userlast;
my $strategy;
my $title;
my $group;
my $sb_division;
my $sb_path = `which sb`;
chomp $sb_path;
my $sbupload_path = `which sbg-uploader.sh`;
chomp $sbupload_path;
my $cred_path = File::Spec->catfile($ENV{HOME}, '.sevenbridges', 'credentials');
my $verbose;

if (scalar(@ARGV) > 1) {
	GetOptions(
		'scan!'         => \$scan,
		'hide!'         => \$hide_files,
		'upload!'       => \$upload,
		'first=s'       => \$userfirst,
		'last=s'        => \$userlast,
		'strategy=s'    => \$strategy,
		'title=s'       => \$title,
		'group=s'       => \$group,
		'division=s'    => \$sb_division,
		'sb=s'          => \$sb_path,
		'sbup=s'        => \$sbupload_path,
		'cred=s'        => \$cred_path,
		'verbose!'      => \$verbose,
	) or die "please recheck your options!\n\n$doc\n";
}
else {
	print $doc;
	exit;
}
$given_dir = shift @ARGV;





######## Check options
if ($scan) {
	die "must provide user first name to scan!\n" unless $userfirst;
	die "must provide user last name to scan!\n" unless $userlast;
}
if ($upload) {
	die "must provide a SB division name!\n" unless $sb_division;
	die "must provide path to sb executable\n" unless $sb_path;
}	die "must provide path to sbg-uploader.sh executable\n" unless $sbupload_path;




######## Global variables
# these are needed since the File::Find callback doesn't accept pass through variables
my $start_time = time;
my @removelist;
my $project;
my %filedata;

# our sequence machine IDs to platform technology lookup
my %machinelookup = (
	'D00550'  => 'Illumina HiSeq',
	'D00294'  => 'Illumina HiSeq',
	'A00421'  => 'Illumina NovaSeq',
	'M05774'  => 'Illumina MiSeq',
	'M00736'  => 'Illumina MiSeq',
);

# experimental strategy
# the SB metadata expects simple value, so define this by matching with regex
# values are suggested by SB documentation, except for Single-cell-Seq
# the GNomEx application is too varied for anything more complicated. 
my $experimental_strategy;
if ($strategy =~ /10X Genomics/) {
	$experimental_strategy = 'Single-Cell-Seq';
}
elsif ($strategy =~ /mirna/i) {
	$experimental_strategy = 'miRNA-Seq';
}
elsif ($strategy =~ /rna/i) {
	$experimental_strategy = 'RNA-Seq';
}
elsif ($strategy =~ /(?:methyl|bisulfite)/i) {
	$experimental_strategy = 'Bisulfite-Seq';
}
elsif ($strategy =~ /(?:exome|capture)/i) {
	$experimental_strategy = 'WXS';
}
elsif ($strategy =~ /(?:dna|chip|atac)/i) {
	$experimental_strategy = 'DNA-Seq';
}
else {
	$experimental_strategy = 'Not available';
}





####### Check directories

# empirical directories
if ($verbose) {
	print " => SB path: $sb_path\n";
	print " => SB uploader path: $sbupload_path\n";
	print " => SB credentials path: $cred_path\n";
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
	print " > working on $project at $given_dir\n";
}
elsif ($given_dir =~ m/(\d{2,4})\/?$/) {
	# old style naming convention without an A prefix or R suffix
	$project = $1;
	print " > working on $project at $given_dir\n";
}
else {
	# non-canonical path, take the last given directory
	my @dir = File::Spec->splitdir($given_dir);
	$project = @dir[-1];
	print " > working on $project at $given_dir\n";
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
print " > changing to $given_dir\n";
chdir $given_dir or die "cannot change to $given_dir!\n";







####### Prepare file names

# file names in project directory
my $manifest_file = $project . "_MANIFEST.csv";
my $remove_file   = $project . "_REMOVE_LIST.txt";
my $notice_file   = "where_are_my_files.txt";

# hidden file names in parent directory
my $alt_remove    = File::Spec->catfile($parent_dir, $project . "_REMOVE_LIST.txt");

if ($verbose) {
	print " =>  manifest file: $manifest_file\n";
	print " =>    remove file: $remove_file or $alt_remove\n";
}


# removed file hidden folder
my $deleted_folder = File::Spec->catfile($parent_dir, $project . "_DELETED_FILES");
print " => deleted folder: $deleted_folder\n" if $verbose;
if (-e $deleted_folder) {
	print " ! deleted files hidden folder already exists! Will not move deleted files\n" if $hide_files;
	$hide_files = 0; # do not want to move zipped files
	print "! cannot re-scan if deleted files hidden folder exists!\n" if $scan;
	$scan = 0;
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
	print " > scanning $project in directory $given_dir\n";
	scan_directory();
}


# upload files to Seven Bridges
if ($upload) {
	if (-e $manifest_file) {
		print " > uploading $project files to $sb_division\n";
		upload_files();
	}
	else {
		print " ! No manifest file! Cannot upload files\n";
	}
}


# hide files
if ($hide_files) {
	if (-e $alt_remove) {
		print " > moving files to $deleted_folder\n";
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
	find( {
			follow => 0, # do not follow symlinks
			wanted => \&callback,
		  }, '.'
	);
	
	# confirm
	if (not scalar keys %filedata) {
		print " > nothing found!\n";
		return;
	}
	
	
	### Generate manifest file
	# check for pairs
	my $is_paired = 0;
	foreach (keys %filedata) {
		if ($filedata{$_}{pairedID} eq '2') {
			# string comparison instead of numeric because it might be null
			$is_paired = 1; # true
			last;
		}
	}

	# compile list
	my @manifest;
	push @manifest, join(',', qw(File sample_id investigation library_id platform 
								platform_unit_id paired_end quality_scale 
								experimental_strategy UserFirstName UserLastName Size Date MD5));
	foreach my $f (sort {$a cmp $b} keys %filedata) {
		push @manifest, join(',', 
			$filedata{$f}{clean},
			$filedata{$f}{sample},
			$project,
			$filedata{$f}{sample},
			sprintf("\"%s\"", $machinelookup{$filedata{$f}{machineID}}),
			$filedata{$f}{laneID},
			$is_paired ? $filedata{$f}{pairedID} : '-',
			'sanger',
			$experimental_strategy,
			sprintf("\"%s\"", $userfirst),
			sprintf("\"%s\"", $userlast),
			$filedata{$f}{size},
			sprintf("\"%s\"", $filedata{$f}{date}),
			$filedata{$f}{md5},
		);
		push @removelist, $filedata{$f}{clean};
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
	print "  > find callback on $file for $fname\n" if $verbose;

	
	### Ignore certain files
	if (not -f $file) {
		# skip directories and symlinks
		print "   > not a file, skipping\n" if $verbose;
		return;
	}
	elsif ($file =~ /libsnappyjava|fdt\.jar/) {
		# devil java spawn, delete!!!!
		print "   > deleting java file\n" if $verbose;
		unlink $file;
		return;
	}
	elsif ($file eq '.DS_Store' or $file eq 'Thumbs.db') {
		# Windows and Mac file browser devil spawn, delete these immediately
		print "   > deleting file browser metadata file\n" if $verbose;
		unlink $file;
		return;
	}
	elsif ($file eq $remove_file) {
		return;
	}
	elsif ($file eq $notice_file) {
		return;
	}
	elsif ($file eq $manifest_file) {
		return;
	}
	elsif ($fname =~ m/^\.\/(?:bioanalysis|Sample.?QC|Library.?QC|Sequence.?QC)\//) {
		# these are QC samples in a bioanalysis or Sample of Library QC folder
		# directly under the main project 
		print "   > skipping bioanalysis file\n" if $verbose;
		return;
	}
	
	
	### Possible Fastq file types
	my ($sample, $machineID, $laneID, $pairedID);
	# 15945X8_190320_M05774_0049_MS7833695-50V2_S1_L001_R2_001.fastq.gz
	# new style: 16013X1_190529_D00550_0563_BCDLULANXX_S12_L001_R1_001.fastq.gz
	if ($file =~ m/^(\d{4,5}[xX]\d+)_\d+_([ADM]\d+)_\d+_[A-Z\d\-]+_S\d+_L(\d+)_R(\d)_001\.(?:txt|fastq)\.gz$/) {
		$sample = $1;
		$machineID = $2;
		$laneID = $3;
		$pairedID = $4;
	}
	# new style index: 15603X1_181116_A00421_0025_AHFM7FDSXX_S4_L004_I1_001.fastq.gz
	elsif ($file =~ m/^(\d{4,5}[xX]\d+)_\d+_([ADM]\d+)_\d+_[A-Z\d\-]+_S\d+_L(\d+)_I\d_001\.(?:txt|fastq)\.gz$/) {
		$sample = $1;
		$machineID = $2;
		$laneID = $3;
		$pairedID = 3;
	}
	# new old style HiSeq: 15079X10_180427_D00294_0392_BCCEA1ANXX_R1.fastq.gz
	elsif ($file =~ m/^(\d{4,5}[xX]\d+)_\d+_([ADM]\d+)_\d+_[A-Z\d\-]+_R(\d)\.(?:txt|fastq)\.gz$/){
		$sample = $1;
		$machineID = $2;
		$laneID = 1;
		$pairedID = $3;
	}
	# old style, single-end: 15455X2_180920_D00294_0408_ACCFVWANXX_2.txt.gz
	elsif ($file =~ m/^(\d{4,5}[xX]\d+)_\d+_([ADM]\d+)_\d+_[A-Z\d]+_(\d)\.txt\.gz$/) {
		$sample = $1;
		$machineID = $2;
		$laneID = $3;
	}
	# old style, paired-end: 15066X1_180427_D00294_0392_BCCEA1ANXX_5_1.txt.gz
	elsif ($file =~ m/^(\d{4,5}[xX]\d+)_\d+_([ADM]\d+)_\d+_[A-Z\d]+_(\d)_[12]\.txt\.gz$/) {
		$sample = $1;
		$machineID = $2;
		$laneID = $3;
		$pairedID = $4;
	}
	# 10X genomics and MiSeq read file: 15454X1_S2_L001_R1_001.fastq.gz, sometimes not gz????
	elsif ($file =~ m/^(\d{4,5}[xX]\d+)_S\d+_L(\d+)_R(\d)_001\.fastq(?:\.gz)?$/) {
		$sample = $1;
		$laneID = $2;
		$pairedID = $3;
		# must grab the machine ID from the read name
		my $head = $file =~ m/\.gz$/ ? qx(gzip -dc $file | head -n 1) : qx(head -n 1 $file);
		if ($head =~ /^@([ADM]\d+):/) {
			$machineID = $1;
		}
	}
	# 10X genomics index file: 15454X1_S2_L001_I1_001.fastq.gz
	elsif ($file =~ m/^(\d{4,5}[xX]\d+)_S\d+_L(\d+)_I1_001\.fastq\.gz$/) {
		$sample = $1;
		$laneID = $2;
		$pairedID = 3;
		# must grab the machine ID from the read name
		my $head = qx(gzip -dc $file | head -n 1);
		if ($head =~ /^@([ADM]\d+):/) {
			$machineID = $1;
		}
	}
	# another MiSeq file: 15092X7_180424_M00736_0255_MS6563328-300V2_R1.fastq.gz
	elsif ($file =~ m/^(\d{4,5}[xX]\d+)_\d+_([ADM]\d+)_\d+_[A-Z\d\-]+_R(\d)\.fastq\.gz$/) {
		$sample = $1;
		$machineID = $2;
		$pairedID = $3;
		$laneID = 1;
	}
	# crazy name: GUPTA_S1_L001_R1_001.fastq.gz
	elsif ($file =~ m/^(\w+)_S\d+_L(\d+)_R(\d)_00\d\.fastq\.gz$/) {
		$sample = $1;
		$laneID = $2;
		$pairedID = $3;
		# must grab the machine ID from the read name
		my $head = qx(gzip -dc $file | head -n 1);
		if ($head =~ /^@([ADM]\d+):/) {
			$machineID = $1;
		}
	}
	# crazy index: GUPTA_S1_L001_I1_001.fastq.gz
	elsif ($file =~ m/^(\w+)_S\d+_L(\d+)_I\d_00\d\.fastq\.gz$/) {
		$sample = $1;
		$laneID = $2;
		$pairedID = 3;
		# must grab the machine ID from the read name
		my $head = qx(gzip -dc $file | head -n 1);
		if ($head =~ /^@([ADM]\d+):/) {
			$machineID = $1;
		}
	}
	# undetermined file: Undetermined_S0_L001_R1_001.fastq.gz
	elsif ($file =~ m/^Undetermined_.+\.fastq\.gz$/) {
		$sample = 'undetermined';
		if ($file =~ m/_L(\d+)/) {
			$laneID = $1;
		}
		if ($file =~ m/_R([12])/) {
			$pairedID = $1;
		}
		# must grab the machine ID from the read name
		my $head = qx(gzip -dc $file | head -n 1);
		if ($head =~ /^@([ADM]\d+):/) {
			$machineID = $1;
		}
	}
	# single checksum file
	elsif ($file =~ m/\.gz\.md5$/) {
		my $fh = IO::File->new($file);
		my $line = $fh->getline;
		my ($md5, undef) = split(/\s+/, $line);
		$fh->close;
		$file =~ s/\.md5$//;
		$filedata{$file}{md5} = $md5;
		print "   > processed md5 file\n" if $verbose;
		return; # do not continue
	}
	# multiple checksum file
	elsif ($file =~ m/^md5_.+\.txt$/) {
		my $fh = IO::File->new($file);
		while (my $line = $fh->getline) {
			my ($md5, $fastqpath) = split(/\s+/, $line);
			my (undef, undef, $fastqfile) = File::Spec->splitpath($fastqpath);
			$filedata{$fastqfile}{md5} = $md5;
		}
		$fh->close;
		print "   > processed md5 file\n" if $verbose;
		return; # do not continue
	}
	else {
		# programmer error!
		print "   ! unrecognized file $fname!\n";
		return;
	}
	
	# stats on the file
	my @st = stat($file);
	
	# generate a clean name for recording
	my $clean_name = $fname;
	$clean_name =~ s|^\./||; # strip the beginning ./ from the name to clean it up
	
	### Record the collected file information
	$filedata{$file}{clean} = $clean_name;
	$filedata{$file}{sample} = $sample;
	$filedata{$file}{machineID} = $machineID;
	$filedata{$file}{laneID} = $laneID;
	$filedata{$file}{pairedID} = $pairedID;
	$filedata{$file}{date} = strftime("%B %d, %Y %H:%M:%S", localtime(@st[9]));
	$filedata{$file}{size} = $st[7];
	
	print "   > processed fastq file\n" if $verbose;
}


sub upload_files {
	
	### Grab missing information
	unless ($userfirst and $userlast and $strategy) {
		my $fh = IO::File->new($manifest_file) or die "unable to read manifest file!";
		my $h = $fh->getline;
		my $l = $fh->getline;
		my @d = split(',', $l);
		$userfirst = $d[9];
		$userlast  = $d[10];
		$strategy  = $d[8];
		$fh->close;
	}
	
	
	### Create the project on Seven Bridges
	my $sbproject;
	
	# check whether it exists
	my $command = join(" ", $sb_path, 'projects', 'list', '--config', $cred_path, 
						'--profile', $sb_division);
	if ($verbose) {
		print "   > Executing: $command\n";
	}
	my $result = qx($command);
	foreach my $line (split "\n", $result) {
		my @f = split "\t", $line;
		if ($f[1] eq "'$project'") {
			# we found it
			$sbproject = $f[0];
			print "   > using existing SB project $sbproject\n";
			last;
		}
	}
	
	# create the project if it doesn't exist
	if (not defined $sbproject) {
		# generate description in markdown
		if ($strategy eq 'Not available') {
			# not available is not a suitable description. Substitute generic sequencing.
			$strategy = 'sequencing';
		}
		my $description = sprintf "# %s\n## %s\n GNomEx project %s is a %s experiment generated by %s %s in the group %s. Details on the experiment may be found in [GNomEx](https://hci-bio-app.hci.utah.edu/gnomex/gnomexFlex.jsp?requestNumber=%s).\n",
			$project, $title, $project, $strategy, $userfirst, $userlast, $group, $project; 
	
		# sb projects create --name playground --description
		my $command = join(" ", $sb_path, 'projects', 'create', '--name', $project, '--description',
						"\"$description\"", '--config', $cred_path, '--profile', $sb_division);
		if ($verbose) {
			print "   > Executing: $command\n";
		}
		my $result = qx($command);
		if ($result =~ m|Created project `([\w\-]+/[\w\-]+)`|) {
			$sbproject = $1;
			print "   > created SB project $sbproject\n";
		}
		else {
			print "   ! failed to make SB project: $result\n";
			return;
		}
	}
	
	### Grab the token
	my $token;
	my $fh = IO::File->new($cred_path) or 
		die "unable to read credentials files!\n";
	while (not defined $token) {
		my $line = $fh->getline or last;
		chomp $line;
		if ($line =~ /^\[$sb_division\]$/) {
			# we found the section!
			while (my $line2 = $fh->getline) {
				if ($line2 =~ /^\[/) {
					# we've gone too far!!!??? start of next section
					last;
				}
				elsif ($line2 =~ /^auth_token\s+=\s+(\w+)$/) {
					# we found it!
					$token = $1;
					last;
				}
			}
		}
	}
	$fh->close;
	unless ($token) {
		print "   ! unable to get token from credentials!\n";
		return;
	}
	
	
	### Upload the files
	my @upcommand = ($sbupload_path, '--project', $sbproject, '--token', $token, 
					'--manifest-file', $manifest_file, '--manifest-metadata', 
					'sample_id', 'investigation', 'library_id', 'platform', 
					'platform_unit_id', 'paired_end', 'quality_scale', 
					'experimental_strategy', 'UserFirstName', 'UserLastName');
	if ($verbose) {
		printf "   > Executing: %s\n", join(' ', @upcommand);
	}
	my $fail = system(@upcommand);
	if ($fail) {
		print "   ! upload failed!\n";
	}
	else {
		print "   > upload successful\n";
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
		my (undef, $dir, $basefile) = File::Spec->splitpath($file);
		unless (-e $file) {
			# older versions may record the project folder in the name, so let's 
			# try removing that
			$file =~ s/^$project\///;
			next unless -e $file; # give up if it's still not there
			(undef, $dir, $basefile) = File::Spec->splitpath($file); # regenerate these
		}
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








