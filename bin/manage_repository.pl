#!/usr/bin/env perl

use warnings;
use strict;
use English qw(-no_match_vars);
use Getopt::Long;
use Cwd;
use File::Spec;
use IO::File;
use Time::Local;
use FindBin qw($Bin);
use lib "$Bin/../lib";
use RepoCatalog;
use RepoProject;
use hciCore qw( generate_prefix generate_bucket );
# Gnomex  is loaded at run time as necessary
# Emailer is loaded at run time as necessary


our $VERSION = 8.2;


######## Documentation
my $doc = <<END;

The main application for managing HCI Bio Repository GNomEx Projects.
This requires a catalog database file and access to the hci-bio-repo server.

It is primarily used for managing the life cycles of GNomEx projects, both 
Analysis and Experiment Requests. This includes searching and listing 
projects based on status, age, size, ownership, etc. Projects that have 
exceeded their lifespan can be prepared for offloading to other storage 
platforms, for example AWS accounts. 

Expired projects may be scanned and inventoried, moved to a hidden directory,
and permanently deleted. Scanning and archiving files is done by a separate
application, 'process_project.pl'. This writes important file lists in the
project directory or parent – all subsequent project file actions depend on
these lists and cannot proceed without them. No project file manipulation is
performed through blind or bulk wildcards. 

All data is stored in a local catalog (database) file. This catalog may be 
exported (and imported) as a simple tab-delimited text file for backup purposes
and/or work in other applications (Excel). Importing new projects can be 
done directly from the GNomEx SQL database.

Best practices include searching for projects and directing output to a 
text file. This file can then be provided to this application as an input 
list. Any errors are printed with leading exclamation marks and should 
be rectified before repeating or proceeding.


VERSION $VERSION

USAGE

  manage_repository.pl --cat <file.db> [options] [projects]

  manage_repository.pl --cat analysis.db --status A1234 A1235 A1236

  manage_repository.pl --cat request.db --list_req_up --status > projects.txt

  manage_repository.pl --cat request.db --list projects.txt --scan > projects.scan.txt

OPTIONS

  Required:
    -c --cat <file>           Provide the path to a catalog database file
  
  Import from GNomEx:
    --import_anal             Fetch and update Analysis projects from GNomEx DB
    --import_req              Fetch and update Request projects from GNomEx DB
  
  Catalog entry selection: 
    --list <file>             File of project identifiers to work on
                                may be tab-delimited, only 1st column used
    --list_req_up             Print or work on Request IDs for upload to AWS
    --list_req_hide           Print or work on Request IDs for hiding
    --list_req_delete         Print or work on Request IDs for deletion
    --list_anal_up            Print or work on Analysis IDs for uploading to AWS
    --list_anal_hide          Print or work on Analysis IDs for hiding
    --list_anal_delete        Print or work on Analysis IDs for deletion
    --list_lab <pi_lastname>  Print or select based on PI last name
    --list_req                Print or select all Request catalog entries
    --list_anal               Print or select all Analysis catalog entries
    --list_all                Print or select all catalog entries
    --list_aa                 Print or select all AutoAnalysis Request entries
    --list_aa_up              Print or work on AutoAnalysis Requests for upload
  
  Catalog selection modifiers:
    --year <YYYY>             Select entries to given year or newer
    --age <days>              Select entries older than minimum age (days)
    --max_age <days>          Select entries younger than maximum age (days)
    --size <bytes>            Select entries bigger than minimum project size
                                allows K, M, and G suffix
    --core                    Select only projects with a defined CORE lab name
    --nocore                  Exclude projects with a CORE lab name
    --external                Select only external projects (assumes no CORE lab)
    --noexternal              Exclude external projects
    --emailed                 Select only projects that have been emailed
    --noemailed               Exclude projects that have been emailed
    
  Display catalog entries (select one): 
    --status                  Print the status of listed projects
    --info                    Print basic information of listed projects
    --aastat                  Print status of Request AutoAnalysis projects
    --path                    Print the local repository path of listed project
    --url                     Print the S3 URL of listed projects
    --print                   Print all the information of listed projects
                                tab-delimited format with header
  
  Actions on projects:
    --scan                    Scan the project directory and write METADATA
    --zip                     Zip archive Analysis files
    --upload                  Upload projects to AWS bucket, skips AutoAnalysis
    --aaupload                Upload projects, including AutoAnalysis folders

  Actions on project directories:
    --hide_zip                Hide the zipped files to _ZIPPED_FILES folder
    --hide_del                Hide the to-delete files to _DELETED_FILES folder
    --show_zip                Unhide files from _ZIPPED_FILES folder
    --show_del                Unhide files from _DELETED_FILES folder
    --restore_zip             Restore files from the zip archive file
    --del_zip                 Delete hidden zipped files
    --delete                  Delete the to-delete files from the project 
                                directory and/or hidden folder
    --notice                  Symlink the notice file into the project folder
    --clean                   Safely remove manifest, list files
  
  Actions to notify:
    --email_anal_del          Email Analysis scheduled deletion
    --email_anal_up           Email Analysis upload
    --email_req_del           Email Request scheduled deletion
    --email_req_up            Email Request upload
  
  Actions to update catalog entries:
       For the <date> value, use 'YYYYMMDD' for specific date, 'now'
       for the current time and date, or '0' to clear.
    --update_scan <date>      Update project scan timestamp
    --update_del <date>       Update project deletion timestamp
    --update_hide <date>      Update project hide timestamp
    --update_up <date>        Update project upload timestamp
    --update_aa_up <date>     Update project AutoAnalysis upload timestamp
    --update_em <date>        Update project email timestamp
    --update_size --update_age Update project size and age from file server
                                ignores QC and AutoAnalysis folders
    --update_core <text>      Update CORE lab name. Use 'none' to clear.
    --update_profile <text>   Update the AWS IAM profile
    --update_bucket <text>    Update the AWS S3 bucket
    --update_prefix <text>    Update the AWS prefix (only one project)
    --generate_s3             Generate default AWS S3 bucket/prefix
    --delete_entry            Delete catalog entry
  
  Actions on catalog file:
    --export <path>           Dump the contents to tab-delimited text file
    --import_file <path>      Import an exported table, requires non-transformed
    --optimize                Run the db file optimize routine (!?)
  
  File paths:
    --labinfo <path>          Path to Lab Information file with CORE lab info
    --cred <path>             Path to AWS credentials file. 
                                Default is ~/.aws/credentials. 

  General:
    --mock                    For scan, upload, zip, and email functions only,
                                 print command or output, but perform no work
    --forks <int>             Number of parallel forks for uploads
    --transform               When exporting transform to human conventions
    --biggest                 Print the biggest size in status (current or previous)
    -v --verbose              Print additional output for some functions
    -h --help                 Print documentation
  
END
 




####### Global variables

my $cat_file;
my $list_req_upload = 0;
my $list_req_hide = 0;
my $list_req_delete = 0;
my $list_aa_upload = 0;
my $list_anal_upload = 0;
my $list_anal_hide = 0;
my $list_anal_delete = 0;
my $list_pi = 0;
my $list_file;
my $list_all = 0;
my $list_req = 0;
my $list_aa = 0;
my $list_anal = 0;
my $year;
my $include_core;
my $min_age;
my $max_age;
my $min_size;
my $external;
my $emailed;
my $show_status = 0;
my $show_info = 0;
my $show_aa_status = 0;
my $show_path = 0;
my $show_url = 0;
my $print_info = 0;
my $delete_entry = 0;
my $scan_size_age = 0;
my $update_scan_date;
my $update_hide_date;
my $update_upload_date;
my $update_aa_upload_date;
my $update_delete_date;
my $update_email_date;
my $update_core_lab;
my $update_profile;
my $update_bucket;
my $update_prefix;
my $generate_s3_path;
my $project_scan;
my $project_upload;
my $project_zip;
my $project_aa_upload;
my $move_zip_files;
my $move_del_files;
my $unhide_zip_files;
my $unhide_del_files;
my $restore_zip_files;
my $delete_zip_files;
my $delete_del_files;
my $add_notice;
my $clean_project_files;
my $email_anal_del = 0;
my $email_anal_up = 0;
my $email_req_del = 0;
my $email_req_up = 0;
my $mock;
my $fetch_analysis;
my $fetch_request;
my $dump_file;
my $transform = 0;
my $biggest_size;
my $import_file;
my $run_optimize;
my $force;
my $forks;
my $labinfo_path;
my $cred_path;
my $verbose;
my $help;



######## Command line options

if (scalar(@ARGV) > 1) {
	GetOptions(
		'c|catalog=s'           => \$cat_file,
		'list_req_up!'          => \$list_req_upload,
		'list_req_hide!'        => \$list_req_hide,
		'list_req_delete!'      => \$list_req_delete,
		'list_aa_up!'           => \$list_aa_upload,
		'list_anal_up!'         => \$list_anal_upload,
		'list_anal_hide!'       => \$list_anal_hide,
		'list_anal_delete!'     => \$list_anal_delete,
		'list_lab|list_pi=s'    => \$list_pi,
		'list_all!'             => \$list_all,
		'list_anal!'            => \$list_anal,
		'list_aa!'              => \$list_aa,
		'list_req!'             => \$list_req,
		'list=s'                => \$list_file,
		'core!'                 => \$include_core,
		'year=i'                => \$year,
		'age=i'                 => \$min_age,
		'max_age=i'             => \$max_age,
		'size=s'                => \$min_size,
		'external!'             => \$external,
		'emailed!'              => \$emailed,
		'status!'               => \$show_status,
		'info!'                 => \$show_info,
		'aastatus!'             => \$show_aa_status,
		'path!'                 => \$show_path,
		'url!'                  => \$show_url,
		'print!'                => \$print_info,
		'delete_entry!'         => \$delete_entry,
		'scan!'                 => \$project_scan,
		'upload!'               => \$project_upload,
		'aaupload!'             => \$project_aa_upload,
		'zip!'                  => \$project_zip,
		'hide_zip!'             => \$move_zip_files,
		'hide_del!'             => \$move_del_files,
		'show_zip!'             => \$unhide_zip_files,
		'show_del!'             => \$unhide_del_files,
		'restore_zip!'          => \$restore_zip_files,
		'del_zip!'              => \$delete_zip_files,
		'delete!'               => \$delete_del_files,
		'notice!'               => \$add_notice,
		'clean!'                => \$clean_project_files,
		'email_anal_del!'       => \$email_anal_del,
		'email_anal_up!'        => \$email_anal_up,
		'email_req_del!'        => \$email_req_del,
		'email_req_up!'         => \$email_req_up,
		'mock!'                 => \$mock,
		'import_anal!'          => \$fetch_analysis,
		'import_req!'           => \$fetch_request,
		'update_scan=s'         => \$update_scan_date,
		'update_hide=s'         => \$update_hide_date,
		'update_up=s'           => \$update_upload_date,
		'update_aa_up=s'        => \$update_aa_upload_date,
		'update_del=s'          => \$update_delete_date,
		'update_email=s'        => \$update_email_date,
		'update_core=s'         => \$update_core_lab,
		'update_profile=s'      => \$update_profile,
		'update_bucket=s'       => \$update_bucket,
		'update_prefix=s'       => \$update_prefix,
		'generate_s3!'          => \$generate_s3_path,
		'update_size|update_age!' => \$scan_size_age,
		'export_file=s'         => \$dump_file,
		'import_file=s'         => \$import_file,
		'force!'                => \$force,
		'forks=i'               => \$forks,
		'transform!'            => \$transform,
		'optimize!'             => \$run_optimize,
		'biggest!'              => \$biggest_size,
		'cred=s'                => \$cred_path,
		'labinfo=s'             => \$labinfo_path,
		'v|verbose!'            => \$verbose,
		'h|help!'               => \$help,
	) or die "please recheck your options!\n\n";
}
else {
	print $doc;
	exit;
}
if ($help) {
	print $doc;
	exit;
}




#### Main functions
check_options();
my @action_list;
my $Catalog = open_import_catalog();
unless (@action_list) {
	@action_list = generate_list();
}
run_metadata_actions();
run_project_actions();
run_project_directory_actions();
run_email_notifications();
print_functions();
if (@action_list) {
	printf STDERR " => %d projects\n", scalar(@action_list);
}
final_catalog_functions();

exit 0;







############### Subroutines to do all the work

sub check_options {
	
	# a catalog file is essential!
	
	if ($cat_file) {
		if ($cat_file !~ m|^/|) {
			# catalog file path is not from root
			$cat_file = File::Spec->rel2abs($cat_file);
		}
		if ( not -e $cat_file and not $import_file ) {
			printf "FATAL: '%s' cannot be found!\n", $cat_file;
			exit 1;
		}
	}
	else {
		print "FATAL: No catalog file provided!\n";
		exit 1;
	}
	
	# search functions
	my $sanity = $list_req_upload + $list_req_hide + $list_req_delete + 
		$list_anal_upload + $list_anal_hide + $list_anal_delete + $list_all + 
		$list_req + $list_anal + $list_aa + $list_aa_upload + ($list_pi ? 1 : 0);
	if ($sanity > 1) {
		print "FATAL: Only 1 Request search allowed at a time!\n";
		exit 1;
	}
	elsif ($sanity == 1) {
		if ($dump_file) {
			print "FATAL: No search functions allowed if exporting!\n" ;
			exit 1;
		}
		if ($import_file) {
			print "FATAL: No search functions allowed if importing!\n";
			exit 1;
		}
		
	}
	
	# print function
	$sanity = 0;
	$sanity = $show_status + $show_info + $show_aa_status + $show_path + $show_url + 
		$print_info;
	if ($sanity > 1) {
		print "FATAL: Only 1 printing function allowed at a time!\n";
		exit 1;
	}
	
	# email function
	$sanity = 0;
	$sanity = $email_anal_del + $email_anal_up + $email_req_del + $email_req_up;
	if ($sanity > 1) {
		print "FATAL: Only 1 email function allowed at a time!\n";
		exit 1;
	}
	
	# year
	if ($year and $year !~ /\d{4}/) {
		print "FATAL: year must be four digits!\n";
		exit 1;
	}
	if (($fetch_analysis or $fetch_request) and not $year) {
		# set default year based on a calculation
		# year is 60 * 60 * 24 * 365 = 31536000 seconds
		my $n = $fetch_analysis ? 94608000 : 63072000; # 3 years Analysis, 2 Request
		my @t = localtime(time - $n);
		$year = $t[5] + 1900;
	}

	# external
	if (defined $external) {
		$external = $external ? 'Y' : 'N';
		$include_core = 0 if $external eq 'Y'; # no external lab should have CORE account
	}
	
	# convert sizes
	if ($min_size) {
		if ($min_size =~ /^([\d]+)k$/i) {
			$min_size = $1 * 1024;
		}
		elsif ($min_size =~ /^(\d+)m$/i) {
			$min_size = $1 * 1048576;
		}
		elsif ($min_size =~ /^(\d+)g$/i) {
			$min_size = $1 * 1073741824;
		}
		elsif ($min_size =~ /^\d+$/) {
			# this is ok, just a number
		}
		else {
			print
			"FATAL: Minimum size filter must be integer! K, M, and G suffix is allowed\n";
			exit 1;
		}
	}
	
	# file manipulations
	if ($unhide_zip_files or $unhide_del_files) {
		if ( $move_del_files or $move_zip_files or $delete_del_files or
			$delete_zip_files
		) {
			print "FATAL: Can't do anything else if unhiding files!\n";
			exit 1;
		}
	}
	
	# contradictory metadata options
	if ( $generate_s3_path and ( $update_bucket or $update_prefix ) ) {
		print 
"FATAL: Cannot specify --generate_s3 with --update_bucket or --update_prefix options\n";
		exit 1;
	}
}


sub open_import_catalog {
	
	# open catalog
	my $Cat = RepoCatalog->new($cat_file) or 
		die "Cannot open catalog file '$cat_file'!\n";

	# import catalog 
	if ($import_file) {
		my $n = $Cat->import_from_file($import_file, $force);
		if ($n) {
			print " Imported $n records from file '$import_file'\n";
		}
		else {
			print " Import from file '$import_file' failed!\n";
		}
		
		# exit immediately, do not do anything else. why? not sure
		exit;
	}
	
	# import from database
	if ($fetch_analysis or $fetch_request) {
		
		# Initialize the GNomEx database adapter
		my $gnomex_good = 0;
		eval {
			require Gnomex;
			$gnomex_good = 1;
		};
		my $GNomEx;
		if ($gnomex_good) {
			$GNomEx = Gnomex->new(
				catalog => $Cat,
				lab     => $labinfo_path,
			) or die "can't instantiate Gnomex object!\n";
		}
		else {
			die "problem! $EVAL_ERROR\n";
		}
		
			
		### Analysis
		if ($fetch_analysis) {
			print " Fetching new analysis projects from database...\n";
			my ($update_list, $new_list, $nochange_list, $skip_count) = 
				$GNomEx->fetch_analyses($year);
			printf " Finished processing %d Analysis project database entries\n", 
				scalar(@{$update_list}) + scalar(@{$new_list}) + scalar(@{$nochange_list});
			
			# update information from the repository file server
			print " Updating project sizes and ages....\n";
			ANALYSIS_FETCH_LOOP:
			foreach my $id (@{$update_list}, @{$new_list}, @{$nochange_list}) {
				my $Entry = $Cat->entry($id);
				my $path  = $Entry->path;
				unless (-e $path) {
					print "  ! $id missing project file path: $path\n";
					next ANALYSIS_FETCH_LOOP;
				}
				my $Project = RepoProject->new($Entry->path);
				if ($Project) {
					my $prev_size = $Entry->size;
					my ($size, $datestamp, $aa_datestamp) = $Project->get_size_age;
					if ($size) {
						$Entry->size($size);
					}
					else {
						# no file sizes!? must be cleaned out, update and move on
						if ( $Entry->size >= 1048576 and $Entry->hidden_datestamp == 0 ) {
							print "  ! $id was forcefully emptied without plan\n";
						}
						$Entry->size(0);
						next ANALYSIS_FETCH_LOOP;
					}
					if ($datestamp) {
						$Entry->youngest_datestamp($datestamp);
					}

					# print warnings if something seems amiss
					# this is entirely dependent on file ages, not on size
					if ( $Entry->deleted_datestamp > 1 and
						$datestamp > $Entry->deleted_datestamp
					) {
						print "  ! New files added to deleted project $id\n";
						next ANALYSIS_FETCH_LOOP;
					}
					elsif ( $Entry->hidden_datestamp > 1 and
						$datestamp > $Entry->hidden_datestamp
					) {
						print "  ! New files added to hidden project $id\n";
						next ANALYSIS_FETCH_LOOP;
					}

					# skip remainder of loop if project hidden or deleted
					next ANALYSIS_FETCH_LOOP if $Entry->deleted_datestamp;
					next ANALYSIS_FETCH_LOOP if $Entry->hidden_datestamp;

					# Check if needs to be scanned
					if ($project_scan) {
						my $do_scan = 0;
						if ( $Entry->scan_datestamp > 1 and
							( time - $Entry->scan_datestamp ) > 3600 and
							( $datestamp - time ) > 3600
						) {
							# previously scanned at least an hour ago and
							# files are at least an hour old
							$do_scan += 1;
						}
						elsif ( $Entry->scan_datestamp == 0 and
								($Entry->age and $Entry->age >= 7 )
						) {
							# otherwise wait for project to "settle" for at least
							# one week before scanning
							$do_scan += 1;
						}
						elsif ( abs($prev_size - $size) >= 1024 and
								$Entry->scan_datestamp > 0
						) {
							# odd situation where size changes by 1 KiB in size
							# with files that don't trigger an age change
							$do_scan += 1;
						}

						# print verbose message
						if ($do_scan) {
							printf "  > will scan %s, age %s, last scanned %s days ago\n",
								$id, $Entry->age || '0', $Entry->scan_datestamp ? 
								sprintf("%.0f",
								(time - $Entry->scan_datestamp) / 86400) : '-';
							push @action_list, $id;
						}
					}
				}
				else {
					printf " ERROR! unable to generate RepoProject object for %s!\n ",
						$path;
					next ANALYSIS_FETCH_LOOP;
				}
			}
			
			# print report
			printf
"\n Analysis project import summary:\n  %d skipped\n  %d unchanged\n  %d updated\n  %d new\n", 
				$skip_count, scalar(@{$nochange_list}), scalar(@{$update_list}), 
				scalar(@{$new_list});
		}
		
		### Request
		if ($fetch_request) {
			print " Fetching new request projects from database...\n";
			my ($update_list, $new_list, $nochange_list, $skip_count) = 
				$GNomEx->fetch_requests($year);
			printf " Finished processing %d Experiment Request project database entries\n", 
				scalar(@{$update_list}) + scalar(@{$new_list}) + scalar(@{$nochange_list});
			
			# update information from the repository file server
			print " Updating project sizes and ages....\n";
			REQUEST_FETCH_LOOP:
			foreach my $id ( @{$update_list}, @{$new_list}, @{$nochange_list} ) {
				my $Entry = $Cat->entry($id);
				my $path  = $Entry->path;
				unless (-e $path) {
					print "  ! $id missing project file path: $path\n";
					next REQUEST_FETCH_LOOP;
				}
				my $Project = RepoProject->new($path);
				if ($Project) {
					my $do_scan = 0;
					my ($size, $datestamp, $aa_datestamp) = $Project->get_size_age;
					if ($size) {
						$Entry->size($size);
					}
					else {
						# no size!? Someone must have emptied it
						if ( $Entry->size >= 1048576 and $Entry->hidden_datestamp == 0 ) {
							print "  ! $id was forcefully emptied without plan\n";
						}
						$Entry->size(0);
						next REQUEST_FETCH_LOOP;
					}
					if ($datestamp) {
						$Entry->youngest_datestamp($datestamp);
					}

					# print warnings if something seems amiss and skip remainder of loop
					if ($Entry->deleted_datestamp and 
						$datestamp > $Entry->deleted_datestamp
					) {
						print "  ! New files added to deleted project $id\n";
						next REQUEST_FETCH_LOOP;
					}
					elsif ($Entry->hidden_datestamp and 
						$datestamp > $Entry->hidden_datestamp
					) {
						print "  ! New files added to hidden project $id\n";
						next REQUEST_FETCH_LOOP;
					}

					# skip remainder of loop if project hidden or deleted
					next REQUEST_FETCH_LOOP if $Entry->deleted_datestamp;
					next REQUEST_FETCH_LOOP if $Entry->hidden_datestamp;

					# AutoAnalysis folder
					my $aa_folder = $Project->get_autoanal_folder;
					if ($aa_folder) {
						if ($Entry->autoanal_folder ) {
							if ( $aa_folder ne $Entry->autoanal_folder ) {
								printf "  ! Updated AutoAnalysis folder for %s to %s\n",
									$id, $aa_folder;
								$Entry->autoanal_folder($aa_folder);
								$do_scan += 1;
							}
							if (
								(time - $aa_datestamp) > 3600 and
								($aa_datestamp - $Entry->scan_datestamp) > 3600
							) {
								# youngest autoanal file is at least 1 hour old
								# and older than the last scan by 1 hour
								$do_scan += 1;
							}
						}
						else {
							$Entry->autoanal_folder($aa_folder);
							$do_scan += 1;
						}
					}
					elsif ( not $aa_folder and $Entry->autoanal_folder ) {
						printf "  ! AutoAnalysis folder for %s was removed\n", $id;
						$Entry->autoanal_folder( q() );
						$do_scan += 1;
					}

					# Check if needs to be scanned, only if there are fastq files present
					if ( $project_scan and $Project->has_fastq ) {
						if ( $Entry->scan_datestamp > 1 ) {
							if ( $datestamp - $Entry->scan_datestamp > 3600 ) {
								# there is a younger file than last scan by 1 hour
								$do_scan += 1;
							}
						}
						else {
							$do_scan += 1;
						}
						if ($do_scan) {
							printf
								"  > will scan %s, age %3s, last scanned %3s days ago\n",
								$id, $Entry->age || 0,
								$Entry->scan_datestamp ? sprintf("%.0f",
								(time - $Entry->scan_datestamp) / 86400) : '-';
							push @action_list, $id;
						}
						# check S3 information
						if ( $Entry->core_lab and not $Entry->bucket ) {
							generate_bucket($Entry);
							generate_prefix($Entry);
						}
					}
				}
				else {
					printf " ERROR! unable to generate RepoProject object for %s!\n ",
						$path;
					next REQUEST_FETCH_LOOP;
				}
			}
			
			# print report
			printf
"\n Request project import summary:\n  %d skipped\n  %d unchanged\n  %d updated\n  %d new\n", 
				$skip_count, scalar(@{$nochange_list}), scalar(@{$update_list}), 
				scalar(@{$new_list});
		}

		# reset flag as this is already done
		$scan_size_age = 0;

		# print number of project to scan as final report
		if (@action_list) {
			printf "\n  %d projects to scan\n", scalar @action_list;
		}

	}

	return $Cat;
}


sub generate_list {
	
	# Go through possible ways of generating the list, only one allowed

	# command line
	if (@ARGV) {
		return @ARGV;
	}
	
	# input file
	elsif ($list_file) {
		my $fh = IO::File->new($list_file) or 
			die "Cannot open import file '$list_file'! $OS_ERROR\n";
		my @list;
		
		# check header
		my $header = $fh->getline;
		if ($header =~ m/^ (?: \d+R | A\d+ ) /x) {
			# the first line looks like a project identifier, so keep it
			chomp $header;
			my ($id, @rest) = split /\s+/, $header;
			push @list, $id;
		}
		
		# load remaining file
		while (my $line = $fh->getline) {
			chomp $line;
			my ($id, @rest) = split /\s+/, $line;
			push @list, $id;
		}
		
		# printf " loaded %d lines from $list_file\n", scalar(@action_list);
		$fh->close;
		return @list;
	}
	
	# search for all entries
	elsif ( $list_all or $list_req or $list_anal ) {
		my @list = $Catalog->list_all(
			age      => $min_age, 
			maxage   => $max_age,
			year     => $year, 
			core     => $include_core,
			external => $external,
			size     => $min_size,
			emailed  => (defined $emailed ? $emailed : -1 ),
		);
		if ($list_req) {
			return grep  {m/^\d+R$/} @list;
		}
		elsif ($list_anal) {
			return grep {m/^A\d+$/} @list;
		}
		else {
			return @list;
		}
	}
	
	# search for requests to upload
	elsif ($list_req_upload) {
		die "Can't find entries if list provided!\n" if @action_list;
		return $Catalog->find_requests_to_upload(
			year     => $year,
			age      => $min_age,
			maxage   => $max_age,
			size     => $min_size,
		);
	}
	
	# search for requests to hide
	elsif ($list_req_hide) {
		die "Can't find entries if list provided!\n" if @action_list;
		return $Catalog->find_requests_to_hide(
			age      => $min_age, 
			maxage   => $max_age,
			year     => $year, 
			core     => $include_core,
			external => $external,
			size     => $min_size,
			emailed  => (defined $emailed ? $emailed : -1 ),
		);
	}
	
	# search for requests to delete
	elsif ($list_req_delete) {
		die "Can't find entries if list provided!\n" if @action_list;
		return $Catalog->find_requests_to_delete(
			age      => $min_age, 
			maxage   => $max_age,
			year     => $year, 
			core     => $include_core,
			external => $external,
			size     => $min_size,
			emailed  => (defined $emailed ? $emailed : -1 ),
		);
	}
	
	# search for analyses to upload
	elsif ($list_anal_upload) {
		die "Can't find entries if list provided!\n" if @action_list;
		return $Catalog->find_analysis_to_upload(
			age      => $min_age, 
			maxage   => $max_age,
			year     => $year, 
			size     => $min_size,
		);
	}
	
	# search for analyses to hide
	elsif ($list_anal_hide) {
		die "Can't find entries if list provided!\n" if @action_list;
		return $Catalog->find_analysis_to_hide(
			age      => $min_age, 
			maxage   => $max_age,
			year     => $year, 
			core     => $include_core,
			external => $external,
			size     => $min_size,
			emailed  => (defined $emailed ? $emailed : -1 ),
		);
	}
	
	# search for analyses to delete
	elsif ($list_anal_delete) {
		die "Can't find entries if list provided!\n" if @action_list;
		return $Catalog->find_analysis_to_delete(
			age      => $min_age, 
			maxage   => $max_age,
			year     => $year, 
			core     => $include_core,
			external => $external,
			size     => $min_size,
			emailed  => (defined $emailed ? $emailed : -1 ),
		);
	}
	
	# search for projects owned by a principal investigator - last name only!
	elsif ($list_pi) {
		die "Can't find entries if list provided!\n" if @action_list;
		return $Catalog->list_projects_for_pi(
			name     => $list_pi,
			year     => $year,
			age      => $min_age,
			maxage   => $max_age,
			size     => $min_size,			
			emailed  => (defined $emailed ? $emailed : -1 ),
		);
	}
	
	# search for requests with autoanalysis folders
	elsif ($list_aa) {
		die "Can't find entries if list provided!\n" if @action_list;
		return $Catalog->find_autoanal_req(
			year     => $year,
			age      => $min_age,
			maxage   => $max_age,
		);
	}
	
	# search for requests with autoanalysis folders
	elsif ($list_aa_upload) {
		die "Can't find entries if list provided!\n" if @action_list;
		return $Catalog->find_autoanal_to_upload(
			year     => $year,
			age      => $min_age,
			maxage   => $max_age,
		);
	}
	
}



sub run_metadata_actions {
	
	# rescan the project file size and age on the server
	if ($scan_size_age) {
		unless (@action_list) {
			die "No list provided to scan filesystem for size and age!\n";
		}
		my $count = 0;
		foreach my $id (@action_list) {
			my $Entry = $Catalog->entry($id) or next;
			my $Project = RepoProject->new($Entry->path) or next;
			my ($size, $age, $aa_age) = $Project->get_size_age;
			$Entry->size($size);
			$Entry->youngest_datestamp($age);
			$count++;
		}
		print " Collected and updated size and age stats for $count entries\n";
	}

	# update the metadata scan date
	if (defined $update_scan_date) {
		my $time;
		if ($update_scan_date =~ /(\d{4}) (\d{2}) (\d{2}) /x) {
			$time = timelocal(0, 0, 12, $3, $2 - 1, $1);
			print " Setting scan date ($update_scan_date) to $time\n";
		}
		elsif ($update_scan_date eq 'now') {
			$time = time;
		}
		elsif ($update_scan_date eq '0') {
			$time = 0;
			print " Clearing scan date\n";
		}
		else {
			die "unrecognizable scan date '$update_scan_date'\n";
		}
		unless (@action_list) {
			die "No list provided to update scan dates!\n";
		}
		my $count = 0;
		foreach my $id (@action_list) {
			my $Entry = $Catalog->entry($id) or next;
			$Entry->scan_datestamp($time);
			$count++;
		}
		print "  updated scan date for $count entries\n";
	}

	# update the metadata upload date
	if (defined $update_upload_date) {
		my $time;
		if ($update_upload_date =~ /(\d{4}) (\d{2}) (\d{2}) /x) {
			$time = timelocal(0, 0, 12, $3, $2 - 1, $1);
			print " Setting upload date ($update_upload_date) to $time\n";
		}
		elsif ($update_upload_date eq 'now') {
			$time = time;
		}
		elsif ($update_upload_date eq '0') {
			$time = 0;
			print " Clearing upload date\n";
		}
		else {
			die "unrecognizable upload date '$update_upload_date'\n";
		}
		unless (@action_list) {
			die "No list provided to update upload dates!\n";
		}
		my $count = 0;
		foreach my $id (@action_list) {
			my $Entry = $Catalog->entry($id) or next;
			$Entry->upload_datestamp($time);
			$count++;
		}
		print "  updated upload date for $count entries\n";
	}

	# update the metadata autoanalysis upload date
	if (defined $update_aa_upload_date) {
		my $time;
		if ($update_aa_upload_date =~ /(\d{4}) (\d{2}) (\d{2}) /x) {
			$time = timelocal(0, 0, 12, $3, $2 - 1, $1);
			print " Setting upload date ($update_aa_upload_date) to $time\n";
		}
		elsif ($update_aa_upload_date eq 'now') {
			$time = time;
		}
		elsif ($update_aa_upload_date eq '0') {
			$time = 0;
			print " Clearing upload date\n";
		}
		else {
			die "unrecognizable upload date '$update_aa_upload_date'\n";
		}
		unless (@action_list) {
			die "No list provided to update upload dates!\n";
		}
		my $count = 0;
		foreach my $id (@action_list) {
			my $Entry = $Catalog->entry($id) or next;
			$Entry->autoanal_up_datestamp($time);
			$count++;
		}
		print "  updated autoanal upload date for $count entries\n";
	}

	# update the metadata hide date
	if (defined $update_hide_date) {
		my $time;
		if ($update_hide_date =~ /(\d{4}) (\d{2}) (\d{2}) /x) {
			$time = timelocal(0, 0, 12, $3, $2 - 1, $1);
			print " Setting hide time ($update_hide_date) to $time\n";
		}
		elsif ($update_hide_date eq 'now') {
			$time = time;
		}
		elsif ($update_hide_date eq '0') {
			$time = 0;
			print " Clearing hide date\n";
		}
		else {
			die "unrecognizable hide date '$update_hide_date'\n";
		}
		unless (@action_list) {
			die "No list provided to update hide dates!\n";
		}
		my $count = 0;
		foreach my $id (@action_list) {
			my $Entry = $Catalog->entry($id) or next;
			$Entry->hidden_datestamp($time);
			$count++;
		}
		print "  updated hide date for $count entries\n";
	}

	# update the metadata deletion date
	if (defined $update_delete_date) {
		my $time;
		if ($update_delete_date =~ /(\d{4}) (\d{2}) (\d{2}) /x) {
			$time = timelocal(0, 0, 12, $3, $2 - 1, $1);
			print " Setting hide date ($update_delete_date) to $time\n";
		}
		elsif ($update_delete_date eq 'now') {
			$time = time;
		}
		elsif ($update_delete_date eq '0') {
			$time = 0;
			print " Clearing delete date\n";
		}
		else {
			die "unrecognizable delete date '$update_delete_date'\n";
		}
		unless (@action_list) {
			die "No list provided to update delete dates!\n";
		}
		my $count = 0;
		foreach my $id (@action_list) {
			my $Entry = $Catalog->entry($id) or next;
			$Entry->deleted_datestamp($time);
			$count++;
		}
		print "  updated deletion date for $count entries\n";
	}

	# update the metadata email date
	if (defined $update_email_date) {
		my $time;
		if ($update_email_date =~ /(\d{4}) (\d{2}) (\d{2}) /x) {
			$time = timelocal(0, 0, 12, $3, $2 - 1, $1);
			print " Setting email date ($update_email_date) to $time\n";
		}
		elsif ($update_email_date eq 'now') {
			$time = time;
		}
		elsif ($update_email_date eq '0') {
			$time = 0;
			print " Clearing email date\n";
		}
		else {
			die "unrecognizable email date '$update_email_date'\n";
		}
		unless (@action_list) {
			die "No list provided to update email dates!\n";
		}
		my $count = 0;
		foreach my $id (@action_list) {
			my $Entry = $Catalog->entry($id) or next;
			$Entry->emailed_datestamp($time);
			$count++;
		}
		print "  updated email date for $count entries\n";
	}

	# update the CORE lab name
	if (defined $update_core_lab) {
		print " Setting CORE Lab name to $update_core_lab\n";
		unless (@action_list) {
			die "No list provided to update division name!\n";
		}
		if ($update_core_lab eq 'none') {
			$update_core_lab = q();
		}
		my $count = 0;
		foreach my $id (@action_list) {
			my $Entry = $Catalog->entry($id) or next;
			$Entry->core_lab($update_core_lab);
			$count++;
		}
		print "  updated division name for $count entries\n";
	}
	
	# update the AWS IAM profile name
	if (defined $update_profile) {
		print " Setting AWS IAM profile name to $update_profile\n";
		unless (@action_list) {
			die "No list provided to update division name!\n";
		}
		if ($update_profile eq 'none') {
			$update_profile = q();
		}
		my $count = 0;
		foreach my $id (@action_list) {
			my $Entry = $Catalog->entry($id) or next;
			$Entry->profile($update_profile);
			$count++;
		}
		print "  updated division name for $count entries\n";
	}
	
	# update the S3 bucket
	if ( defined $update_bucket ) {
		unless (@action_list) {
			die "No list provided to update bucket!\n";
		}
		$update_bucket =~ s|^s3://||;
		$update_bucket =~ s|/$||;
		my $count    = 0;
		my $nocore   = 0;
		my $uploaded = 0;
		my $skipped  = 0;
		foreach my $id (@action_list) {
			my $Entry = $Catalog->entry($id) or next;
			if (not $Entry->core_lab) {
				$nocore++;
				next;
			}
			if ( $Entry->upload_datestamp > 1000 ) {
				$uploaded++;
				next;
			}
			if ($Entry->project_url and not $force) {
				$skipped++;
				next;
			}
			if ( $update_bucket eq 'none' ) {
				$Entry->bucket( q() );
			}
			else {
				$Entry->bucket($update_bucket);
			}
			$count++;
		}
		print "  updated the bucket name for $count entries\n";
		if ($skipped) {
			print "  skipped $skipped entries with existing S3 path (use --force)\n";
		}
		if ($nocore) {
			print "  skipped $nocore entries with no CORE lab assignment\n";
		}
		if ($uploaded) {
			print "  skipped $uploaded entries already uploaded\n";
		}
	}

	# update the S3 prefix
	if (defined $update_prefix) {
		unless (@action_list) {
			die "No list provided to update prefix!\n";
		}
		if (scalar @action_list > 1) {
			print "! Only the first project will have its prefix updated!\n"
		}
		$update_prefix =~ s|/$||;
		my $id = $action_list[0];
		my $Entry = $Catalog->entry($id);
		if ($Entry) {
			if ( not $Entry->core_lab ) {
				print " ! Project $id is not assigned to a CORE lab\n";
			}
			elsif ( $Entry->upload_datestamp > 1000 ) {
				print " ! Project $id has already been uploaded, cannot change\n";
			}
			else {
				# must be ok
				$Entry->prefix($update_prefix);
				print "  updated prefix for $id\n";
			}
		}
		else {
			print " no Catalog entry for '$id'!\n";
		}
	}

	if ($generate_s3_path) {
		print " Generating default bucket/prefix S3 paths\n";
		unless (@action_list) {
			die "No list provided to update division name!\n";
		}
		my $count    = 0;
		my $nocore   = 0;
		my $uploaded = 0;
		my $skipped  = 0;
		foreach my $id (@action_list) {
			my $Entry = $Catalog->entry($id) or next;
			if (not $Entry->core_lab) {
				$nocore++;
				next;
			}
			if ( $Entry->upload_datestamp > 1000 ) {
				$uploaded++;
				next;
			}
			if ($Entry->project_url and not $force) {
				$skipped++;
				next;
			}
			generate_bucket($Entry);
			generate_prefix($Entry);
			$count++;
		}
		print "  generated bucket/prefix default paths for $count entries\n";
		if ($skipped) {
			print "  skipped $skipped entries with existing S3 path (use --force)\n";
		}
		if ($nocore) {
			print "  skipped $nocore entries with no CORE lab assignment\n";
		}
		if ($uploaded) {
			print "  skipped $uploaded entries already uploaded\n";
		}
	}
	
}


sub run_project_actions {

	# delete projects
	if ($delete_entry) {
		print " Deleting projects...\n";
		unless (@action_list) {
			die "No list provided to delete entries!\n";
		}
		foreach my $id (@action_list) {
			if ($Catalog->delete_entry($id) ) {
				print "  deleted $id\n";
			}
			else {
				print "  ! $id not found\n";
			}
		}
	}
	
	# scan the project
	if ($project_scan) {
		print " Scanning projects...\n";
		unless (@action_list) {
			die "No list provided to scan projects!\n";
		}
		my @success;
		foreach my $id (@action_list) {
			
			# generate the command for external utility
			# these will be executed one at a time
			my $command = sprintf "%s/process_project.pl --catalog %s --project %s",
				$Bin, $cat_file, $id;
			if ($verbose) {
				$command .= " --verbose";
			}
			if (defined $project_zip) {
				# special situation to negate writing a zip list file, default is true
				if ($project_zip) {
					$command .= " --zip";   # technically redundant since default true
				}
				else {
					$command .= " --nozip";
				}
			}
			else {
				# determine empirically based on CORE status
				# we should not be generating an ARCHIVE list if we will never use it
				my $Entry = $Catalog->entry($id) or next;
				if ( $Entry->core_lab ) {
					$command .= " --zip";   # technically redundant since default true
				}
				else {
					$command .= " --nozip";
				}
			}
			if ($mock) {
				print " MOCK Executing '$command'\n";
				push @success, $id;
			}
			else {
				print " Executing '$command'\n";
				my $r = system($command);
				if ($r) {
					print " \n FAILURE while scanning $id\n";
				}
				else {
					push @success, $id;
				}
			}
		}
		# reset the action list based on the success of this step
		@action_list = @success;
	}
	
	# zip files
	# technically a directory action, but this needs to be done before uploading
	# and after scanning, so we do it here
	if ($project_zip) {
		print " Zipping project files...\n";
		unless (@action_list) {
			die "No list provided to zip projects!\n";
		}
		my @success;

		# remember current directory, as we will move around
		my $start_dir = getcwd();

		# iterate through projects
		foreach my $id (@action_list) {
			printf "\n > Zipping files for project %s\n", $id;
			
			# Collect project path
			my $Entry = $Catalog->entry($id);
			unless ($Entry) {
				print " ! Identifier $id not in Catalog! skipping\n";
				next;
			}
			my $Project = RepoProject->new($Entry->path, $verbose);
			unless ($Project) { 
				printf  " ! unable to initiate Repository Project for path %s! skipping\n", 
					$Entry->path;
				next;
			}
			unless (chdir $Project->given_dir) {
				printf " ! cannot change to given directory %s! $OS_ERROR\n",
					$Project->given_dir;
				next;
			};

			# Check project and process accordingly
			unless ( $Entry->scan_datestamp ) {
				printf " ! %s has not been scanned\n", $id;
				next;
			}
			unless ( -e $Project->alt_ziplist_file ) {
				printf " ! %s does not have a zip list file\n", $id;
				next;
			}
			if ( $Entry->scan_datestamp < $Entry->youngest_datestamp ) {
				printf " ! %s has newer files since last scan\n", $id;
				next;
			}
			if ( -e $Project->zip_file) {
				printf " ! %s is already archived, %s exists\n", $id, $Project->zip_file;
				next;
			}
			if ($mock) {
				printf " MOCK Zipping %s....\n", $id;
				push @success, $id;
			}
			else {
				my $failure_count = $Project->zip_archive_files;
				if ($failure_count) {
					printf "  ! Project %s had %d zip failures\n", $id, $failure_count;
				}
				else {
					printf "  > Completed project %s successfully\n", $id;
					push @success, $id;
				}
			}
		}

		# move back
		chdir $start_dir;

		# this action should already be completed, so set to false
		$move_zip_files = 0;

		# reset the action list based on the success of this step
		@action_list = @success;
	}

	# upload the project
	if ( $project_upload or $project_aa_upload ) {
		print " Preparing projects for upload...\n";
		unless (@action_list) {
			die "No list provided to update division name!\n";
		}

		# prepare valid project upload list
		my @upload_list;
		foreach my $id (@action_list) {
			my $Entry = $Catalog->entry($id);
			unless ($Entry) {
				print " ! Identifier $id not in Catalog! skipping\n";
				next;
			}
			unless ($Entry->core_lab) {
				print " ! $id is not assigned to a CORE lab\n";
				next;
			}
			unless ($Entry->profile) {
				print " ! $id does not have an AWS profile\n";
				next;
			}
			unless ($Entry->bucket) {
				print " ! $id is not assigned a destination bucket\n";
				next;
			}
			unless ($Entry->prefix) {
				print " ! $id is not assigned a destination prefix\n";
				next;
			}
			unless ($Entry->scan_datestamp) {
				print " ! $id has not been scanned yet\n";
				next;
			}
			unless ( $Entry->scan_datestamp > $Entry->youngest_datestamp ) {
				# this is not a fool proof test without initiating a real scan, 
				# but it's better than nothing
				# go ahead and proceed with a warning
				print " ! $id has new files since last scan\n";
			}

			push @upload_list, $id;
		}
		printf " > %d projects to upload: %s\n", scalar @upload_list,
			join(', ', @upload_list);

		# generate base command
		my $base_command = sprintf "%s/upload_repo_projects.pl --catalog %s", $Bin,
			$cat_file;
		if ($project_aa_upload) {
			$base_command .= ' --aa';
		}
		if ($forks) {
			$base_command .= sprintf " --forks %d", $forks;
		}
		if ($verbose) {
			$base_command .= ' --verbose';
		}
		if ($mock) {
			# Not the actual mock command, which goes through the entire forking
			# and dryrun aws s3 commands. Instead just checks bucket and counts
			# number of files to upload, which is what we really want here. 
			$base_command .= ' --check';
		}

		# run upload tool
		my @success;
		foreach my $id (@upload_list) {
			my $command = sprintf "%s --project %s", $base_command, $id;
			print "\n Executing '$command'\n";
			my $r = system($command);
			if ($r) {
				print " \n ! Incomplete or no uploads of $id\n";
			}
			else {
				push @success, $id;
			}
		}
		# reset the action list based on the success of this step
		@action_list = @success;
	}
}



sub run_project_directory_actions {
	
	# Run any number of project management functions
	# we group all of these together for convenience
	return unless (
		$move_zip_files or $move_del_files or $unhide_zip_files or $unhide_del_files or 
		$restore_zip_files or $delete_zip_files or $delete_del_files or $add_notice or 
		$clean_project_files);
	
	# remember current directory, as we will move around
	my $current_dir = getcwd();
	
	# we will do all given functions at once for each project
	my @success;
	foreach my $id (@action_list) {
		
		# Collect project path
		my $Entry = $Catalog->entry($id);
		unless ($Entry) {
			print " ! Identifier $id not in Catalog! skipping\n";
			next;
		}
		my $Project = RepoProject->new($Entry->path, $verbose);
		unless ($Project) { 
			printf  " ! unable to initiate Repository Project for path %s! skipping\n", 
				$Entry->path;
			next;
		}
		
		# Process accordingly
		printf " > working on %s at %s\n", $Project->project, $Project->given_dir;
		my $failure_count = 0;

		# change to the given directory
		unless (chdir $Project->given_dir) {
			print "cannot change to given directory! $OS_ERROR\n";
			next;
		};

		# hide the zipped files
		if ($move_zip_files) {
			if (-e $Project->ziplist_file) {
				printf " > hiding %s zipped files to %s\n", $Project->project, 
					$Project->zip_folder;
				$failure_count += $Project->hide_zipped_files;
			}
			else {
				printf " ! %s has no zipped files to move\n", $Project->project;
			}
		} 

		# unhide files
		if ($unhide_zip_files) {
			if (-e $Project->zip_folder) {
				printf " > unhiding %s zipped files from directory %s to %s\n",
					$Project->project, $Project->zip_folder, $Project->given_dir;
				$failure_count += $Project->unhide_zip_files;
			}
			else {
				printf " ! Zip folder %s doesn't exist!\n", $Project->zip_folder;
			}
		}

		# restore hidden files
		if ($unhide_del_files) {
			if (-e $Project->delete_folder) {
				printf " > unhiding %s deleted files from directory %s to %s\n",
					$Project->project, $Project->delete_folder, $Project->given_dir;
				my $failure = $Project->unhide_deleted_files;
				if (not $failure) {
					if ($Entry->hidden_datestamp) {
						$Entry->hidden_datestamp(0);
						print "    updated catalog hidden timestamp\n";
					}
				}
				$failure_count += $failure;
			}
			else {
				printf " ! Deleted folder %s doesn't exist!\n", $Project->delete_folder;
			}
		}

		# delete zipped files
		if ($delete_zip_files) {
			if (-e $Project->zip_folder) {
				printf " > deleting %s zipped files in %s\n", $Project->project, 
					$Project->zip_folder;
				$failure_count += $Project->delete_zipped_files_folder;
			}
			else {
				printf " ! no hidden zip folder for %s to delete\n", 
					$Project->project;
			}
		} 


		# restore the zip archive
		if ($restore_zip_files) {
			if (-e $Project->zip_file) {
				if (-e $Project->zip_folder) {
					printf " ! %s hidden zip folder %s exists!\n", $Project->project, 
						$Project->zip_folder;
					print  "    Restore from the zip folder\n";
					$failure_count++;
				}
				else {
					printf " > Restoring %s files from zip archive %s\n", $Project->project,
						$Project->zip_file;
					chdir $Project->given_dir;
					my $command = sprintf "unzip -n %s", $Project->zip_file;
					print  "  > executing: $command\n";
					my $result = system($command);
					if ($result) {
						print "     failed!\n";
						$failure_count++;
					}
				}
			}
			else {
				printf " ! %s Zip archive not present!\n", $Project->project;
			}
		}


		# move the deleted files
		if ($move_del_files) {
			if (-e $Project->alt_remove_file) {
				printf " > hiding %s deleted files to %s\n", $Project->project, 
					$Project->delete_folder;
				my $failure = $Project->hide_deleted_files;
				if (not $failure) {
					$Entry->hidden_datestamp(time);
					print "    updated catalog hidden timestamp\n";
				}
				$failure_count += $failure;
			}
			else {
				printf " ! %s has no deleted files to hide!\n", $Project->project;
			}
		} 


		# actually delete the files
		if ($delete_del_files) {
			if (-e $Project->delete_folder and -e $Project->remove_file) {
				printf " > deleting %s files in hidden delete folder %s\n", $Project->project, 
					$Project->delete_folder;
				my $failure = $Project->delete_hidden_deleted_files();
				if (not $failure) {
					$Entry->deleted_datestamp(time);
					print "    updated catalog deleted timestamp\n";
				}
				$failure_count += $failure;
			}
			else  {
				printf " > deleting %s files in %s\n", $Project->project, $Project->given_dir;
				my $failure = $Project->delete_project_files();
				if (not $failure) {
					$Entry->deleted_datestamp(time);
					print "    updated catalog deleted timestamp\n";
				}
				$failure_count += $failure;
			}
			if (-e $Project->zip_folder) {
				print " ! Hidden zip folder also exists, use option --del_zip\n";
			}
		}


		# add notice file
		if ($add_notice) {
			printf " > Linking notice in %s\n", $Project->project;
			$failure_count += $Project->add_notice_file;
		}


		# clean project files
		if ($clean_project_files) {
			printf " > Cleaning %s project files\n", $Project->project;

			# zip archive
			if (-e $Project->zip_file) {
				unlink $Project->zip_file;
				printf "  ! Deleted %s\n", $Project->zip_file;
			}

			# zip list
			if (-e $Project->zip_folder) {
				printf "  ! Zip folder still exists! Not removing %s\n", $Project->ziplist_file;
				$failure_count++;
			}
			elsif (-e $Project->alt_ziplist_file) {
				unlink $Project->alt_ziplist_file;
				printf "  Deleted %s\n", $Project->alt_ziplist_file;
			}
			elsif (-e $Project->ziplist_file) {
				unlink $Project->ziplist_file;
				printf "  Deleted %s\n", $Project->ziplist_file;
			}

			# remove lists
			if (-e $Project->delete_folder) {
				printf "  ! Hidden delete folder still exists! Not removing %s\n", $Project->remove_file;
				$failure_count++;
			}
			elsif (-e $Project->remove_file) {
				unlink $Project->remove_file;
				printf "  Deleted %s\n", $Project->remove_file;
			}
			if (-e $Project->alt_remove_file) {
				unlink $Project->alt_remove_file;
				printf "  Deleted %s\n", $Project->alt_remove_file;
			}

			# manifest
			if (-e $Project->manifest_file) {
				unlink $Project->manifest_file;
				printf "  Deleted %s\n", $Project->manifest_file;
				$Entry->scan_datestamp(0);
			}
		}


		######## Finished
		if ($failure_count) {
			printf " ! finished with %s with %d failures\n\n", $id,
				$failure_count;
		}
		else {
			printf " > finished with %s with 0 failures\n\n", $id;
			push @success, $id
		}

	}
	
	# change back
	chdir $current_dir;

	# reset the action list based on the success of this step
	@action_list = @success;
}



sub run_email_notifications {
	
	# Initialize the emailer object as necessary
	my $Email;
	if ($email_anal_del or $email_anal_up or $email_req_del or $email_req_up) {
		eval {
			require Emailer;
			$Email = Emailer->new();
		};
		unless ($Email) {
			die " Unable to initialize Emailer! $EVAL_ERROR\n";
		}
	}
	else {
		return;
	}
	
	# request scheduled deletion notification 
	if ($email_req_del) {
		foreach my $id (@action_list) {
			my $Entry = $Catalog->entry($id) or next;
		
			my $result = $Email->send_request_deletion_email($Entry, 'mock' => $mock);
			if ($result) {
				printf " > Sent Request deletion email for $id: %s\n", 
					ref($result) ? $result->message : "\n$result";
				$Entry->emailed_datestamp(time) if not $mock;
			}
			else {
				print " ! Failed Request deletion email for $id\n";
			}
		}
	}
	
	# request upload notification
	if ($email_req_up) {
		foreach my $id (@action_list) {
			my $Entry = $Catalog->entry($id) or next;
		
			my $result = $Email->send_request_upload_email($Entry, 'mock' => $mock);
			if ($result) {
				printf " > Sent Request AWS upload email for $id: %s\n", 
					ref($result) ? $result->message : "\n$result";
				$Entry->emailed_datestamp(time) if not $mock;
			}
			else {
				print " ! Failed Request upload email for $id\n";
			}
		}
	}
	
	# analysis deletion notification
	if ($email_anal_del) {
		foreach my $id (@action_list) {
			my $Entry = $Catalog->entry($id) or next;
		
			my $result = $Email->send_analysis_deletion_email($Entry, 'mock' => $mock);
			if ($result) {
				printf " > Sent Analysis deletion email for $id: %s\n", 
					ref($result) ? $result->message : "\n$result";
				$Entry->emailed_datestamp(time) if not $mock;
			}
			else {
				print " ! Failed Analysis deletion email for $id\n";
			}
		}
	}
	
	# analysis upload notification
	if ($email_anal_up) {
		foreach my $id (@action_list) {
			my $Entry = $Catalog->entry($id) or next;
		
			my $result = $Email->send_analysis_upload_email($Entry, 'mock' => $mock);
			if ($result) {
				printf " > Sent Analysis AWS upload email for $id: %s\n", 
					ref($result) ? $result->message : "\n$result";
				$Entry->emailed_datestamp(time) if not $mock;
			}
			else {
				print " ! Failed Analysis upload email for $id\n";
			}
		}
	}

}



sub print_functions {
	# Various reporting and printing functions
	# only one is allowed
	
	# Print project status to screen
	if ($show_status) {
		unless (@action_list) {
			die "No list provided to show status!\n";
		}


		# we have 67 print characters plus 7 tabs
		printf "%-6s\t%-6s\t%-5s\t%-10s\t%-10s\t%-10s\t%-10s\t%-10s\n",
			qw(ID Size Age Scan Upload Hide Delete CORELab);
		# loop through items
		foreach my $id (@action_list) {
			my $Entry = $Catalog->entry($id) or next;
		
			# get and format the current size to human readable sizes
			# we know how big the project was
			my $size = $Entry->size || 0;
			if ( $biggest_size and $Entry->last_size > $size ) {
				# take whatever was largest, previous or current size
				# the previous size may not be a guarantee of the biggest size
				$size = $Entry->last_size;
			}
			$size = _format_size($size);
		
			# the datetime stamps are converted from epoch to YYYY-MM-DD
			# if not set, just prints blanks
			# since I'm using local time and not gm time, funny things happen regarding 
			# the year when I try to set it to epoch time (0). I get back either year 1969 or 
			# 1970, so I need to check both.
		
			# scan day
			my @scan   = localtime($Entry->scan_datestamp || 0);
			my $scan_day = ($scan[5] == 69 or $scan[5] == 70) ? q(          ) : 
				sprintf("%04d-%02d-%02d", $scan[5] + 1900, $scan[4] + 1, $scan[3]);
			# upload day
			my @upload = localtime($Entry->upload_datestamp || 0);
			my $up_day = ($upload[5] == 69 or $upload[5] == 70) ? q(          ) : 
				sprintf("%04d-%02d-%02d", $upload[5] + 1900, $upload[4] + 1, $upload[3]);
			# hide day
			my @hide   = localtime($Entry->hidden_datestamp || 0);
			my $hide_day = ($hide[5] == 69 or $hide[5] == 70) ? q(          ) : 
				sprintf("%04d-%02d-%02d", $hide[5] + 1900, $hide[4] + 1, $hide[3]);
			# delete day
			my @delete = localtime($Entry->deleted_datestamp || 0);
			my $delete_day = ($delete[5] == 69 or $delete[5] == 70) ? q(          ) : 
				sprintf("%04d-%02d-%02d", $delete[5] + 1900, $delete[4] + 1, $delete[3]);
			
			# division
			my $division = 'none'; # default
			my $ext = $Entry->external; # default is undefined
			if (defined $ext) {
				$division = $ext eq 'Y' ? 'external' : $Entry->core_lab || 'none';
			}
			
			# age
			my $age = $Entry->age;
			unless (defined $age) {
				$age = q(?);
			}
			
			# print
			printf "%-6s\t%-7s\t%-5s\t%s\t%s\t%s\t%s\t%s\n", $id, $size, 
				$age, $scan_day, $up_day, $hide_day, $delete_day, $division;
		}
	}

	# print AutoAnalysis stats
	elsif ($show_aa_status) {
		unless (@action_list) {
			die "No list provided to show status!\n";
		}
		# print header, we have 67 characters plus 6 tabs
		printf "%-6s\t%-6s\t%-5s\t%-10s\t%-10s\t%-20s\t%-10s\n", 
			qw(ID Size Age Scan AAUpload AAFolder CORELab);
		# loop through items
		foreach my $id (@action_list) {
			my $Entry = $Catalog->entry($id) or next;
		
			# format size, use last given size instead of current size, so that we
			# we know how big the project was
			# this is not specific to the AutoAnalysis folder
			my $size = $Entry->size || 0;
			if ( $biggest_size and $Entry->last_size > $size ) {
				# take whatever was largest, previous or current size
				# the previous size may not be a guarantee of the biggest size
				$size = $Entry->last_size;
			}
			$size = _format_size($size);

			# scan day
			my $scan_day = q(          );
			if ( $Entry->scan_datestamp > 1 ) {
				my @scan = localtime($Entry->scan_datestamp);
				$scan_day = sprintf("%04d-%02d-%02d", $scan[5] + 1900, $scan[4] + 1,
					$scan[3]);
			}

			# upload day
			my $up_day = q(          );
			if ( $Entry->autoanal_up_datestamp > 1 ) {
				my @scan = localtime($Entry->autoanal_up_datestamp);
				$up_day = sprintf("%04d-%02d-%02d", $scan[5] + 1900, $scan[4] + 1,
					$scan[3]);
			}

			# division
			my $division = 'none'; # default
			my $ext = $Entry->external; # default is undefined
			if (defined $ext) {
				$division = $ext eq 'Y' ? 'external' : $Entry->core_lab || 'none';
			}

			# print
			printf "%-6s\t%-6s\t%-5s\t%-10s\t%-10s\t%-20s\t%-10s\n", $id, $size,
				$Entry->age || q(?), $scan_day, $up_day, $Entry->autoanal_folder,
				$division;
		}
	}
	
	# print generic information to screen
	elsif ($show_info) {
		unless (@action_list) {
			die "No list provided to show status!\n";
		}
		printf "%-6s\t%-10s\t%-16s\t%-16s\t%s\n", qw(ID Date UserName LabName Name);
		foreach my $id (@action_list) {
			my $Entry = $Catalog->entry($id) or next;
		
			printf "%-6s\t%-10s\t%-16s\t%-16s\t%s\n", 
				$id, 
				$Entry->date, 
				substr(sprintf("%s,%s", $Entry->user_last, $Entry->user_first),0,16),
				substr(sprintf("%s,%s", $Entry->lab_last, $Entry->lab_first),0,16),
				$Entry->name;
		}
	}
	
	# print the Repository file path
	elsif ($show_path) {
		unless (@action_list) {
			die "No list provided to show status!\n";
		}
		foreach my $id (@action_list) {
			my $Entry = $Catalog->entry($id) or next;
		
			printf "%s\n", $Entry->path;
		}
	}
	
	# print the remote AWS S3 URI
	elsif ($show_url) {
		unless (@action_list) {
			die "No list provided to show URLs!\n";
		}
		my $missing = 0;
		foreach my $id (@action_list) {
			my $Entry = $Catalog->entry($id) or next;
			my $url = $Entry->project_url;
			if ($url) {
				printf "%s\t%s\n", $id, $url;
			}
			else {
				printf "%s\t\n", $id;
				$missing++;
			}
		}
		if ($missing) {
			printf STDERR " ! There were %d projects without URLs\n", $missing;
		}
	}
	
	# print everything to screen
	elsif ($print_info) {
		unless (@action_list) {
			die "No list provided to show status!\n";
		}
		my $header = RepoCatalog->header;
		print $header;
		foreach my $id (@action_list) {
			my $Entry = $Catalog->entry($id) or next;
			print($Entry->print_string($transform));
		}
	
	}
	
	# just print identifiers to screen
	elsif (
		# check to see if we have a list of IDs that were from a search
		@action_list and
		(
			$list_req_upload or $list_req_hide or $list_req_delete or
			$list_anal_upload or $list_anal_hide or $list_anal_delete or 
		 	$list_pi or $list_aa_upload or $list_aa or
		 	( ( $list_all or $list_req or $list_anal ) and 
		 		($year or $min_age or $max_age or $min_size or $external)
			)
		)
	) {
		# just print them
		printf "%s\n", join("\n", @action_list);
	}
}


sub final_catalog_functions {
	
	if ($dump_file) {
		my $s = $Catalog->export_to_file($dump_file, $transform);
		if ($s) {
			print " Exported to file '$dump_file'\n";
		}
		else {
			print " Export to file '$dump_file' failed!\n";
		}
	}

	if ($run_optimize) {
		print " Optimizing...\n";
		$Catalog->optimize;
	}
}


sub _format_size {
	my $size = shift;
	# using binary sizes here
	if ($size > 1099511627776) {
		return sprintf("%.1fT", $size / 1099511627776);
	}
	elsif ($size > 1073741824) {
		return sprintf("%.1fG", $size / 1073741824);
	}
	elsif ($size > 1048576) {
		return sprintf("%.1fM", $size / 1048576);
	}
	elsif ($size > 1000) {
		# avoid weird formatting situations of >1000 and < 1024 bytes
		return sprintf("%.1fK", $size / 1024);
	}
	else {
		return sprintf("%dB", $size);
	}
}
