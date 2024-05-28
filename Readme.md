# HCI-Bio-Repository

These are scripts for working with the HCI [GNomEx](https://hci-bio-app.hci.utah.edu/gnomex/home) 
Bio-Repository file server, particularly with respect in preparation and migration of 
GNomEx data to AWS Buckets (and formerly Seven Bridges).

Main scripts are in the `bin` directory. Modules required for execution are in the `lib` 
directory. 

Current operation is predicated around collecting disparate bits of information, 
including metadata from the GNomEx database, metadata for each GNomEx project directory 
on the bio-repository file server, collated information regarding known AWS lab 
divisions, and metadata regarding actions performed on GNomEx projects, all coalesced 
into a Catalog database file. Actions on the GNomEx projects take place through, and 
recorded into, the Catalog file. 


## Contents

### Scripts

Main scripts in the `bin` folder. 

- `manage_repository.pl`

The primary script for working with the GNomEx repository projects and file directories.
This is a comprehensive application for cataloging, managing, processing, and uploading
GNomEx projects. All functions revolve around a local Catalog database file.

- `process_project.pl`

General unified script for processing both GNomEx Experiment Request and Analysis 
projects in the repository. Writes a unified structure project `MANIFEST.csv` file
with the metadata of the files, including file type, size, date stamp, MD5 checksum,
and sequencing information (for Fastq files only). Writes zip archive and remove file
list files in parent directory and hidden from the GNomEx web application.

- `upload_repo_projects.pl`

Manifest-based, multi-threaded, file uploader of GNomEx repository projects to AWS
buckets. 

- other

There are a few other sundry scripts for various one-off purposes and such. 

### Special folders

Special purpose scripts and modules, mostly retired, are moved into separate folders
for sanity purposes.

- Seven Bridges

There are multiple scripts for working with the 
[Seven Bridges](https://www.sevenbridges.com) platform (now part of
[Velsera](https://velsera.com/)), and the
[Cancer Genomics Cloud](https://www.cancergenomicscloud.org) platform. These include 
`sbg_project_manager.pl` (an extensive file and project manager for listing, moving, 
and exporting files), `sbg_vol_manager.pl` (manager for mounted AWS buckets),
`sbg_async_folder_copy.pl` (bulk, recursive, asynchronous file copy within or between 
projects), `check_divisions.pl` (list projects, members, tasks, and more), and others.
These are mostly retired. These are in the [sevenbridges](sevenbridges) directory.

Perl libraries that wrap around the Seven Bridges API have been moved to a private 
repository, [Net::SB](https://github.com/tjparnell/Net-SB). The code was originally
part of this package but split off to make it more generally accessible. It is 
currently not on CPAN and must be installed manually.

- Migration

Several scripts used in the Great Data Migration from the Seven Bridges platform to 
private AWS buckets. These include `generate_project_export_prefixes.pl` for automatic
generation of destination buckets and prefixes for manual inspection,
`select_sbg_project_files_to_delete.pl` for finding and removing unwanted files from 
Seven Bridges projects, `generate_bulk_export_commands.pl` for generating the actual 
shell scripts for managing and executing the transfer, and `verify_transfers.pl` for
directly comparing the paths and sizes between Seven Bridges projects and AWS buckets
for confirmation of successful file transfer. These are in the [migration](migration)
directory.

- Deprecation

Old scripts and stuff are placed in the [deprecated](deprecated/Readme.md) directory for
posterity only. See its dedicated page for more information.


### Modules

There are specific Perl library modules for working with the Catalog database 
file, Repository project folders on the file server, and GNomEx. These are fairly 
specific to the scripts here and unlikely to be of general interest. 

**NOTE**: These are not installed by a package manager. All of the applications in the
`script` folder will look for the `lib` folder in the parent directory. Downloading
the package as-is and running in place should work. Otherwise, they can be manually
placed in your `PERL5LIB` path.


## Requirements

The scripts are written in Perl and can be executed under the system Perl. Additional 
Perl modules are required for execution. These may be installed with a Perl package 
manager, such as [CPANminus](https://metacpan.org/pod/App::cpanminus) or CPAN, using
the included `cpanfile`.

	cpanm --installdeps .

The [Net::SB](https://github.com/tjparnell/Net-SB) module is not currently on CPAN and
must be installed manually.


### Compiling single executables

To ease distribution to other servers, single file executables can be generated using
[PAR-Packer](https://metacpan.org/pod/pp). Note that this is not perfect, and may not 
work across different systems, e.g. Linux distributions.

    pp -c -o executables/script bin/script.pl

For some scripts that use runtime loader modules, you will have to actually execute 
the script with real arguments (no, simple `--help` won’t cut it).

    pp -x --xargs " --option foo --bar" -o executables/script bin/script.pl


# License

This package is free software; you can redistribute it and/or modify
it under the terms of the Artistic License 2.0.  

	 Timothy J. Parnell, PhD
	 Bioinformatic Analysis Shared Resource
	 Huntsman Cancer Institute
	 University of Utah
	 Salt Lake City, UT, 84112




