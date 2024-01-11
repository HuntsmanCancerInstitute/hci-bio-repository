# HCI-Bio-Repository

These are scripts for working with the HCI [GNomEx](https://hci-bio-app.hci.utah.edu/gnomex/home) 
Bio-Repository file server, particularly with respect in preparation and migration of 
GNomEx data to AWS Buckets (and formerly Seven Bridges).

Main scripts are in the `bin` directory. Modules required for execution are in the `lib` 
directory. Old stuff are placed in the `deprecated` directory for posterity only.

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

- `process_analysis_project.pl`

Script for processing GNomEx Analysis project folders. Identifies most common 
bioinformatic file types, classifies them, and collects file size, date stamp, and MD5 
checksum attributes, and writes these to a project `MANIFEST.csv` file. Optionally 
gzip compress large text files or archive into a single bulk Zip archive for data 
storage efficiency. 

- `process_request_project.pl`

Script for processing GNomEx Experiment Request project folders. Collects essential 
sequencing metadata from Fastq files, including sample ID, platform, lane, paired-end 
status, and MD5 checksum. Metadata is written to a `MANIFEST.csv` file. 

- `upload_repo_projects.pl`

Manifest-based, multi-threaded, file uploader of GNomEx repository projects to AWS
buckets. 

- Seven Bridges scripts

There are multiple scripts for working with the 
[Seven Bridges](https://www.sevenbridges.com) platform (now part of
[Velsera](https://velsera.com/)), and the
[Cancer Genomics Cloud](https://www.cancergenomicscloud.org) platform. These include 
`sbg_project_manager.pl` (an extensive file and project manager for listing, moving, 
and exporting files), `sbg_vol_manager.pl` (manager for mounted AWS buckets),
`sbg_async_folder_copy.pl` (bulk, recursive, asynchronous file copy within or between 
projects), `check_divisions.pl` (list projects, members, tasks, and more), and others.
These are mostly retired.

- other

There are a few other sundry scripts for data migration, various one-off purposes,
and such. These also include scripts for the bulk migration from Seven Bridges to
AWS buckets. 


### Modules

There are specific Perl library modules for working with the Catalog database 
file, Repository project folders on the file server, and GNomEx. These are fairly 
specific to the scripts here and unlikely of general interest.


### Deprecated Stuff

These are bunch of old, deprecated scripts. See its dedicated page for more information.


## Requirements

The scripts are written in Perl and can be executed under the system Perl. Additional 
Perl modules are required for execution. These may be installed with a Perl package 
manager, such as [CPANminus](https://metacpan.org/pod/App::cpanminus) or CPAN, using
the included `cpanfile`.

	cpanm --installdeps .

The [Net::SB](https://github.com/tjparnell/Net-SB) library module is the Perl wrapper 
around the Seven Bridges API. The code was originally part of this package but split 
off to make it more generally accessible. It is currently not on CPAN and must be 
installed manually.


### Compiling single executables

To ease distribution to other servers, single file executables can be generated using
[PAR-Packer](https://metacpan.org/pod/pp). Note that this is not perfect, and may not 
work across different systems, e.g. Linux distributions.

    pp -c -o executables/script bin/script.pl

For some scripts that use runtime loader modules, you will have to actually execute 
the script with real arguments (no, simple `--help` doesn’t cut it).

    pp -x --xargs " " -o executables/script bin/script.pl


# License

This package is free software; you can redistribute it and/or modify
it under the terms of the Artistic License 2.0.  

	 Timothy J. Parnell, PhD
	 Bioinformatic Analysis Shared Resource
	 Huntsman Cancer Institute
	 University of Utah
	 Salt Lake City, UT, 84112




