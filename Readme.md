# HCI-Bio-Repository

These are scripts for working with the HCI [GNomEx](https://hci-bio-app.hci.utah.edu/gnomex/home) 
Bio-Repository file server, particularly with respect in preparation and migration of 
GNomEx data to [Seven Bridges](https://www.sevenbridges.com). 

**NOTE:** Most of the scripts are specific to HCI infrastructure and likely not
applicable elsewhere, with the notable exception of 
[sbg_project_manager](https://github.com/HuntsmanCancerInstitute/hci-bio-repository/blob/master/bin/sbg_project_manager.pl).

Main scripts are in the `bin` directory. Modules required for execution are in the `lib` 
directory. Old stuff are placed in the `deprecated` directory for posterity only.

Current operation is predicated around collecting disparate bits of information, 
including metadata from the GNomEx database, metadata for each GNomEx project directory 
on the bio-repository file server, collated information regarding known Seven Bridges lab 
divisions, and metadata regarding actions performed on GNomEx projects, all coalesced 
into a Catalog database file. Actions on the GNomEx projects take place through, and 
recorded into, the Catalog file. 


## Scripts

Main scripts in the `bin` folder. 

- `manage_repository.pl`

The primary script for working with the repository Catalog database and GNomEx projects 
and file directories. This is a comprehensive application for indexing, managing, and 
processing GNomEx projects. All functions revolve around the Catalog database file.

- `process_analysis_project.pl`

Script for processing GNomEx Analysis project folders. Identifies most common 
bioinformatic file types, classifies them, and collects file size, date stamp, and MD5 
checksum attributes, and writes these to a project `MANIFEST.csv` file. Optionally 
gzip compress large text files or archive into a single bulk Zip archive for data 
storage efficiency. Optionally will handle creating a corresponding Seven Bridges project 
and uploading the folder contents to it.

- `process_request_project.pl`

Script for processing GNomEx Experiment Request project folders. Collects essential 
sequencing metadata from Fastq files, including sample ID, platform, lane, paired-end 
status, and MD5 checksum. Metadata is written to a `MANIFEST.csv` file. Optionally 
will handle creating a corresponding Seven Bridges project and uploading the folder 
contents to it.

- `sbg_project_manager.pl`

A comprehensive script application for working with files and folders on the Seven 
Bridges platform. Unlike their simple `sb` command line tool, this will exhaustively 
recurse through a project when performing file tasks. Tasks include listing, filtering, 
generating download URLs, deleting, and exporting files. 

- `add_user_sb_project.pl`

A simple script for quickly adding a Seven Bridges division member to an existing project. 

- `check_divisions.pl`

A simple script to check one or all divisions and print the number of members and 
projects.

- other

There are a few other sundry scripts for various one-off purposes and such.


## Modules

There are a few specific Perl library modules for working with the Catalog database 
file, Repository project folders on the file server, and GNomEx. These are fairly 
specific to the scripts here and unlikely of general interest.


## Deprecated Stuff

These are bunch of old, deprecated scripts. See it's dedicated page for more information.


## Requirements

The scripts are written in Perl and can be executed under the system Perl. Some 
additional Perl modules are required for execution. These should be readily available 
through the system package manager (`yum` or equivalent) or Perl package manager, such 
as [CPANminus](https://metacpan.org/pod/App::cpanminus) or CPAN. 

The [Net::SB](https://github.com/tjparnell/Net-SB) library module is the Perl wrapper 
around the Seven Bridges API. The code was originally part of this package but split 
off to make it more generally accessible.

Likely need to be installed:

- [DBD::ODBC](https://metacpan.org/pod/DBD::ODBC)

- [DBM::Deep](https://metacpan.org/pod/DBM::Deep)

- [Email::Sender](https://metacpan.org/pod/Email::Sender)

- [Email::Simple](https://metacpan.org/pod/Email::Simple)

- [IO::Socket::SSL](https://metacpan.org/pod/IO::Socket::SSL)

Likely standard with your Perl installation:

- [Digest::MD5](https://metacpan.org/pod/Digest::MD5)

- [HTTP::Tiny](https://metacpan.org/pod/HTTP::Tiny)

- [JSON::PP](https://metacpan.org/pod/JSON::PP)


In addition, you will need to generate your 
[Seven Bridges credentials file](https://docs.sevenbridges.com/docs/store-credentials-to-access-seven-bridges-client-applications-and-libraries) 
with developer tokens for each of your divisions. The scripts make an assumption that 
the profile and division name are the same.

On a CentOS system, you can install dependencies as 

    yum install perl-JSON perl-JSON-PP perl-Email-Simple perl-Email-Sender perl-IO-Socket-SSL perl-HTTP-Tiny perl-DBM-Deep perl-DBD-ODBC



# License

This package is free software; you can redistribute it and/or modify
it under the terms of the Artistic License 2.0.  

	 Timothy J. Parnell, PhD
	 Bioinformatic Analysis Shared Resource
	 Huntsman Cancer Institute
	 University of Utah
	 Salt Lake City, UT, 84112




