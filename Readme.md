# HCI-Bio-Repository

These are scripts for working with the HCI [GNomEx](https://hci-bio-app.hci.utah.edu/gnomex/home) 
Bio-Repository file server, particularly with respect in preparation and migration of 
GNomEx data to [Seven Bridges](https://www.sevenbridges.com). 

**NOTE:** Most of the scripts are specific to HCI infrastructure and might not be 
applicable elsewhere. The possible exception is the `SB2` Perl module for interacting 
with the Seven Bridges platform using the 

Main scripts are in the `bin` directory. Modules required for execution are in the `lib` 
directory. Old stuff are placed in the `deprecated` directory for posterity only.

Current operation is predicated around collecting disparate bits of information, 
including metadata from the GNomEx database, metadata for each GNomEx project directory 
on the bio-repository file server, collated information regarding known Seven Bridges lab 
divisions, and metadata regarding actions performed on GNomEx projects, all coalesced 
into a Catalog database file. Actions on the GNomEx projects take place through, and 
recorded into, the Catalog file. 


## Modules

General API Perl modules for use in the accompanying scripts.

- `SB2`

A general purpose Perl API for interacting with the 
[Seven Bridges RESTful API](https://docs.sevenbridges.com/page/api). This is by no 
means complete coverage, but is sufficient for purposes of this project.

- `RepoCatalog`

The main API module for working with the Catalog database file.

- `RepoProject`

A module providing common code functions for working with Repository project directory 
folders on the bio-repository file server.

- `Gnomex`

A HCI-specific interface for querying the GNomEx database.

- `Emailer`

A module for sending out form email notifications to GNomEx users. The content of the 
email forms are embedded in this module.


## Scripts

Main scripts in the `bin` folder. 

- `manage_repository.pl`

The main script for working with the repository Catalog and GNomEx projects and file 
directories. All actions go through here.

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

- `add_user_sb_project.pl`

A simple script for quickly adding a Seven Bridges division member to an existing project. 

- `check_divisions.pl`

A simple script to check one or all divisions and print the number of members and 
projects.


## Deprecated Stuff

These are bunch of old, deprecated scripts. See it's dedicated page for more information.


## Requirements

The scripts are written in Perl and can be executed under the system Perl. Some 
additional Perl modules are required for execution. These should be readily available 
through the system package manager (`yum` or equivalent) or directly from CPAN. 


- [DBI](https://metacpan.org/pod/DBI)

- [DBI::ODBC](https://metacpan.org/pod/DBI::ODBC)

- [DBM::Deep](https://metacpan.org/pod/DBM::Deep)

- [Digest::MD5](https://metacpan.org/pod/Digest::MD5)

- [Email::Sender::Simple](https://metacpan.org/pod/Email::Sender::Simple)

- [Email::Simple](https://metacpan.org/pod/Email::Simple)

- [HTTP::Tiny](https://metacpan.org/pod/HTTP::Tiny)

- [JSON::PP](https://metacpan.org/pod/JSON::PP)

- [SB upload Java utility](https://docs.sevenbridges.com/docs/upload-via-the-command-line) 


In addition, you will need to generate your 
[Seven Bridges credentials file](https://docs.sevenbridges.com/docs/store-credentials-to-access-seven-bridges-client-applications-and-libraries) 
with developer tokens for each of your divisions.


# License

This package is free software; you can redistribute it and/or modify
it under the terms of the Artistic License 2.0.  

	 Timothy J. Parnell, PhD
	 Bioinformatic Analysis Shared Resource
	 Huntsman Cancer Institute
	 University of Utah
	 Salt Lake City, UT, 84112




