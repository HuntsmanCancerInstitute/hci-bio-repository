# HCI-Bio-Repository

These are scripts for working with the HCI [GNomEx](https://hci-bio-app.hci.utah.edu/gnomex/home) 
Bio-Repository file server, particularly with respect in preparation and migration of 
GNomEx data to [Seven Bridges](https://www.sevenbridges.com). 

**NOTE:** Most of the scripts are specific to HCI infrastructure and might not be 
applicable elsewhere.

## Modules

General API code for use in the scripts.

- `SB.pm`

A Perl module with wrapper functions around the Seven Bridges 
[command line tool](https://docs.sevenbridges.com/docs/command-line-interface) `sb` 
for use in Perl scripts. 

## GNomEx Project Scripts

- `fetch_gnomex_data.pl`

A script for running database queries against the GNomEx database to extract Experiment 
Request and Analysis project metadata for purposes of identifying those to be 
uploaded to the Seven Bridges platform and removed from GNomEx. 

- `add_division_url.pl`

A script to append Seven Bridges lab division information and generate a URL for a list 
of GNomEx projects, such as that generated from `fetch_gnomex_data.pl`. Requires a 
current list of labs with active Seven Bridges accounts.

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

- `manage_project.pl`

A general purpose script for managing GNomEx Analysis and Request project folders on 
the file server. It will hide files that were bundled into a Zip archive, hide files 
targeted for deletion, restore hidden files, permanently delete hidden files, and insert 
a symbolic link to a missing file notification. 

## General Scripts

General scripts for working on the file server or Seven Bridges. 

- `scan_directory_age.pl`

A general purpose script to find the age of a folder hierarchy. For a given folder 
name, it will recursively identify and report the youngest modification date of any 
file within the folder. Age is reported in days. Useful to find out how old a project 
folder is. The script skips directory time stamps, symbolic links, hidden files, and 
known project metadata files (such as `MANIFEST.csv` files).

- `check_legacy_sb_division.pl`

A script to modify legacy GNomEx projects on Seven Bridges by changing the display 
name and adding a Markdown description to the project to helpfully educate users 
about the project and avoid unnecessary, accidental deletions.

- `compare_versions.pl`

A script for identifying corrupt file versions and replacing it with one restored from 
a tape backup restore. Runs MD5 checksum on every single file.

- `combine_lab_sizes.pl`

A script to concatenate information on both Analysis and Request projects per lab. 
Requires input files containing LabFirstName, LabLastName, Size (in bytes), and 
Date for every GNomEx project. Requires Bio::ToolBox.

## Deprecated Scripts

These are older versions for posterity, mostly for the initial Snowball transfer, and 
should not be used in current production. 

- `get_final_snowball_manifest.pl`

A script to concatenate all project manifest text files into a single master file.

- `get_manifest_file_specs.pl`

A script to collect the file specs on manifest text files themselves.

- `prepare_repository.pl`

Primary script for preparing Request and Analysis project folders for Seven Bridges 
uploading through Snowballs. Handles zip archiving, hiding, unhiding, and deleting 
files in projects. Writes manifest text files.


## Requirements

The scripts are written in Perl and can be executed under the system Perl. Some 
additional Perl modules are required for execution. These should be readily available 
through `yum` package manager or CPAN. Different scripts require different modules, so 
requirements differ depending on usage.

External applications for working with Seven Bridges are also required.

- Perl module `JSON::PP`

- Perl module `Digest::MD5`

- Perl modules `DBI` and `DBI::ODBC`

- Perl module `IO::Prompter`

- Perl module `List::MoreUtils`

- [sb command line tool](https://docs.sevenbridges.com/docs/command-line-interface)

- [sb upload utility](https://docs.sevenbridges.com/docs/upload-via-the-command-line)

- Seven Bridges credentials file with division tokens










