# HCI-Bio-Repository

These are scripts for working with the HCI bio repository file server, particularly 
with respect in preparation and migration of GNomEx data to Seven Bridges. 

## Modules

- `SB.pm`

A Perl module with wrapper functions around the Seven Bridges 
[command line tool](https://docs.sevenbridges.com/docs/command-line-interface) `sb` 
for use in Perl scripts. 

## Scripts

A quick list of what is in here:

**NOTE** Most of these are specific to HCI infrastructure and not really applicable 
anywhere else.

- `check_legacy_sb_division`

A script to modify legacy GNomEx projects on Seven Bridges by changing the display 
name and adding a Markdown description to the project to helpfully educate users 
about the project and avoid unnecessary, accidental deletions.

- `combine_lab_sizes.pl`

A script to concatenate information on both Analysis and Request projects per lab. 
Requires input files containing LabFirstName, LabLastName, Size (in bytes), and 
Date for every GNomEx project. Requires Bio::ToolBox.

- `compare_versions`

A script for identifying corrupt file versions and replacing it with one restored from 
a tape backup restore. Runs MD5 checksum on every single file.

- `fetch_gnomex_data`

A script for running database queries against the GNomEx database to extract Request 
and Analysis metadata for purposes of tagging Seven Bridges files.

- `get_final_snowball_manifest`

A script to concatenate all project manifest text files into a single master file.

- `get_manifest_file_specs`

A script to collect the file specs on manifest text files themselves.

- `prepare_repository`

Primary script for preparing Request and Analysis project folders for Seven Bridges 
uploading through Snowballs. Handles zip archiving, hiding, unhiding, and deleting 
files in projects. Writes manifest text files.

- `process_request_project`

Script for processing Request project folders. Handles writing manifest CSV file, 
creating new project on Seven Bridges platform under given lab division, uploads and 
tags files using the Seven Bridges uploader, and hides local files. Supersedes the 
older `prepare_repository` script.


