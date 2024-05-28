# Deprecated HCI-Bio-Repository stuff

These are old scripts and modules that no are longer actively maintained but are kept 
here for posterity, and in case I ever need to go back to replicate and/or trouble shoot
old behavior. As such, they are here entirely for historical, academic purposes and 
should not be deployed for production.

- `SB.pm`

A Perl module with wrapper functions around the Seven Bridges 
[command line tool](https://docs.sevenbridges.com/docs/command-line-interface) `sb` 
for use in Perl scripts. 

- `fetch_gnomex_data.pl`

A script for running database queries against the GNomEx database to extract Experiment 
Request and Analysis project metadata for purposes of identifying those to be 
uploaded to the Seven Bridges platform and removed from GNomEx. 

- `add_division_url.pl`

A script to append Seven Bridges lab division information and generate a URL for a list 
of GNomEx projects, such as that generated from `fetch_gnomex_data.pl`. Requires a 
current list of labs with active Seven Bridges accounts.

- `manage_project.pl`

A general purpose script for managing GNomEx Analysis and Request project folders on 
the file server. It will hide files that were bundled into a Zip archive, hide files 
targeted for deletion, restore hidden files, permanently delete hidden files, insert 
a symbolic link to a missing file notification, and more. 

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

- `combine_lab_sizes.pl`

A script to concatenate information on both Analysis and Request projects per lab. 
Requires input files containing LabFirstName, LabLastName, Size (in bytes), and 
Date for every GNomEx project. Requires Bio::ToolBox.

- `get_final_snowball_manifest.pl`

A script to concatenate all project manifest text files into a single master file.

- `get_manifest_file_specs.pl`

A script to collect the file specs on manifest text files themselves.

- `prepare_repository.pl`

Primary script for preparing Request and Analysis project folders for Seven Bridges 
uploading through Snowballs. Handles zip archiving, hiding, unhiding, and deleting 
files in projects. Writes manifest text files.

- `process_analysis_project.pl`

Old script for processing GNomEx Analysis project folders. Identifies most common 
bioinformatic file types, classifies them, and collects file size, date stamp, and MD5 
checksum attributes, and writes these to a project `MANIFEST.csv` file. Optionally 
gzip compress large text files or archive into a single bulk Zip archive for data 
storage efficiency. 

- `process_request_project.pl`

Old script for processing GNomEx Experiment Request project folders. Collects essential 
sequencing metadata from Fastq files, including sample ID, platform, lane, paired-end 
status, and MD5 checksum. Metadata is written to a `MANIFEST.csv` file. 


