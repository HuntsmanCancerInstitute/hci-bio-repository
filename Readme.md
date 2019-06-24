# HCI-Bio-Repository

These are scripts for working with the HCI bio repository file server, particularly 
with respect in preparation and migration of GNomEx data to Seven Bridges. 

## Internal use only

These are specific to HCI infrastructure and not really applicable anywhere else.

### Scripts

A quick list of what is in here:

- `compare_versions`

A script for identifying corrupt file versions and replacing it with one restored from 
a tape backup restore. Runs MD5 checksum on every single file.

- `fetch_analysis`

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


