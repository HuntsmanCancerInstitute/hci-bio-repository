package RepoEntry;

use strict;
use English qw(-no_match_vars);
use Carp;
use constant {
	ID          => 0,     # gnomex ID
	PATH        => 1,     # bio-repo file server path
	NAME        => 2,     # gnomex project name
	DATE        => 3,     # gnomex project made date yyyy-mm-dd format
	GROUP       => 4,     # gnomex group fold name
	USEREMAIL   => 5,     # user email address
	USERFIRST   => 6,     # user first name
	USERLAST    => 7,     # user last name
	LABFIRST    => 8,     # PI first name
	LABLAST     => 9,     # PI last name
	CORELAB     => 10,    # name of CORE lab
	BUCKET      => 11,    # s3 bucket name
	PREFIX      => 12,    # s3 prefix
	EXTERNAL    => 13,    # y or n boolean
	STATUS      => 14,    # gnomex request status
	APPLICATION => 15,    # gnomex request application type
	ORGANISM    => 16,    # gnomex organism string
	GENOME      => 17,    # gnomex genome build
	SIZE        => 18,    # current project file size total in bytes
	LASTSIZE    => 19,    # previous project file size total in bytes
	AGE         => 20,    # unix timestamp for youngest observed file in project
	SCAN        => 21,    # unix timestamp for scanning
	UPLOAD      => 22,    # unix timestamp for uploading
	HIDDEN      => 23,    # unix timestamp for hiding
	DELETED     => 24,    # unix timestamp for deleting
	EMAILED     => 25,    # unix timestamp for emailing
	AAUPLOAD    => 26,    # unix timestamp for Request AutoAnalysis upload
	QCSCAN      => 27,    # unix timestamp for request QC folder scan
	AAFOLD      => 28,    # Request AutoAnalysis folder name
	LAB_EMAIL   => 0,     # Email address for the PI
	LAB_UPLOAD  => 1,     # Boolean whether uploads allowed
	LAB_ACCT    => 2,     # CORE lab account name
	ACT_PROFILE => 0,     # service account profile name for accessing account
	ACT_NUMBER  => 1,     # AWS account number used for lab uploads
	DAY         => 86400, # 60 seconds * 60 minutes * 24 hours
	KB          => 1024,  # binary size prefixes
	MB          => 1048576,
	GB          => 1073741824,
	TB          => 1099511627776,
};

our $VERSION = 'v9.0.0';


my $BASE_URL = 'https://hci-apps-ext.hci.utah.edu/core-browser';

sub new {
	my ($class, $data, $lab, $account) = @_;
	if (ref($data) !~ /DBM..Deep/) {
		confess "not a DBM::Deep reference!";
		return;
	}
	my $self = {
		data    => $data,
		lab     => $lab,
		account => $account
	};
	return bless $self, $class;
}


sub is_request {
	my $self = shift;
	if ($self->{data}->[ID] =~ /\d+R$/) {
		return 1;
	}
	else {
		return 0;
	}
}


sub id {
	my $self = shift;
	return $self->{data}->[ID];
}


sub path {
	my $self = shift;
	if (@_) {
		$self->{data}->[PATH] = $_[0];
	}
	return $self->{data}->[PATH];
}


sub name {
	my $self = shift;
	if (@_) {
		$self->{data}->[NAME] = $_[0];
	}
	return $self->{data}->[NAME];
}


sub date {
	my $self = shift;
	if (@_) {
		$self->{data}->[DATE] = $_[0];
	}
	return $self->{data}->[DATE];
}


sub group {
	my $self = shift;
	if (@_) {
		$self->{data}->[GROUP] = $_[0];
	}
	return $self->{data}->[GROUP];
}


sub user_email {
	my $self = shift;
	if (@_) {
		$self->{data}->[USEREMAIL] = $_[0];
	}
	return $self->{data}->[USEREMAIL];
}


sub user_first {
	my $self = shift;
	if (@_) {
		$self->{data}->[USERFIRST] = $_[0];
	}
	return $self->{data}->[USERFIRST];
}


sub user_last {
	my $self = shift;
	if (@_) {
		$self->{data}->[USERLAST] = $_[0];
	}
	return $self->{data}->[USERLAST];
}


sub lab_first {
	my $self = shift;
	if (@_) {
		$self->{data}->[LABFIRST] = $_[0];
	}
	return $self->{data}->[LABFIRST];
}


sub lab_last {
	my $self = shift;
	if (@_) {
		$self->{data}->[LABLAST] = $_[0];
	}
	return $self->{data}->[LABLAST];
}


sub pi_email {
	my $self = shift;
	if (@_) {
		carp 'pi_email() is a read-only method!';
	}
	my $labname = sprintf "%s %s", $self->lab_first, $self->lab_last;
	if ( $labname and exists $self->{lab}->{$labname} ) {
		return $self->{lab}->{$labname}->[LAB_EMAIL];
	}
	else {
		print " ! No lab information available for '$labname'\n";
		return;
	}
}

sub allow_upload {
	my $self = shift;
	my $labname = sprintf "%s %s", $self->lab_first, $self->lab_last;
	if ( $labname and exists $self->{lab}->{$labname} ) {
		my $v = $self->{lab}->{$labname}->[LAB_UPLOAD];
		if ($v eq 'Y') {
			return $self->{lab}->{$labname}->[LAB_ACCT];
		}
		elsif ($v eq 'N') {
			return 0;
		}
		else {
			print " ! lab '$labname' has an invalid upload response '$v'\n";
			return 0;
		}
	}
	else {
		return 0;
	}
}

sub core_lab {
	my $self = shift;
	if (@_) {
		$self->{data}->[CORELAB] = $_[0];
	}
	return $self->{data}->[CORELAB];
}

sub profile {
	my $self = shift;
	if (@_) {
		carp 'profile() is a read-only method!';
	}
	my $core = $self->core_lab;
	return unless $core;
	if ( exists $self->{account}->{$core} ) {
		return $self->{account}->{$core}->[ACT_PROFILE];
	}
	else {
		print " ! No account information available for '$core'\n";
		return;
	}
}

sub account_number {
	my $self = shift;
	if (@_) {
		carp 'account_number() is a read-only method!';
	}
	my $core = $self->core_lab;
	return unless $core;
	if ( exists $self->{account}->{$core} ) {
		return $self->{account}->{$core}->[ACT_NUMBER];
	}
	else {
		print " ! No account information available for '$core'\n";
		return 0;
	}
}

sub bucket {
	my $self = shift;
	if ( @_ and defined $_[0] ) {
		$self->{data}->[BUCKET] = $_[0];
	}
	return $self->{data}->[BUCKET] || q();
}

sub prefix {
	my $self = shift;
	if ( @_ and defined $_[0] ) {
		$self->{data}->[PREFIX] = $_[0];
	}
	return $self->{data}->[PREFIX] || q();
}


sub external {
	my $self = shift;
	if (@_) {
		$self->{data}->[EXTERNAL] = $_[0];
	}
	return $self->{data}->[EXTERNAL];
}


sub request_status {
	my $self = shift;
	if (@_) {
		$self->{data}->[STATUS] = $_[0];
	}
	return $self->{data}->[STATUS];
}


sub request_application {
	my $self = shift;
	if (@_) {
		$self->{data}->[APPLICATION] = $_[0];
	}
	return $self->{data}->[APPLICATION];
}


sub organism {
	my $self = shift;
	if (@_) {
		$self->{data}->[ORGANISM] = $_[0];
	}
	return $self->{data}->[ORGANISM];
}


sub genome {
	my $self = shift;
	if (@_) {
		$self->{data}->[GENOME] = $_[0];
	}
	return $self->{data}->[GENOME];
}


sub size {
	my $self = shift;
	if (@_) {
		my $newsize = $_[0];
		my $cursize = $self->{data}->[SIZE] || 0;
		if ($cursize) {
			my $delta = abs($cursize - $newsize);
			if ($delta > 1024) {
				# there's a significant change of greater than 1Kb
				# then store the last size
				$self->{data}->[LASTSIZE] = $cursize;
			}
		}
		$self->{data}->[SIZE] = $newsize;
	}
	return $self->{data}->[SIZE] || 0;
}


sub last_size {
	my $self = shift;
	return $self->{data}->[LASTSIZE] || 0;
}


sub youngest_datestamp {
	my $self = shift;
	if (@_ and defined $_[0]) {
		$self->{data}->[AGE] = $_[0];
	}
	my $a = $self->{data}->[AGE];
	return $a if defined $a;
	return -1;
}


sub age {
	my $self = shift;
	# calculate current age in days
	my $a = $self->youngest_datestamp;
	if (defined $a and $a > 1) {
		return sprintf("%.0f", (time - $a) / DAY);
	}
	return;
}


sub upload_age {
	my $self = shift;
	my $u = $self->upload_datestamp;
	if ($u > 1) {
		return sprintf("%.0f", (time - $u) / DAY);
	}
	return;
}

sub scan_datestamp {
	my $self = shift;
	if (@_) {
		$self->{data}->[SCAN] = $_[0];
	}
	return $self->{data}->[SCAN] || 0;
}


sub upload_datestamp {
	my $self = shift;
	if (@_) {
		$self->{data}->[UPLOAD] = $_[0];
	}
	return $self->{data}->[UPLOAD] || 0;
}


sub hidden_datestamp {
	my $self = shift;
	if (@_) {
		$self->{data}->[HIDDEN] = $_[0];
	}
	return $self->{data}->[HIDDEN] || 0;
}


sub hidden_age {
	my $self = shift;
	my $h = $self->hidden_datestamp;
	if ($h) {
		return sprintf("%.0f", (time - $h) / DAY);
	}
	return;
}

sub deleted_datestamp {
	my $self = shift;
	if (@_) {
		$self->{data}->[DELETED] = $_[0];
	}
	return $self->{data}->[DELETED] || 0;
}


sub emailed_datestamp {
	my $self = shift;
	if (@_) {
		$self->{data}->[EMAILED] = $_[0];
	}
	return $self->{data}->[EMAILED] || 0;
}

sub autoanal_up_datestamp {
	my $self = shift;
	if (@_) {
		$self->{data}->[AAUPLOAD] = $_[0];
	}
	return $self->{data}->[AAUPLOAD] || 0;
}

sub autoanal_upload_age {
	my $self = shift;
	my $u = $self->autoanal_up_datestamp;
	if ($u > 1) {
		return sprintf("%.0f", (time - $u) / DAY);
	}
	return;
}

sub qc_scan_datestamp {
	my $self = shift;
	if (@_) {
		$self->{data}->[QCSCAN] = $_[0];
	}
	return $self->{data}->[QCSCAN] || 0;
}

sub autoanal_folder {
	my $self = shift;
	if (@_) {
		$self->{data}->[AAFOLD] = $_[0];
	}
	return $self->{data}->[AAFOLD];
}

sub project_s3_uri {
	my $self = shift;
	my $bucket = $self->bucket;
	my $prefix = $self->prefix;
	return q() unless ( $bucket and $prefix );
	return sprintf("s3://%s/%s/", $bucket, $prefix );
}

sub project_core_url {
	my $self   = shift;
	my $org    = $self->core_lab;
	my $number = $self->account_number;
	my $bucket = $self->bucket;
	my $prefix = $self->prefix;
	return q() unless ( $org and $number and $bucket and $prefix );

	# escape spaces
	$org =~ s/\ /%20/g;

	# generate url to CORE Browser - this assumes always AWS accounts
	my $url = sprintf "%s/goto?organization=%s&account=AWS%%20%s&path=/%s/%s", $BASE_URL,
		$org, $number, $bucket, $prefix;
	return $url;
}



sub print_string {
	my $self = shift;
	my $transform = shift || 0;
	
	# collect the data
	# following the same order as the stored array - see the constant hash above
	my @data = (
		$self->id,
		$self->path,
		$self->name,
		$self->date,
		$self->group,
		$self->user_email,
		$self->user_first,
		$self->user_last,
		$self->lab_first,
		$self->lab_last,
		$self->core_lab || q(),
		$self->bucket || q(),
		$self->prefix || q(),
		$self->external || q(),
		$self->request_status || q(),
		$self->request_application || q(),
		$self->organism || q(),
		$self->genome || q(),
		$self->size || q(),
		$self->last_size || q(),
		$self->youngest_datestamp || 0,
		$self->scan_datestamp || 0,
		$self->upload_datestamp || 0,
		$self->hidden_datestamp || 0,
		$self->deleted_datestamp || 0,
		$self->emailed_datestamp || 0,
		$self->autoanal_up_datestamp || 0,
		$self->qc_scan_datestamp || 0,
		$self->autoanal_folder || q()
	);
	
	# transform posix times as necessary
	if ($transform) {
		
		# convert times from epoch to YYYYMMDD
		for my $i (AGE, SCAN, UPLOAD, HIDDEN, DELETED, EMAILED, AAUPLOAD, QCSCAN) {
			next unless (defined $data[$i] and $data[$i]);
			my @times = localtime($data[$i]);
			if ($times[5] == 69 or $times[5] == 70) {
				$data[$i] = q();
			}
			else {
				$data[$i] = sprintf("%04d-%02d-%02d", 
					$times[5] + 1900, $times[4] + 1, $times[3]);
			}
		}
		
		# convert sizes
		for my $i (SIZE, LASTSIZE) {
			if ($data[$i] > TB) {
				$data[$i] = sprintf("%.1fT", $data[$i] / TB);
			}
			elsif ($data[$i] > GB) {
				$data[$i] = sprintf("%.1fG", $data[$i] / GB);
			}
			elsif ($data[$i] > MB) {
				$data[$i] = sprintf("%.1fM", $data[$i] / MB);
			}
			elsif ($data[$i] > 1000) {
				$data[$i] = sprintf("%.1fK", $data[$i] / KB);
			}
			else {
				$data[$i] = sprintf("%dB", $data[$i]);
			}
		}
	}
	
	# return as tab-delimited string
	return sprintf("%s\n", join("\t", @data));
}

1;

=head1 NAME

RepoEntry - A Repository project entry in the catalog

=head1 DESCRIPTION

An object representing a project entry in the catalog. This is 
what users interact with working with the catalog. 

=head1 FUNCTIONS

Provides a number of get/set functions for the various fields for 
a Repository project entry. Most are self-explanatory. Call the function
to return the value. Pass a value to set the field.

The timestamp fields return Unix epoch time, which must be converted 
to a human readable format. 

The age functions return the difference in days between the current time
and the recorded date time stamp.

=over 4

=item is_request

=item id

=item path

=item name

=item date

=item group

=item user_email

=item user_first

=item user_last

=item lab_first

=item lab_last

=item pi_email

This is now a read-only function.

=item allow_upload

=item core_lab

=item profile

This is now a read-only function.

=item account_number

This is a read-only function.

=item bucket

=item prefix

=item external

=item request_status

=item request_application

=item organism

=item genome

=item size

This gets/sets the current size of the project in bytes. When setting, the
previous size is automatically stored as the last size.

=item last_size

This a read-only function.

=item youngest_datestamp

=item age

Calculates the age in days from the youngest datestamp to now.

=item scan_datestamp

=item upload_datestamp

=item hidden_datestamp

=item deleted_datestamp

=item emailed_datestamp

=itme autoanal_up_datestamp

=item autoanal_upload_age

Calculates the age in days for the autoanalysis folder.

=item qc_scan_datestamp

=item autoanal_folder

=item project_s3_uri

=item project_core_url

=item print_string

Returns a printable, tab-delimited string with new line ending
representation of the project. Pass a true/false value to transform
datestamps from Unix epoch integers to date-time formatted strings,
and sizes in bytes to a magnitude suffix (K, M, G, T) using binary
(base 2, instead of base 10) transformations.

=back

=head1 AUTHOR

 Timothy J. Parnell, PhD
 Bioinformatics Shared Resource
 Huntsman Cancer Institute
 University of Utah
 Salt Lake City, UT, 84112

This package is free software; you can redistribute it and/or modify
it under the terms of the Artistic License 2.0.  



