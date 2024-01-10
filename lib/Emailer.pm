package Emailer;

use strict;
use Carp;
use Email::Simple;
use Email::Sender::Simple;
use Email::Sender::Transport::SMTP;

# default values
my $default_from_email = 'Timothy Parnell <timothy.parnell@hci.utah.edu>';
my $default_smtp = 'smtp.utah.edu';

our $VERSION = 6.0;


sub new {
	my $class = shift;
	my %opts = @_;
	$opts{smtp_address} ||= $default_smtp;
	$opts{from} ||= $default_from_email;
	
	# initialize transport
	$opts{smtp} = Email::Sender::Transport::SMTP->new( {
		host    => $opts{smtp_address},
	} );
	unless ($opts{smtp}) {
		croak "failed to initialize SMTP Transport!";
	}
	
	return bless \%opts, $class;
}

sub from {
	return shift->{from};
}

sub smtp {
	return shift->{smtp};
}

sub send_request_upload_email {
	my $self = shift;
	
	# options
	my $opt = $self->_process_options(@_);
	
	# compose body
	my $body = <<DOC;
Hello $opt->{username} and $opt->{piname},

Your GNomEx Request project ‘$opt->{id}’, ‘$opt->{name}’, has been uploaded to your Amazon Web Services (AWS) account ‘$opt->{core}’. You may view the files using the CORE Browser application (https://hci-apps-ext.hci.utah.edu/core-browser). They are located in the bucket ‘$opt->{bucket}’ in the folder prefix ‘$opt->{prefix}’; this is based on the Group folder and metadata in the GNomEx database.

The standard bucket lifecycle policies will archive the Fastq files to Deep Glacier within 3 days, unless the bucket is configured otherwise. This is meant for long-term storage (>6 months) at the lowest cost available (currently \$0.001 per GB per month). Archived files will need to be temporarily restored before they can be viewed or downloaded; this will incur a small fee per standard AWS cost policies. 

Your files may remain on GNomEx for your convenience as space allows (a few months). A manifest of the files will always remain on GNomEx, as well as the record of your sequencing request and samples in the GNomEx database.

For more information, see our website at https://uofuhealth.utah.edu/huntsman/shared-resources/gcb/cbi/data-access-storage.  

DOC
		
	my $subject = sprintf "GNomEx Request %s upload to your AWS account", $opt->{id};
	
	# send
	return $self->_send_email($opt, $body, $subject);
}


sub send_analysis_upload_email {
	my $self = shift;
	
	# options
	my $opt = $self->_process_options(@_);
	
	# compose body
	my $body = <<DOC;
Hello $opt->{username} and $opt->{piname},

Your GNomEx Analysis project ‘$opt->{id}’, ‘$opt->{name}’, is $opt->{age} days old and has reached the age limits for storage on GNomEx. The files have been uploaded to your Amazon Web Services (AWS) account ‘$opt->{core}’. You may view the files using the CORE Browser application (https://hci-apps-ext.hci.utah.edu/core-browser). They are located in the bucket ‘$opt->{bucket}’ in the folder prefix ‘$opt->{prefix}’; this is based on the Group folder and metadata in the GNomEx database.

The standard bucket lifecycle policies will archive large files to Deep Glacier within 3 days, unless the bucket is configured otherwise. This is meant for long-term storage (>6 months) at the lowest cost available (currently \$0.001 per GB per month). Archived files will need to be temporarily restored before they can be viewed or downloaded; this will incur a small fee per standard AWS cost policies. 

The files on GNomEx have been removed, although a manifest of the files will always remain, as well as the database entry for the project. Certain analysis files may remain as a courtesy for serving to genome browsers.

For more information, see our website at https://uofuhealth.utah.edu/huntsman/shared-resources/gcb/cbi/data-access-storage.  

DOC
		
	my $subject = sprintf "GNomEx Analysis %s upload to your AWS account", $opt->{id};
	
	# send
	return $self->_send_email($opt, $body, $subject);
}


sub send_request_deletion_email {
	my $self = shift;
	
	# options
	my $opt = $self->_process_options(@_);
	
	# compose body
	my $body = <<DOC;
Hello $opt->{username} and $opt->{piname},

Your GNomEx Request project ‘$opt->{id}’, ‘$opt->{name}’, is $opt->{age} days old and has reached the age limits for storage on GNomEx and WILL BE DELETED. Long-term storage is no longer available on the GNomEx server. We urge you to make sure these data files are secured offsite. In many cases, publications and granting agencies require that genomic data be made available or retained for a certain time. Please verify that you have a copy of these files, especially Fastq files.

Files will be removed in one week.

A manifest of the files will remain on GNomEx, as well as the record of your sequencing request and samples. 

For more information, including long-term cloud storage options, please see our website at https://uofuhealth.utah.edu/huntsman/shared-resources/gcb/cbi/data-access-storage. Please contact us if you have any questions or would like to set up an AWS cloud storage account.

Cancer Bioinformatics Shared Resource
https://uofuhealth.utah.edu/huntsman/shared-resources/gcb/cbi. 

DOC
		
	my $subject = sprintf "GNomEx Request %s scheduled deletion", $opt->{id};
	
	# send
	return $self->_send_email($opt, $body, $subject);
}


sub send_analysis_deletion_email {
	my $self = shift;
	
	# options
	my $opt = $self->_process_options(@_);
	
	# compose body
	my $body = <<DOC;
Hello $opt->{username} and $opt->{piname},

Your GNomEx Request project ‘$opt->{id}’, ‘$opt->{name}’, is $opt->{age} days old and has reached the age limits for storage on GNomEx and WILL BE DELETED. Long-term storage is no longer available on the GNomEx server. We urge you to make sure these data files are secured offsite. In many cases, publications and granting agencies require that genomic data be made available or retained for a certain time. Please verify that you have a copy of these files, especially Fastq files.

Files will be removed in one week.

A manifest of the files will always remain on GNomEx, as well as the database entry for the project. Certain analysis files may remain as a courtesy for serving to genome browsers.

For more information, including long-term cloud storage options, please see our website at https://uofuhealth.utah.edu/huntsman/shared-resources/gcb/cbi/data-access-storage. Please contact us if you have any questions or would like to set up an AWS cloud storage account.

Cancer Bioinformatics Shared Resource
https://uofuhealth.utah.edu/huntsman/shared-resources/gcb/cbi. 

DOC
		
	my $subject = sprintf "GNomEx Analysis %s scheduled deletion", $opt->{id};
	
	# send
	return $self->_send_email($opt, $body, $subject);
}


sub _process_options {
	my $self = shift;
	
	my %opt;
	if (ref($_[0]) eq 'RepoEntry') {
		# yay, an object to use!
		my $E = shift @_;
		$opt{username}  = $E->user_first . q( ) . $E->user_last;
		$opt{piname}    = $E->lab_first . q( ) . $E->lab_last;
		$opt{useremail} = $E->user_email;
		$opt{piemail}   = $E->pi_email;
		$opt{id}        = $E->id;
		$opt{name}      = $E->name;
		$opt{core}      = $E->core_lab;
		$opt{bucket}    = $E->bucket;
		$opt{prefix}    = $E->prefix;
		$opt{size}      = $E->size;
		$opt{age}       = $E->age;
		
		if (@_) {
			# can't rely on a recent version of List::Util being installed,
			# so do it old fashioned way
			while (@_) {
				my $key = shift @_;
				my $val = shift @_ || q();
				$opt{$key} = $val;
			}
		}
	}
	else {
		# a list of key values, I hope? hope we have the right ones!
		%opt = @_;
		$opt{username}  ||= q();
		$opt{piname}    ||= q();
		$opt{useremail} ||= q();
		$opt{piemail}   ||= q();
		$opt{id}        ||= q();
		$opt{name}      ||= q();
		$opt{core}      ||= q();
		$opt{bucket}    ||= q();
		$opt{prefix}    ||= q();
		$opt{size}      ||= q();
		$opt{age}       ||= '?';
	}
	unless (exists $opt{from}) {
		$opt{from}      = $self->from;
	}
	
	return \%opt;
}

sub _send_email {
	my ($self, $opt, $body, $subject) = @_;
	
	# assemble email
	my $email = Email::Simple->create(
		header  => [
			To      => sprintf("%s <%s>, %s <%s>", $opt->{username}, $opt->{useremail}, 
							$opt->{piname}, $opt->{piemail}),
			Cc      => sprintf("%s", $opt->{from}),
			From    => $opt->{from},
			Subject => $subject,
		],
		body    => $body,
	) or die "unable to compose email!\n";
	
	# return email as string
	if (exists $opt->{mock} and $opt->{mock}) {
		return $email->as_string;
	}
	
	# send
	return Email::Sender::Simple->try_to_send($email, {transport => $self->smtp});
}

1;

__END__

=head1 NAME 

Emailer - HCI-specific library for sending out form emails

=head1 DESCRIPTION

These are subroutines for sending standard form emails to University
of Utah GNomEx users. The content of the emails is hard coded here.

=head1 USAGE

There are four exported functions for sending emails. 

=over 4

=item send_analysis_upload_email

This is sent to the GNomEx user and the corresponding principal investigator 
upon the completion of uploading a GNomEx Analysis project to their bucket
in their corresponding AWS account.

=item send_request_upload_email

This is sent to the GNomEx user and the corresponding principal investigator 
upon the completion of uploading a GNomEx Request project to their bucket
in their corresponding AWS account.

=item send_analysis_deletion_email

This is sent to the GNomEx user and the corresponding principal investigator 
to notify them of the impending scheduled removal of their GNomEx Analysis 
project in one week. This is primarily meant for labs that do not have a 
an active AWS account.  

=item send_request_deletion_email

This is sent to the GNomEx user and the corresponding principal investigator 
to notify them of the impending scheduled removal of their GNomEx Request 
project in one week. This is primarily meant for labs that do not have a 
an active AWS account.  

=back

To use these functions, pass a L<RepoEntry> Catalog object, from which 
various values regarding the project can be used to fill in specific 
fields in the form email. Specific values can be overridden and/or 
supplemented by passing additional C<key> =E<gt> C<value> pairs. 

If no Catalog entry object is available, then all fields must be 
provided as C<key> =E<gt> C<value> pairs.

List of keys include the following:

=over 4

=item username

User name, usually "First Last"

=item useremail

User email address

=item piname

Principal Investigator name, "First Last"

=item piemail

Principal Investigator email address

=item id

GNomEx project identifier

=item name

GNomEx project title name

=item core_lab

AWS CORE lab name

=item bucket

The AWS bucket name

=item prefix

The AWS bucket prefix

=item size

Size of project in bytes

=item age

Age of project in days

=back

An example of the usage is below.

    my $Entry = $Catalog->entry('A1234');
    my $success = send_analysis_upload_email(
        $Entry, 
        useremail => 'first.name@utah.edu',
    );


=head1 AUTHOR

 Timothy J. Parnell, PhD
 Cancer Bioinformatics Shared Resource
 Huntsman Cancer Institute
 University of Utah
 Salt Lake City, UT, 84112

This package is free software; you can redistribute it and/or modify
it under the terms of the Artistic License 2.0.  




