package Emailer;

use strict;
use Carp;
require Exporter;
use Email::Simple;
use Email::Sender::Simple;
use Email::Sender::Transport::SMTP;


our @ISA = qw(Exporter);
our @EXPORT = qw(
	send_analysis_upload_email
	send_analysis_deletion_email
	send_request_upload_email
	send_request_deletion_email
);

1;

# SMTP server
my $smtp = Email::Sender::Transport::SMTP->new( {
	host    => 'smtp.utah.edu',
} );

# From address
my $default_from_email = 'Timothy Parnell <timothy.parnell@hci.utah.edu>';


sub send_request_upload_email {
	
	# options
	my $opt = _process_options(@_);
	
	# compose body
	my $body = <<DOC;
Hello $opt->{username} and $opt->{piname},

Your GNomEx Request project $opt->{id}, $opt->{name}, has been uploaded to your lab division $opt->{division} on the Seven Bridges platform. You may find it at $opt->{url}. 

You will need a Seven Bridges account to view the project. If you are unable to view the project, contact your lab division administrator (Your PI or lab manager).

Your files are on standard AWS S3 storage and may be used immediately for analysis on the platform. It will cost about $opt->{s3_cost} per month.

You may archive these files to AWS Glacier at a reduced cost of $opt->{glacier_cost} per month.

The files will remain on GNomEx for your convenience for an additional six months before being removed. A manifest of the files will remain on GNomEx, as well as the record of your sequencing request and samples.

For more information, see our FAQ at https://uofuhealth.utah.edu/huntsman/shared-resources/gba/bioinformatics/cloud-storage/seven-bridges-usage-faq.php. 

DOC
		
	my $subject = sprintf "GNomEx Request %s upload to Seven Bridges", $opt->{id};
	
	# send
	return _send_email($opt, $body, $subject);
}


sub send_analysis_upload_email {
	
	# options
	my $opt = _process_options(@_);
	
	# compose body
	my $body = <<DOC;
Hello $opt->{username} and $opt->{piname},

Your GNomEx Analysis project $opt->{id}, $opt->{name}, is $opt->{age} days old and has reached the age limits for storage on GNomEx. The files have been uploaded to your lab division $opt->{division} on the Seven Bridges platform. You may find it at $opt->{url}. 

You will need a Seven Bridges account to view the project. If you are unable to view the project, contact your lab division administrator (Your PI or lab manager).

Your files are on standard AWS S3 storage and may still be accessed immediately. It will cost about $opt->{s3_cost} per month to keep these files.

You may archive these files to AWS Glacier at a reduced cost of $opt->{glacier_cost} per month.

The files on GNomEx have been removed, although a manifest of the files will always remain, as well as the database entry for the project. Certain analysis files may remain as a courtesy for serving to genome browsers.

For more information, see our FAQ at https://uofuhealth.utah.edu/huntsman/shared-resources/gba/bioinformatics/cloud-storage/seven-bridges-usage-faq.php. 

DOC
		
	my $subject = sprintf "GNomEx Analysis %s upload to Seven Bridges", $opt->{id};
	
	# send
	return _send_email($opt, $body, $subject);
}


sub send_request_deletion_email {
	
	# options
	my $opt = _process_options(@_);
	
	# compose body
	my $body = <<DOC;
Hello $opt->{username} and $opt->{piname},

Your GNomEx Request project $opt->{id}, $opt->{name}, is $opt->{age} days old and has reached the age limits for storage on GNomEx and WILL BE DELETED. Long-term storage is no longer available on the GNomEx server. We urge you to make sure these data files are secured offsite. In many cases, publications and granting agencies require that genomic data be made available or retained for a certain time. Please verify that you have a copy of these files.

Files will be removed in one week.

A manifest of the files will remain on GNomEx, as well as the record of your sequencing request and samples. 

For more information, including cloud storage options, see our storage policy at https://uofuhealth.utah.edu/huntsman/shared-resources/gba/bioinformatics/cloud-storage/.

DOC
		
	my $subject = sprintf "GNomEx Request %s scheduled deletion", $opt->{id};
	
	# send
	return _send_email($opt, $body, $subject);
}


sub send_analysis_deletion_email {
	
	# options
	my $opt = _process_options(@_);
	
	# compose body
	my $body = <<DOC;
Hello $opt->{username} and $opt->{piname},

Your GNomEx Analysis project $opt->{id}, $opt->{name}, is $opt->{age} days old and has reached the age limits for storage on GNomEx and WILL BE DELETED. Long-term storage is no longer available on the GNomEx server. We urge you to make sure these data files are secured offsite. In many cases, publications and granting agencies require that genomic data be made available or retained for a certain time. Please verify that you have a copy of these files.

Files will be removed in one week.

A manifest of the files will always remain on GNomEx, as well as the database entry for the project. Certain analysis files may remain as a courtesy for serving to genome browsers.

For more information, including cloud storage options, see our storage policy at https://uofuhealth.utah.edu/huntsman/shared-resources/gba/bioinformatics/cloud-storage/. 

DOC
		
	my $subject = sprintf "GNomEx Analysis %s scheduled deletion", $opt->{id};
	
	# send
	return _send_email($opt, $body, $subject);
}


sub _process_options {
	
	my %opt;
	if (ref($_[0]) eq 'RepoEntry') {
		# yay, an object to use!
		my $E = shift @_;
		$opt{username}  = $E->user_first . ' ' . $E->user_last;
		$opt{piname}    = $E->lab_first . ' ' . $E->lab_last;
		$opt{useremail} = $E->user_email;
		$opt{piemail}   = $E->pi_email;
		$opt{id}        = $E->id;
		$opt{name}      = $E->name;
		$opt{division}  = $E->division;
		$opt{url}       = $E->project_url;
		$opt{size}      = $E->size;
		$opt{age}       = $E->age;
		
		if (@_) {
			# can't rely on a recent version of List::Util being installed,
			# so do it old fashioned way
			while (@_) {
				my $key = shift @_;
				my $val = shift @_ || '';
				$opt{$key} = $val;
			}
		}
	}
	else {
		# a list of key values, I hope? hope we have the right ones!
		%opt = @_;
		$opt{username}  ||= '';
		$opt{piname}    ||= '';
		$opt{useremail} ||= '';
		$opt{piemail}   ||= '';
		$opt{id}        ||= '';
		$opt{name}      ||= '';
		$opt{division}  ||= '';
		$opt{url}       ||= '';
		$opt{size}      ||= '';
		$opt{age}       ||= '?';
	}
	unless (exists $opt{from}) {
		$opt{from}      = $default_from_email;
	}
	
	# calculate costs
	if ($opt{size}) {
		$opt{s3_cost} = sprintf "\$%.2f", ($opt{size} / 1000000000) * 0.023;
		$opt{glacier_cost} = sprintf "\$%.2f", ($opt{size} / 1000000000) * 0.004;
	}
	else {
		$opt{s3_cost} = '';
		$opt{glacier_cost} = '';
	}
	
	return \%opt;
}

sub _send_email {
	my ($opt, $body, $subject) = @_;
	
	# assemble email
	my $email = Email::Simple->create(
		header  => [
			To      => sprintf("%s <%s>", $opt->{username}, $opt->{useremail}),
			Cc      => sprintf("%s <%s>", $opt->{piname}, $opt->{piemail}),
			From    => $opt->{from},
			Subject => $subject,
		],
		body    => $body,
	) or die "unable to compose email!\n";
	
	# send
	return Email::Sender::Simple->try_to_send($email, {transport => $smtp});
}





=cut


my $result = Email::Sender::Simple->try_to_send($email, {
	transport => $smtp,
	to        => [$to],
	from      => $from,
});

