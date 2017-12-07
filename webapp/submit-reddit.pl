#!/usr/bin/perl -w
# Program: cass_sample.pl
# Note: includes bug fixes for Net::Async::CassandraCQL 0.11 version

use strict;
use warnings;
use 5.10.0;
use FindBin;

use Scalar::Util qw(
        blessed
    );
use Try::Tiny;

use Kafka::Connection;
use Kafka::Producer;

use Data::Dumper;
use CGI qw/:standard/, 'Vars';

my $subreddit = param('subreddit');
if(!$subreddit) {
    exit;
}
my $comments = param('comments');
my $golds = param('golds');
my $score = param('score');

my ( $connection, $producer );
try {
    #-- Connection
    $connection = Kafka::Connection->new( host => '10.0.0.2', port => 6667 ); # cluster
    #$connection = Kafka::Connection->new( host => 'localhost', port => 9092 ); # VM

    #-- Producer
    $producer = Kafka::Producer->new( Connection => $connection );
    # Only put in the subreddit and post stats because those are the only ones we care about
    my $message = '{"subreddit":"'.$subreddit.'",';
    $message.='"comments":"'.$comments.'",';
    $message.='"golds":"'.$golds.'",';
    $message.='"score":"'.$score.'"}';

    # Sending a single message
    my $response = $producer->send(
	'subreddit-post',          # topic
	0,                                 # partition
	$message                           # message
        );
} catch {
    if ( blessed( $_ ) && $_->isa( 'Kafka::Exception' ) ) {
	warn 'Error: (', $_->code, ') ',  $_->message, "\n";
	exit;
    } else {
	die $_;
    }
};

# Closes the producer and cleans up
undef $producer;
undef $connection;

print header, start_html(-title=>'Submit subreddit post',-head=>Link({-rel=>'stylesheet',-href=>'/table.css',-type=>'text/css'}));
print table({-class=>'CSS_Table_Example', -style=>'width:80%;'},
            caption('Subreddit post submitted'),
	    Tr([th(["subreddit","comments","golds","score"]),
	        td([$subreddit, $comments, $golds, $score])]));

#print $protocol->getTransport->getBuffer;
print end_html;

