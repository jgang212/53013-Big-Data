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

my $station = param('station');
if(!$station) {
    exit;
}
my $fog = param('fog') ? 1 : 0;
my $rain = param('rain') ? 1 : 0;
my $snow = param('snow') ? 1 : 0;
my $hail = param('hail') ? 1 : 0;
my $thunder = param('thunder') ? 1 : 0;
my $tornado = param('tornado') ? 1 : 0;

my ( $connection, $producer );
try {
    #-- Connection
    # $connection = Kafka::Connection->new( host => '10.0.0.2', port => 6667 ); # cluster
    $connection = Kafka::Connection->new( host => 'localhost', port => 9092 ); # VM

    #-- Producer
    $producer = Kafka::Producer->new( Connection => $connection );
    # Only put in the station_id and weather elements because those are the only ones we care about
    my $message = '{"station":"'.param("station").'",';
    $message.='"fog":'.($fog? 'true,':'false,');
    $message.='"rain":'.($rain? 'true,':'false,');
    $message.='"snow":'.($snow? 'true,':'false,');
    $message.='"hail":'.($hail? 'true,':'false,');
    $message.='"thunder":'.($thunder? 'true,':'false,');
    $message.='"tornado":'.($tornado? 'true,':'false}');

    # Sending a single message
    my $response = $producer->send(
	'weather-reports',          # topic
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

print header, start_html(-title=>'Submit weather',-head=>Link({-rel=>'stylesheet',-href=>'/table.css',-type=>'text/css'}));
print table({-class=>'CSS_Table_Example', -style=>'width:80%;'},
            caption('Weather report submitted'),
	    Tr([th(["station","fog","rain","snow", "hail","thunder","tornado"]),
	        td([$station, $fog, $rain, $snow, $hail, $thunder, $tornado])]));

#print $protocol->getTransport->getBuffer;
print end_html;

