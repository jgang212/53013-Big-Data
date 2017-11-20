#!/usr/bin/perl -w
# Creates an html table of flight delays by weather for the given route

# Needed includes
use strict;
use warnings;
use 5.10.0;
use HBase::JSONRest;
use CGI qw/:standard/;

# Read the origin and destination airports as CGI parameters
my $origin = param('origin');
my $dest = param('dest');
 
# Define a connection template to access the HBase REST server
# If you are on out cluster, hadoop-m will resolve to our Hadoop master
# node, which is running the HBase REST server
my $hbase = HBase::JSONRest->new(host => "localhost:8080");

# This function takes a row and gives you the value of the given column
# E.g., cellValue($row, 'delay:rain_delay') gives the value of the
# rain_delay column in the delay family.
# It uses somewhat tricky perl, so you can treat it as a black box
sub cellValue {
    my $row = $_[0];
    my $field_name = $_[1];
    my $row_cells = ${$row}{'columns'};
    foreach my $cell (@$row_cells) {
	if ($$cell{'name'} eq $field_name) {
	    return $$cell{'value'};
	}
    }
    return 'missing';
}

# Query hbase for the route. For example, if the departure airport is ORD
# and the arrival airport is DEN, the "where" clause of the query will
# require the key to equal ORDDEN
my $records = $hbase->get({
  table => 'weather_delays_by_route_8',
  where => {
    key_equals => $origin.$dest
  },
});

# There will only be one record for this route, which will be the
# "zeroth" row returned
my $row = @$records[0];
    
# Get the value of all the columns we need and store them in named variables
# Perl's ability to assign a list of values all at once is very convenient here
my($clear_flights, $clear_delays, $fog_flights, $fog_delays, $rain_flights, $rain_delays,
   $snow_flights, $snow_delays, $hail_flights, $hail_delays, $thunder_flights, $thunder_delays,
   $tornado_flights, $tornado_delays)
 =  (cellValue($row, 'delay:clear_flights'), cellValue($row, 'delay:clear_delays'),
     cellValue($row, 'delay:fog_flights'), cellValue($row, 'delay:fog_delays'),
     cellValue($row, 'delay:rain_flights'), cellValue($row, 'delay:rain_delays'),
     cellValue($row, 'delay:snow_flights'), cellValue($row, 'delay:snow_delays'),
     cellValue($row, 'delay:hail_flights'), cellValue($row, 'delay:hail_delays'),
     cellValue($row, 'delay:thunder_flights'), cellValue($row, 'delay:thunder_delays'),
     cellValue($row, 'delay:tornado_flights'), cellValue($row, 'delay:tornado_delays'));

# Given the number of flights and the total delay, this gives the average delay
sub average_delay {
    my($flights, $delay) = @_;
    return $flights > 0 ? sprintf("%.1f", $delay/$flights) : "-";
}

# Print an HTML page with the table. Perl CGI has commands for all the
# common HTML tags
print header, start_html(-title=>'hello CGI',-head=>Link({-rel=>'stylesheet',-href=>'/table.css',-type=>'text/css'}));
print div({-style=>'margin-left:275px;margin-right:auto;display:inline-block;box-shadow: 10px 10px 5px #888888;border:1px solid #000000;-moz-border-radius-bottomleft:9px;-webkit-border-bottom-left-radius:9px;border-bottom-left-radius:9px;-moz-border-radius-bottomright:9px;-webkit-border-bottom-right-radius:9px;border-bottom-right-radius:9px;-moz-border-radius-topright:9px;-webkit-border-top-right-radius:9px;border-top-right-radius:9px;-moz-border-radius-topleft:9px;-webkit-border-top-left-radius:9px;border-top-left-radius:9px;background:white'}, '&nbsp;Flight Delays from ' . $origin . ' to ' . $dest . ' by weather&nbsp;');
print     p({-style=>"bottom-margin:10px"});
print table({-class=>'CSS_Table_Example', -style=>'width:60%;margin:auto;'},
	    Tr([td(['Clear', 'Fog', 'Rain', 'Snow', 'Hail', 'Thunder', 'Tornado']),
                td([average_delay($clear_flights, $clear_delays),
                    average_delay($fog_flights, $fog_delays),
                    average_delay($rain_flights, $rain_delays),
                    average_delay($snow_flights, $snow_delays),
                    average_delay($hail_flights, $hail_delays),
                    average_delay($thunder_flights, $thunder_delays),
                    average_delay($tornado_flights, $tornado_delays)])])),
    p({-style=>"bottom-margin:10px"})
    ;

print end_html;
