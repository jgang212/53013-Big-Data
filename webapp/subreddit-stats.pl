#!/usr/bin/perl -w
# Creates an html table of flight delays by weather for the given route

# Needed includes
use strict;
use warnings;
use 5.10.0;
use HBase::JSONRest;
use CGI qw/:standard/;
use Data::Dumper;

# Read the subreddit as CGI parameter
my $subreddit = param('subreddit');
 
# Define a connection template to access the HBase REST server
# If you are on out cluster, hadoop-m will resolve to our Hadoop master
# node, which is running the HBase REST server
my $hbase = HBase::JSONRest->new(host => "10.0.0.5:8082");

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

# Query hbase for the subreddit.
my $batch_records = $hbase->get({
  table => 'subreddit_stats_hbase',
  where => {
    key_equals => $subreddit
  },
});

my $speed_records = $hbase->get({
  table => 'speed_subreddit_stats_hbase',
  where => {
    key_equals => $subreddit
  },
});

sub combinedCellValue {
    my $field_name = $_[0];
    if(!@$speed_records && !@$batch_records) {
      return "missing";
    }
    my $result = 0;
    if(@$batch_records) {
	$result += cellValue(@$batch_records[0], $field_name);
    }
    if(@$speed_records) {
    	my $packed_value = cellValue(@$speed_records[0], $field_name);
    	if($packed_value ne "missing") {
    	    $result += unpack('Q>', $packed_value);
    	}
    }
    return $result;
}

# There will only be one record for this route, which will be the
# "zeroth" row returned
my $batch_row = @$batch_records[0];
my $speed_row = @$speed_records[0];

# Get the value of all the columns we need and store them in named variables
# Perl's ability to assign a list of values all at once is very convenient here
my($posts, $total_comments, $total_gilded, $total_score)
 =  (combinedCellValue('stats:posts'), combinedCellValue('stats:total_comments'),
     combinedCellValue('stats:total_gilded'), combinedCellValue('stats:total_score'));

# Given the number of posts and the sum of the various stats, this gives the average stats
sub average_stats {
    my($posts, $stats, $precision) = @_;
    return $posts > 0 ? sprintf("%.5f", $stats/$posts) : "-";
}

# Print an HTML page with the table. Perl CGI has commands for all the
# common HTML tags
print header, start_html(-title=>'hello CGI',-head=>Link({-rel=>'stylesheet',-href=>'/table.css',-type=>'text/css'}));
print div({-style=>'margin-left:350px;margin-right:auto;display:inline-block;box-shadow: 10px 10px 5px #888888;border:1px solid #000000;-moz-border-radius-bottomleft:9px;-webkit-border-bottom-left-radius:9px;border-bottom-left-radius:9px;-moz-border-radius-bottomright:9px;-webkit-border-bottom-right-radius:9px;border-bottom-right-radius:9px;-moz-border-radius-topright:9px;-webkit-border-top-right-radius:9px;border-top-right-radius:9px;-moz-border-radius-topleft:9px;-webkit-border-top-left-radius:9px;border-top-left-radius:9px;background:white'}, '&nbsp;Stats for the ' . $subreddit . ' subreddit&nbsp;');
print     p({-style=>"bottom-margin:10px"});
print table({-class=>'CSS_Table_Example', -style=>'width:60%;margin:auto;'},
	    Tr([td(['Total Posts', 'Comments/Post', 'Reddit Gold/Post', 'Average Score']),
                td([$posts,
                    average_stats($posts, $total_comments, 1),
                    average_stats($posts, $total_gilded, 5),
                    average_stats($posts, $total_score, 1)])])),
    p({-style=>"bottom-margin:10px"})
    ;

print end_html;
