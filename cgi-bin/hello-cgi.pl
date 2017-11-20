#!/usr/bin/perl -w

use strict;
use warnings;
use 5.10.0;

use Data::Dumper;
use CGI qw/:standard/, 'Vars';

my $params = Vars;
print header, start_html('hello CGI'), Dumper($params), end_html;
