#!perl -T
use 5.006;
use strict;
use warnings;
use Test::More;

plan tests => 1;

BEGIN {
    use_ok( 'Data::LoggerSQL' ) || print "Bail out!\n";
}

diag( "Testing Data::LoggerSQL $Data::LoggerSQL::VERSION, Perl $], $^X" );
