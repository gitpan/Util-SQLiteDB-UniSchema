#!perl -T

use Test::More tests => 1;

BEGIN {
	use_ok( 'Util::SQLiteDB::UniSchema' );
}

diag( "Testing Util::SQLiteDB::UniSchema $Util::SQLiteDB::UniSchema::VERSION, Perl $], $^X" );
