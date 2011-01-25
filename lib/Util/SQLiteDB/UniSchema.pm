=head1 NAME

Util::SQLiteDB::UniSchema - Interface to a single-schema SQLite DB

=cut
package Util::SQLiteDB::UniSchema;

=head1 VERSION

This documentation describes version 0.01

=cut
use version;      our $VERSION = qv( 0.01 );

use warnings;
use strict;
use Carp;

use DBI;

=head1 SCHEMA
 
 key TEXT NOT NULL,
 value TEXT,
 PRIMARY KEY ( key )

=cut
our $SQLITE = '/usr/bin/sqlite3';
our @SCHEMA =
(
    key   => 'TEXT NOT NULL PRIMARY KEY',
    value => 'TEXT',
);

=head1 SYNOPSIS
 
 use Util::SQLiteDB::UniSchema;
 
 my $file = '/db/file/path';
 my @table = qw( table1 table2 .. );

 my $db = Util::SQLiteDB::UniSchema->new( $file, table => \@table );
 my $db_existing = Util::SQLiteDB::UniSchema->new( $file );

 $db->set( 'table1', 'key1', 'value1' );

 my %keyval = $db->dump( 'table2' );

 map { $db->delete( $_, key => 'key1', val => 'value2' ) } $db->table();

 $db->truncate( 'table2' );

 my $quoted = $db->quote( "bob loblaw's law blog" )

=cut
sub new
{
    my ( $class, $db, %param ) = @_;
##  connect to DB
    my %config = ( RaiseError => 1, PrintWarn => 0, PrintError => 0 );
    my $dbh = DBI->connect( "DBI:SQLite:dbname=$db", '', '', \%config );

    croak "open $db: $!" unless open my $fh, '<', $db;
##  create schema
    my $table = $param{table};
    my @table;
    my $this = bless +{ sth => {}, dbh => $dbh, fh => $fh },
        ref $class || $class;

    if ( $table )
    {
        $dbh->disconnect;

        my $sqlite = $param{sqlite} || $SQLITE;
        my $handle;
        my $schema = join "\n", $this->_schema( @table = @$table );    
        
        croak $! unless open( $handle, "| $sqlite $db > /dev/null" )
            && length $schema == ( syswrite( $handle, $schema ) || 0 );

        close $handle;

        $this->{dbh} =
            DBI->connect( "DBI:SQLite:dbname=$db", '', '', \%config );
    }
    else
    {
        $table = $dbh->table_info( undef, undef, undef, 'TABLE' )
            ->fetchall_hashref( 'TABLE_NAME' );
        @table = keys %$table;
    }
##  prepare statement handle
    map { $this->_statement( $_ ) } @table;
    return $this;
}

=head2 set( table, @key, @value )

INSERT or UPDATE keys and values into table. Returns status of operation.

=cut
sub set
{
    my ( $this, $table ) = splice @_, 0, 2;
    my $result = $this->_execute( $table, 'insert', @_ );
}

=head2 dump( table )

Dump all records from a table into a HASH.
Returns HASH reference in scalar context.
Returns flattened HASH in list context.

=cut
sub dump
{
    my ( $this, $table ) = @_;
    my ( $result, $sth ) = $this->_execute( $table, 'select_all' );
    my %result = map { @$_ } @{ $sth->fetchall_arrayref() } if $result;

    return wantarray ? %result : \%result;
}

=head2 delete( table, delete_key => key, delete_val => value )

Deletes by key, or by value, or by key or value from a table.

=cut
sub delete
{
    my ( $this, $table, %param ) = @_;

    map { $this->_execute( $table, 'delete_' . $_, $param{$_} )
        if defined $param{$_} } qw( key val );
}

=head2 truncate( table )

Deletes all records from a table.

=cut
sub truncate
{
    my ( $this, $table ) = @_;
    my $result = $this->_execute( $table, 'truncate' );
}

=head2 quote( string )

See DBI::quote().

=cut
sub quote
{
    my $this = shift @_;
    $this->{dbh}->quote( @_ );
}

=head2 table()

Returns a list of all tables. Returns ARRAY reference in scalar context.

=cut
sub table
{
    my $this = shift @_;
    my @table = keys %{ $this->{sth} };

    return wantarray ? @table : \@table;
}

=head2 stat()

Stat of database file. Also see stat().
Returns ARRAY reference in scalar context.

=cut
sub stat
{
    my $this = shift @_;
    my @stat = stat $this->{fh};

    return wantarray ? @stat : \@stat;
}

sub _statement
{
    my ( $this, $table ) = @_;

    my @attr = $this->_attribute();
    my $key = join ',', @attr;
    my $val = join ',', map { '?' } @attr; 

    my %op =
    (
        truncate => 'DELETE FROM %s',
        select_all => 'SELECT * FROM %s',
        insert => "INSERT OR REPLACE INTO %s ($key) VALUES ($val)",
    );

    map { $op{ 'delete_'.$_ } = "DELETE FROM %s WHERE $_ = ?" } @attr;

    my $dbh = $this->{dbh};
    my $sth = $this->{sth}{$table} ||= {};
    my $neat = DBI::neat( $table );

    map { $sth->{$_} = $dbh->prepare( sprintf $op{$_}, $neat ) } keys %op;
}

sub _execute
{
    my ( $this, $table, $name ) = splice @_, 0, 3;
    my $handle = $this->{sth};

    return unless $table && $handle->{$table};

    my ( $sth, $result ) = $handle->{$table}{$name};

    while ( $sth )
    {   
        $result = eval { $sth->execute( @_ ) };
        last unless $@;
        croak $@ if $@ !~ /locked/;
    }

    return $result, $sth;
}

sub _attribute
{
    my $class = shift @_;
    my @attr = map { $SCHEMA[ $_ << 1 ] } 0 .. @SCHEMA / 2 - 1;

    return wantarray ? @attr : \@attr;
}

sub _schema
{
    my $class = shift @_;
    my @schema;

    for ( my $i = 0; $i < @SCHEMA; )
    {
        push @schema, sprintf '%s %s', @SCHEMA[ $i ++, $i ++ ];
    }

    my $schema = join ",\n", map { sprintf '    %s', $_ } @schema;

    map { sprintf "CREATE TABLE %s (\n%s\n);\n", DBI::neat( $_ ), $schema } @_;
}

=head1 SEE ALSO

DBI

=head1 AUTHOR

Kan Liu

=head1 COPYRIGHT and LICENSE

Copyright (c) 2010. Kan Liu

This program is free software; you may redistribute it and/or modify
it under the same terms as Perl itself.

=cut

1;

__END__
