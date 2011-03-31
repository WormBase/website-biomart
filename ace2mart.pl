#!/usr/bin/env perl

=head1 NAME

ace2mart.pl - Populates a BioMart database from data in an AceDB database

=head1 SYNOPSIS

perl ace2mart.pl [options] config_module(s)

Options:
 --help
 --info
 --verbosity
 --ace_host
 --ace_port
 --ace_user
 --ace_pass
 --mart_host
 --mart_port
 --mart_user
 --mart_pass
 --mart_dbname
 --no
 --no_insert
 --no_create
 --no_index
 --force_recreate
 --resume_gene

=head1 OPTIONS

Populates a BioMart database from data in an AceDB database. Requires
one or more Perl B<config_module> that maps data in the AceDB schema into
appropriate tables in the WormMart schema. See make_wormmart.pm
--info for more details.

B<--help>
  Print a brief help message and exits.

B<--info>
  Print man page and exit

B<--verbosity>
  Verbosity of output in range 0-3 (def 1)

B<--ace_host>
  Hostname of wormbase source ACEDB server (def localhost)

B<--ace_port>
  Port of wormbase source ACEDB server (def 23100)

B<--ace_user>
  User for source ACEDB server (def invoking user)

B<--ace_pass>
  User for source ACEDB server (def invoking user)

B<--mart_db>
  Name of BioMart MySQL database to write to (required)

B<--mart_host>
  Hostname of target MySQL server (def localhost)

B<--mart_port>
  Port of target MySQL server (def 3306)

B<--mart_user>
  User for target MySQL server (def invoking user)

B<--mart_pass>
  User password target MySQL server (def empty)

B<--no_insert>
  Don't write data to the mart database. Useful for debug.

B<--no_create>
  Don't try to create tables in the Mart database

B<--no_index>
  Don't index tables in the Mart database

B<--no>
  Implies --no_insert, --no_create, --no_index. I.e. no DB work at all!

B<--force_recreate>
  If loading into an already populated dataset, this option forces the
  removal of the existing dataset without prompting

B<--resume_gene>
  If the script died during GeneLoader, you can safely resume the script
  at the given WBGeneID

=head1 DESCRIPTION

B<This program> 

Loads WormBase gene data into a BioMart-schema database

The B<config_module> exports two things; the Ace and Mart DB
B<connection properties>, and the main B<CONFIGURATION> specification
for mapping data from Ace to Mart schema.

B<connection properties> include the host, port, user and pass for the
Ace and Mart database servers to connect to. Mart includes an
additional dbname property. All of these settings can be overridden
by the command line options. All B<config_module> files _MUST_ export
a $CONNECTIONS hashref, even if empty.


B<CONFIGURATION> data is a hash-based heirarchy
that associates BioMart attributes extracted from an acedb database
with a MySQL table schema.  Hash keys are one of;

   a table name (TBL_ prefix), whose value is a hashref indicating 
   a descent in the heirarchy,

   the 'OBJECT' string, whose value is a coderef that returns a list
   of objects for this heirarchy when passed an object from the parent 
   heirarchy (default top-level object is an acedb handle),

   an attribute name (VAL_ prefix), whose value is a coderef that 
   returns the attribute's value when passed the corresponding OBJECT
   and parent OBJECT.

   an attribute name (COL_ prefix), whose value is the mysql create 
   deffinition for the attribute's column.

   the 'IDX' string, whose value is an arrayref of attributes to index,
   multi-column indexes are represented by an arrayref within the 
   arrayref. Note that many indexes (primary/foreign keys) are indexed
   by default.

 The config is now defined and exported from a seperate module that
 is indicated by the script's calling parameters. So...
 Load the CONFIG from the config module

Maintained by Will Spooner <whs@ebi.ac.uk>

=cut
#======================================================================

package ace2mart;
use lib "/home/acabunoc/extlib/lib/perl5";
use strict;
use warnings;

use Pod::Usage;
use Getopt::Long;
use Data::Dumper qw(Dumper);
$Data::Dumper::Indent = 1;
#use Devel::Size;

use DBI;
use Ace;
use Ace::Sequence;
use constant NULL => 'NULL';
use WormMartTools;

#our $COL_PRIMARY_KEY   = qq(int(10) unsigned unique NOT NULL);
our $COL_FOREIGN_KEY   = qq(int(10) unsigned NOT NULL);
our $COL_COUNT         = qq(int(10) unsigned default NULL);

our $MAX_CACHE_OBJECTS = 40000; # Maximum number of objects in the cache

our $NO_INSERT;
our $FORCE;
our $VERBOSITY;
our $ACEDB;

my %TABLE_ROWS;

MAIN:{
  my $H;
  my $I;
  my $ace_host,
  my $ace_port,
  my $ace_user,
  my $ace_pass,
  my $mart_dbname,
  my $mart_host,
  my $mart_port,
  my $mart_user;
  my $mart_pass;
  my $no_create;
  my $no_index;
  my $no_;
  my $resume_gene;

  my $opts = GetOptions
      (
       "help"           => \$H,
       "info"           => \$I,
       "verbosity=i"    => \$VERBOSITY,
       "ace_host=s"     => \$ace_host,
       "ace_port=i"     => \$ace_port,
       "ace_user=s"     => \$ace_user,
       "ace_pass=s"     => \$ace_pass,
       "mart_dbname=s"  => \$mart_dbname,
       "mart_host:s"    => \$mart_host,
       "mart_port:i"    => \$mart_port,
       "mart_user:s"    => \$mart_user,
       "mart_pass:s"    => \$mart_pass,
       "no_insert"      => \$NO_INSERT,
       "no_index"       => \$no_index,
       "no_create"      => \$no_create,
       "no"             => \$no_,
       "force_recreate" => \$FORCE,
       "resume_gene=s"    => \$resume_gene,
       )|| pod2usage(2);

  pod2usage(-verbose => 2) if $I;
  pod2usage(1) if $H;

  defined( $VERBOSITY ) or  $VERBOSITY =1;
  defined( $NO_INSERT ) and $NO_INSERT =1;
  defined( $no_index  ) and $no_index  =1;
  defined( $no_create ) and $no_create =1;
  defined( $no_       ) and ( $NO_INSERT,$no_index,$no_create ) = (1,1,1);
  if ( defined ($resume_gene) ){
    $no_create = 1;
  }

  @ARGV || ( warn( "\nMissing: config_module\n\n"  ) && pod2usage(2) );
  my @config_modules= @ARGV;

  foreach my $config_module( @config_modules ){

    # Load the config_module
    $config_module =~ s/.pm$//;
    eval "require $config_module";
    if( $@ ){ die( "[*DIE] $@\n" ) }
    use vars qw( $CONNECTION $CONFIG );
    $config_module->import(qw( $CONNECTION $CONFIG ));
    # Sort ace/mart connection parameters
    $ace_host    ||= $CONNECTION->{ACE_HOST} ||= 'localhost';
    $ace_port    ||= $CONNECTION->{ACE_PORT} ||= '23100';
    $ace_user    ||= $CONNECTION->{ACE_USER} ||= getpwuid($>);
    $ace_pass    ||= $CONNECTION->{ACE_PASS} ||= undef;
    $mart_host   ||= $CONNECTION->{MART_HOST} ||= 'localhost';
    $mart_port   ||= $CONNECTION->{MART_PORT} ||= '3306';
    $mart_user   ||= $CONNECTION->{MART_USER} ||= getpwuid($>);
    $mart_pass   ||= $CONNECTION->{MART_PASS} ||= undef;
    $mart_dbname ||= $CONNECTION->{MART_DBNAME} ||
        ( warn( "Missing --mart_dbname\n"  ) && pod2usage(2) );

    # Connect to AceDB server
    #Ace->debug(1);
    $ACEDB = Ace->connect(-host=>$ace_host,-port=>$ace_port,-user=>$ace_user,
                          #-cache=>{} 
                          )
        || die( "[*DIE] AceDB Connection failure ".
                "$ace_user\@$ace_host:$ace_port: ",
                Ace->error."\n");
    if( $VERBOSITY > 1 ){ $ACEDB->debug( $VERBOSITY ) }
    my $dbname = $ACEDB->title;
    my $dbvers = $ACEDB->version;
    info( 1, "AceDB: Connected to $ace_user\@$ace_host:$ace_port ".
          "($dbname, $dbvers)" );
    
    # Connect to Mart DB server (mysql only for now)
    my $dsn = "DBI:mysql:$mart_dbname;$mart_host;$mart_port";
    my $mart_dbh = DBI->connect($dsn, $mart_user, $mart_pass,
                                {RaiseError=>0,PrintError=>0} )
        or die( "[*DIE] Mart DB Connection failure". $DBI::errstr."\n");
    info( 1, "Mart:  Connected to $mart_user\@$mart_host:$mart_port ".
          "($mart_dbname)" );
    
    # In the original implementation, $CONFIG was a package variable defined
    # in the ClassLoader module. Now we move to auto-generate $CONFIG based
    # on the ACE DB schema, so we need a run-time method to manage this.
    unless( $CONFIG ){
      $config_module->import(qw( get_config ));
      $CONFIG = &get_config($ACEDB);
    }
    
    # Validate database schema
    if( !$no_create ){
      create_mart_tables($mart_dbh,$CONFIG);
    } else { #get table key values
       my $tables = $mart_dbh->selectall_arrayref("SHOW TABLES");
       foreach my $tbl (@$tables){
         $tbl = $tbl->[0];
         my $sth = $mart_dbh->prepare("SELECT count(*) FROM $tbl");
         $sth->execute();
         my $count = $sth->fetchrow_array();
         $sth->finish();
         $TABLE_ROWS{"TBL_$tbl"} = $count;
       }
    }
    
    foreach my $top_table( keys %$CONFIG ){
      if( $top_table !~ /^TBL_/ ){ next }
      my $config = $CONFIG->{$top_table};
      
      # The $config->{'OBJECTS'} setting can return
      # A. a code ref that can be called to return a list of AcePerl objects
      # B. a code ref that returns an AcePerl iterator

      my $code = $config->{'OBJECTS'} || 
          die( "[*DIE] could not find top-level objects" );
      my $objects_ref = &$code( $ACEDB ); 
      delete $config->{'OBJECTS'};    # Stop &fetch_data getting objects
my $create = 0;
      my $i = 0;
      if( ref( $objects_ref ) eq 'ARRAY' ){ # We have an array of objects
        my $num_objects = scalar( @$objects_ref );      
        while( my $obj = shift @$objects_ref ){ # Use shift to free memory 
          info( 1, "Processing obj ". ++$i ." of $num_objects $obj" );
          my $data = &fetch_data( $top_table, $config, $obj );
#           _test_print($data);
          &write_data( $mart_dbh, $data );
          $config->{'DESTROY'} && &{$config->{'DESTROY'}}($obj); # cleanup 
          &examine_cache($objects_ref);
        }
      }
      elsif( $objects_ref->isa('Ace::Iterator') ){ # Itterator
        while( my $obj = $objects_ref->next ){
          info( 1, "Processing obj ". ++$i ." (itterator) $obj" );

# use this code if something crashes while you're running geneLoader (likely to happen)
if(defined ($resume_gene)){
  if($obj->name eq $resume_gene){
    $create = 1;
  }
  next unless $create;
}
          my $data = &fetch_data( $top_table, $config, $obj );
          &write_data( $mart_dbh, $data );
          $config->{'DESTROY'} && &{$config->{'DESTROY'}}($obj); # cleanup 
          &examine_cache($objects_ref);
        }
      }
      else{ die( $objects_ref ) }
    }

    # All data collected...
    # Compress and add indexes to tables
    compress_tables($mart_dbh,$CONFIG) unless ($no_index);
    add_indices( $mart_dbh, $CONFIG ) if ! $no_index;

    info(0, "Completed $config_module");
#     foreach my $k (keys %TABLE_ROWS){
#       my $v = $TABLE_ROWS{$k};
#       info(0, "$k: $v");
#     }
  }
  info(0,"Completed ace2mart");
  exit 0;
}

#test printing
sub _test_print{
   my $data = shift;
    while( my ($k, $v) = each %$data ) {
        print "\n\n$k:";
        foreach my $line (@{$v}) {
          while( my ($k2, $v2) = each %$line ) {
            print "\n  $k2:";
            if($v2){ print $v2 }
          }
        }
    }
}

#----------------------------------------------------------------------
# Uses config data to query an AceDB object, using data to populate rows of
# BioMart tables. Self-referential to cope with heirarchical $CONFIG data.
# Arg[0] Name of the table to populate
# Arg[1] Reference to the portion of $CONFIG corresponding to the table
# Arg[2] AceDB object from the parent config heirarchy; used to get objects for
#        this heirarchy
#
# my %TABLE_ROWS;
sub fetch_data{
  my $table  = shift;
  my $config = shift;
  my $parent_obj = shift;
  my $pparent_obj = shift;
  my $key_field;

  if( $table =~ /^TBL_/ ){
    my @bits = split /__/, $table;        # E.g. ws136_gene_wb__gene__main 
    $key_field = $bits[-2]."_key";        # E.g. gene_key
    #$table = join( "__", @bits[-2..-1] ); # E.g. gene__main
  }
  else{ die( "[*DIE] cannot parse table name $table\n" ) }

  # Use parent object and code in config's 'OBJECTS' to get list of 
  # objects for this heirarchy 
  my @self_objects =  ($parent_obj); # Default to parent obj
  if( my $code = $config->{'OBJECTS'} ){
    my $obj_ref = &$code( $parent_obj, $pparent_obj );
    ref( $obj_ref ) eq 'ARRAY' 
        or die( "[*DIE] OBJECTS for $table did not return an ARRAY ref" );
    @self_objects = @{$obj_ref};
    unless( scalar( @self_objects ) ){ @self_objects = ( undef ) };
  }
#  if( ! scalar( @self_objects ) ){
#    # If no objects are found, we need to force an empty row in the table and
#    # child (e.g. dimension) tables to ensure referential integrity.
#    #@self_objects = ($parent_obj); 
#    @self_objects = ( undef );
#  };

  my $dm_data   = {}; # What we return at the end
  $dm_data->{$table} = []; # Initialise for this table
  foreach my $stub_obj( @self_objects ){ # 1 table row per object
    $TABLE_ROWS{$table} ++;
#     $TABLE_ROWS{$table} = $mart_dbh->
    my $obj = $stub_obj->fetch if( UNIVERSAL::isa( $stub_obj, "Ace::Object") );
    $obj ||= $stub_obj;
    my $obj_data = {};
    my $obj_dm_data = {};
    foreach my $attrib( keys %$config ){
      my $retriever = $config->{$attrib};
      if( $attrib =~ s/^VAL_// ){ # Strip prefix
        $obj_data->{$attrib} = &$retriever($obj,$parent_obj,$pparent_obj);
      }
      elsif( ref( $retriever ) eq 'HASH' ){ # RECURSE to next tbl in heirarchy
        my $data = fetch_data($attrib,$retriever,$obj,
                              $parent_obj,$pparent_obj);
        map{ $obj_dm_data->{$_} = $data->{$_} } keys %$data;
      }
    }
  
    # Determine counts and propogate primary key
    foreach my $dm_table( keys %$obj_dm_data ){
      my @bits = split('__', $dm_table);
      my $count_field .= $bits[-2]."_count";
      my $count = 0;
      my @dm_rows = @{$obj_dm_data->{$dm_table}};
      foreach my $rowref( @dm_rows ){
        if( scalar( grep{$_ !~ /(_key)|(_count)$/ and 
                             $rowref->{$_}} keys %$rowref ) ){ $count++ }
        $rowref->{$key_field} = $TABLE_ROWS{$table};
      }
      $obj_data->{$count_field} = $count;
      $dm_data->{$dm_table} ||= [];
      push @{$dm_data->{$dm_table}}, @dm_rows;
    }
    $obj_data->{$key_field} = $TABLE_ROWS{$table} if $table =~ /__main$/;
    push @{$dm_data->{$table}}, $obj_data; # Update record with own data

    # Copy data from this main table to conforming main table(s)
    # This step has to be done last, and probably only ever for genes/cds
    foreach my $table2( grep /__main$/, keys %$dm_data ){
      next if $table eq $table2; # Skip this table!
      foreach my $table2_row( @{$dm_data->{$table2}} ){ 
        # update each row of conforming main table with each field of
        # parent main table...
        foreach my $attrib( keys %$obj_data ){
          next if exists($table2_row->{$attrib}); # Skip if already populated
          $table2_row->{$attrib} = $obj_data->{$attrib}; # Copy attrib
        }
      }
    } # End conforming-main
    undef( $obj ); #reclaim memory
  }

  # Return the data
  return $dm_data;
}

#----------------------------------------------------------------------
# Checks that the tables indicated in $CONFIG are also in the Mart database.
# If the tables already contain data, user gets the chance to delete
# existing rows.
sub create_mart_tables{
  my $dbh       = shift;
  my $config    = shift;

  my $tables = $dbh->selectall_arrayref("SHOW TABLES");
  my %existing_tables = map{$_->[0]=>1} @$tables;
  my %schema_data = %{fetch_schema( $config )};

  # Do any of the tables for this dataset exist already?
  # If so, get rid!
  foreach my $tbl( sort{ reverse($b) cmp reverse($a) } keys %schema_data ){
    my @bits = split( "__", $tbl );
    if( exists($existing_tables{$tbl}) ){
      my $ds = $bits[0];
      if( $NO_INSERT ){
        warning(0,"Dataset $ds already exists");
      } 
      else{
        my $response = 'y';
        unless( $FORCE ){ # Prompt to drop existing dataset  
          warning(0,"Dataset $ds already exists. Recreate? [y/n]");
          $response = <STDIN>;
          chomp $response;
        }
        if( $response =~ /^y/i ){
          my @tables_to_drop;
          my $q1 = qq(SHOW TABLES LIKE "${ds}__%__main");
          my $q2 = qq(SHOW TABLES LIKE "${ds}__%__dm");
          push( @tables_to_drop,
                @{$dbh->selectall_arrayref($q1)},
                @{$dbh->selectall_arrayref($q2)} );
          foreach my $tbl( map{$_->[0]} @tables_to_drop ){
            $dbh->do( "DROP TABLE $tbl" ) or die( "[*DIE] $dbh->errstr" );
            warning(0,"Deleted table $tbl");
          }
        }
        else{
          die( "[*DIE] Writing data to a populated mart will result ".
               "in inconsistent data\n" );
        } 
      }
      last; # Only delete once!
    }
  }

  # Create the tables 
  foreach my $tbl( keys %schema_data ){
    &create_table( $dbh, $tbl, $schema_data{$tbl} );
  }

  return 1;
}

#----------------------------------------------------------------------

# Itterates through each table and sets the length of all BLOB fields to 
# the appropriate VARCHAR to hold their data.
sub compress_tables{
  my $dbh       = shift;
  my $config    = shift;
  info( 1, "compressing tables" );

  foreach my $tbl( values %{_find_tables( $config )} ){
    my $sth = $dbh->prepare( "DESCRIBE $tbl" );
    my $rv = $sth->execute || die( $sth->errstr );
    my $data = $sth->fetchall_arrayref({Field=>1,Type=>1,Null=>1,DEFAULT=>1});
    foreach my $row ( @{$data} ){
      #uc($row->{Type}) eq 'BLOB' or next;
      $row->{Type} =~ m/^((BLOB)|(INT))/i or next;
      my $type = uc($1);
      my $tq = "SELECT MAX( LENGTH( `$row->{Field}` ) ) from $tbl";
      my $tsth = $dbh->prepare( $tq );
      my $trv = $tsth->execute || die( $tsth->errstr );
      my $length = @{$tsth->fetchrow_arrayref}[0];
      if( ! $length ){
        # Warn on empty fields and drop column
        warning( 1, "$tbl.$row->{Field} contains no data! Dropping" );
        $length = 1;
next;
        my $aq = "ALTER TABLE $tbl DROP $tbl.$row->{Field}";
        my $asth = $dbh->prepare( $aq );
        $asth->execute || die( $asth->errstr ) unless $NO_INSERT;
        next;
      }
      if( $length > 255 ){
        # Skip long fields
        warning( 2, "$tbl.$row->{Field} is long ($length chars)" );
        next; 
      }
      info( 2, "Compressing $tbl.$row->{Field} to $length chars" );
      
      my $new_type = $row->{Type};
      if( $type eq 'BLOB' ){ $new_type =~ s/BLOB/VARCHAR($length)/i }
      if( $type eq 'INT'  ){ $new_type =~ s/\d+/$length/ }
      my $aq = "ALTER TABLE $tbl MODIFY $row->{Field} $new_type";
      warning( 3, $aq );
      my $asth = $dbh->prepare( $aq );
      $asth->execute || die( $asth->errstr ) unless $NO_INSERT;
    }
  }
  return 1;
}

#----------------------------------------------------------------------
# Adds indexes to the tables
sub add_indices{
  my $dbh       = shift;
  my $config    = shift;

  info( 1, "Adding indices..." );
  my $alter_sqlt = qq|
ALTER TABLE %s %s |;

  my $index_sqlt = qq|
ADD INDEX (%s) |;

  my $index_data = fetch_indices( $config );

  foreach my $tbl( keys %$index_data ){
    my @index_sql;
    foreach my $idx_ref( @{$index_data->{$tbl}} ){
      my @cols = @$idx_ref;
      push @index_sql, sprintf( $index_sqlt, join(',',@cols) );
    }

    if( ! @index_sql ){ warning( 1, "No indices for $tbl" ); next }

    info( 1, "Adding ".scalar(@index_sql)." indices to $tbl" );

    foreach my $ind_sql( @index_sql ){
      my $alter_sql = sprintf( $alter_sqlt, $tbl, $ind_sql);
      info( 2, "DB: $alter_sql" );
      if( $NO_INSERT ){ next }
      $dbh->do( $alter_sql ) || warning( 1, $dbh->errstr );
    }
  }
  return 1;
}


#----------------------------------------------------------------------
# 'Internal' method to collate all table names for a given config hash
sub _find_tables{
  my $conf   = shift;
  my %TABLE_NAMES;
  foreach my $key( grep /^TBL_/, keys %$conf ){
    my $tbl_conf = $conf->{$key};
    my $tbl = $key;
    $tbl =~ s/^TBL_//;
    %TABLE_NAMES = ( %TABLE_NAMES, 
                     $key => $tbl,
                     %{_find_tables($tbl_conf)} );
  }
  return {%TABLE_NAMES};
}

#----------------------------------------------------------------------
# Interrogates the config and returns a hash of hashes 
# where each top-level-hash key is the table name, each second-level-hash
# key is the column name, whose value is the SQL create deffinition
sub fetch_schema{
  my $config = shift;
  my %tables = ();

  foreach my $tbl( grep /^TBL_/, keys %$config ){

    my $tbl_conf = $config->{$tbl};
    $tbl =~ s/^TBL_//;

    my %child_tbls = ( %{fetch_schema($tbl_conf)} );
    my %this_tbl = ();

    # Apply primary key
    my $pkey;
    if( $tbl =~ /^\w+?__(\w+?)__main$/ ){
      $pkey = $1."_key";
      $this_tbl{$pkey} = $COL_FOREIGN_KEY;
    }

    # Apply explicit cols
    foreach my $col( grep /^VAL_/, keys %{$tbl_conf} ){
      $col =~ s/^VAL_/COL_/;
      my $col_sql = $tbl_conf->{$col};
      $col_sql ||= "blob default NULL"; #Default to large string
      #$col_sql || die("[*DIE] Bad config; $col missing from table $tbl");
      $col =~ s/^COL_//; # strip prefix
      $this_tbl{$col} = $col_sql;
    } 

    foreach my $child_tbl( keys %child_tbls ){
      # Apply 'count' fields
      my $count_col;
      if( $child_tbl =~ /\w+?__(\w+?)__\w+?$/ ){
        $this_tbl{$1."_count"} = $COL_COUNT;
      }
      # Apply foreign key
      if( $pkey ){
        $child_tbls{$child_tbl}->{$pkey} = $COL_FOREIGN_KEY;
      }
    }

    foreach my $child_main_tbl( grep /__main$/, keys %child_tbls ){
      # Copy main table cols to child-main table
      foreach my $col( keys %this_tbl ){
        next if $col eq $pkey;
        $child_tbls{$child_main_tbl}->{$col} = $this_tbl{$col};
      }
    }
    %tables = ( %tables, %child_tbls, $tbl=>\%this_tbl );
    
  }
  return {%tables};
}

#----------------------------------------------------------------------
# Interrogates the config and returns a hash of arrays of arrays
# where each hash key is the table name, each array is it's indices 
# and each sub-array contains the fields comprising an individual index
sub fetch_indices{
  my $config = shift;
  my %indices = ();

  foreach my $table( grep /^TBL_/, keys %$config ){

    my $tbl_conf = $config->{$table};
    $table =~ s/^TBL_//;

    my %tbl_idx = ( %{fetch_indices($tbl_conf)}, 
                    $table=>[] );

    # Apply primary/foreign key index to all tables in this group
    if( $table =~ /__main$/ ){ # Central table
      my $key_col;
      ( $key_col = $table ) =~ s/__main/_key/;
      $key_col =~ s/^\w+?__//;
      map{unshift @$_, [$key_col]} values %tbl_idx;
    }

    # Apply explicit indices. Can be an array, or array of arrays
    if( my $idx_ref = $tbl_conf->{IDX} ){
      my @idx = map{ref($_) eq 'ARRAY'? $_:[$_]} @{$idx_ref};
      push @{$tbl_idx{$table}}, @idx;
    }

    # Copy indexes to main hash
    %indices = ( %indices, %tbl_idx );
  }
  return {%indices};
}

#----------------------------------------------------------------------
# 
sub write_data{
  # Takes a hashref for the data corresponding to an object,
  # each key representing a table, and writes the data to the DB.
  my $mart_dbh = shift;
  my $data = shift;
  foreach my $table( keys %$data ){ 
    foreach my $row(  @{$data->{$table}||[]} ){
      $table =~ s/^TBL_//;
      &write_row_data( $mart_dbh, $table, $row );
    }
  }
}

sub write_row_data{
  my $dbh = shift;
  my $table_name = shift;
  my %data = %{shift @_};

  my $sql = qq(INSERT INTO $table_name SET %s);

  my @set_terms;
  foreach my $f( keys %data ){
    my $v = $data{$f} || next;
    push @set_terms, sprintf( '%s=%s', '`'.$f.'`', $dbh->quote($v) );
  }
  
  $sql = sprintf( $sql, join( ", ", @set_terms ) ); 
  info(2,"DB insert: $sql");
  if( $NO_INSERT ){ return 1 }; # Debug. Enable with --no_insert flag
  my $sth = $dbh->prepare( $sql );
  my $rv = $sth->execute || die( "$sql \n".$sth->errstr );
  return $rv;
}

#----------------------------------------------------------------------
#
sub create_table{
  my $dbh = shift;
  my $table_name = shift;
  my %columns    = %{shift @_};

  my $CREATE_TABLE_SQLT = "
CREATE TABLE %s ( %s 
) TYPE=MyISAM";

  my $CREATE_COL_SQLT = "
  %s %s";

  my @columns_sql;
  foreach my $col( sort keys %columns ){
    push @columns_sql, sprintf($CREATE_COL_SQLT, '`'.$col.'`', $columns{$col} );
  }
  my $column_sql = join ",", @columns_sql;
  my $table_sql = sprintf( $CREATE_TABLE_SQLT, $table_name, $column_sql );

  info( 2, "DB table $table_name scheduled for creation" ); 
  info( 2, "DB table: $table_sql" );

  if( $NO_INSERT ){ return 1 }
  my $sth = $dbh->prepare( $table_sql );
  my $rv = $sth->execute || die( $table_sql . '\n' . $sth->errstr );
  info( 1,"Table $table_name created" );
  return 1;
}

#----------------------------------------------------------------------
sub examine_cache{
  # Perfoms cache maintenance after each object is processed
  my $objects_ref = shift;
  if( $WormMartTools::ObjectCache_count > $MAX_CACHE_OBJECTS ){
    # Clear the cache if it gets too big
    WormMartTools::clear_large_caches($ACEDB, $MAX_CACHE_OBJECTS );
  }
  my $cache_debug = 0;
  if( $cache_debug ){
    # Need to use Devel::Size for this to work
    warn "OBJECTS  > ". Devel::Size::total_size($objects_ref);
    warn "COUNT    > ". $WormMartTools::ObjectCache_count;
    warn "OBJ_CACHE> ". Devel::Size::total_size
        ($WormMartTools::ObjectCache);
    foreach my $obj( keys %{$WormMartTools::ObjectCache} ){
      warn( "     _OBJ> $obj ".
            Devel::Size::total_size
            ( $WormMartTools::ObjectCache->{$obj} ));
    }
    warn "DAT_CACHE> ". Devel::Size::total_size
        ($WormMartTools::ObjectDataCache);
  }
  return 1;
}

#----------------------------------------------------------------------
sub info{
  my $v   = shift;
  my $msg = shift;
  if( ! defined($msg) ){ $msg = $v; $v = 0 }

  if( $v > $VERBOSITY ){ return 1 }
  warn( "[INFO] ".$msg."\n" );
  return 1;
}

#----------------------------------------------------------------------
sub warning{
  my $v   = shift;
  my $msg = shift;
  if( ! defined($msg) ){ $msg = $v; $v = 0 }
  $msg || ( carp("Need a warning message" ) && return );

  if( $v > $VERBOSITY ){ return 1 }
  warn( "[WARN] ".$msg."\n" );
  return 1;
}

#----------------------------------------------------------------------




