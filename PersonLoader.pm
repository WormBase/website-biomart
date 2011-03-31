package PersonLoader;

use strict;
use warnings;
use WormMartTools;

use vars qw( @ISA @EXPORT_OK $CONNECTION $CONFIG $ACEDB );

@ISA = qw( Exporter );

@EXPORT_OK = qw( $CONNECTION $CONFIG get_config );            

$CONNECTION->{ACE_HOST}    = undef(); # Def localhost
$CONNECTION->{ACE_PORT}    = undef(); # Def 23100
$CONNECTION->{ACE_USER}    = undef(); # Def euid user
$CONNECTION->{ACE_PASS}    = undef(); # Def empty

$CONNECTION->{MART_HOST}   = 'sarek'; # Def localhost
$CONNECTION->{MART_PORT}   = 3307;    # Def 3306
$CONNECTION->{MART_USER}   = 'root';  # Def euid user
$CONNECTION->{MART_PASS}   = undef(); # Def empty
$CONNECTION->{MART_DBNAME} = ''; # No default


my $acedb_class = 'Person';
my $dataset     = 'wormmart_' . lc( $acedb_class );
my $ACE_QUERY   = '*'; # All 
# Examples?
#$ACE_QUERY = 'WBPerson1'; # First one
#$ACE_QUERY = 'WBPerson625'; # Paul Sternberg
#$ACE_QUERY = 'WBPerson46'; # Circular ancestory (Robert Barstead)

$CONFIG = undef; # Use &get_config instead

sub get_config{
  my $acedb  = shift || die "Need an ACE DB handle";
  my $CONFIG = &autogen_ace2mart_config( $acedb, $acedb_class, 
                                         $dataset, $ACE_QUERY );

  # Need to create the following dimensions;
  # Index_Ancestor_Person
  # Index_Descendent_Person

  my $main_table_name = "TBL_${dataset}__${dataset}__main";
  my $main_config = $CONFIG->{$main_table_name} || 
      die( "Cannot find $main_table_name" );
  
  # This is a simple person dimension, except that the objects are
  # created by recursion
  my %cols = ( Index_Descendent_Person => ['Lineage.Supervised'],
               Index_Ancestor_Person => ['Lineage.Supervised_by'], );


  foreach my $col( keys %cols ) {
    my @pos = @{$cols{$col}};
    my %conf_extra = 
        (
         "TBL_${dataset}__${col}__dm" => { 
           OBJECTS => sub{[ &recursive_cached_data_at($_[0],@pos) ]},
           VAL_paper          => &cached_val_sub('name'),
           VAL_cgc_name       => &cached_val_sub('cgc_name'),
           VAL_pmid           => &cached_val_sub('pmid'),
           VAL_brief_citation => &cached_val_sub('brief_citation'),
           IDX => ['paper','cgc_name','pmid'],
         },
         "VAL_${col}_dmlist" => sub{
           my %unique;
           return join( $WormMartTools::LIST_SEPARATOR,
                        map{ $unique{$_->{name}} ++ ? () : $_->{name} }
                        &recursive_cached_data_at($_[0],@pos) );
         },
         "VAL_${col}_dminfo" => sub{
           return join( $WormMartTools::LIST_SEPARATOR,
                        map{ $_->{info} || () }
                        &recursive_cached_data_at($_[0],@pos) );
         }
         );
    my $main = "TBL_${dataset}__${dataset}__main";
    map{ $CONFIG->{$main}->{$_} = $conf_extra{$_ } } keys ( %conf_extra );
  }

  return $CONFIG;
}


#----------------------------------------------------------------------
1;
