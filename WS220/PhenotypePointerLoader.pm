package PhenotypePointerLoader;

use strict;
use warnings;
use Data::Dumper qw( Dumper );
use WormMartTools;

use vars qw( @ISA @EXPORT_OK $CONNECTION $CONFIG );

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

my $acedb_class = 'phenotype';
my $dataset     = 'wormbase_phenotype_pointer';

my $ACE_QUERY   = 'WBPhenotype*'; # WBPhenotype only, i.e. ignore old          
#$ACE_QUERY = 'WBPhenotype0000583'; # Dpy Test case
#$ACE_QUERY = 'WBPhenotype0001179'; # WT Nasty test case
#$ACE_QUERY = 'WBPhenotype000058*';  # A few test cases
#$ACE_QUERY = 'WBPhenotype0000102';

$CONFIG = undef; # Use &get_config instead

sub get_config{
  my $acedb  = shift || die "Need an ACE DB handle";

  #====================
  # Build config
  my $main = "TBL_${dataset}__${acedb_class}__main";
  my $main_conf = {};
  $CONFIG = { $main => $main_conf };

  #--------------------
  # Skip the WT phenotype; it tends to cause core dumps...
  $main_conf->{OBJECTS} = sub{
    my $ace_handle = shift;
    my @last_objs; 
    return [ grep{$_ ne 'WBPhenotype0001179'} 
             $ace_handle->fetch( $acedb_class => $ACE_QUERY ) ];
  };


  #--------------------
  # Main table columns
  $main_conf->{"VAL_phenotype"} = sub{ map{return $_->{name} } 
                                       &object_data_cache($_[0]) };
  foreach my $col( qw( primary_name label description info ) ){
    $main_conf->{"VAL_${col}"} = sub{ map{return $_->{$col} }
                                      &object_data_cache($_[0]) };
  }
  $main_conf->{"IDX"} = ['phenotype','primary_name'];
  
  my %short_name = &insert_simple_dimension
      ('Name.Short_name',$dataset, 'Name_ShortName_PhenotypeName');
  foreach my $key( keys %short_name ){
    ##$main_conf->{$key} = %short_name{$key};
      $main_conf->{$key} = $short_name{$key};
  }
  
  #--------------------
  # Create indexes
  # Index.Ancestor   -> phenotype__Index_Ancestor_Phenotype__dm
  # Index.Descendent -> phenotype__Index_Descendent_Phenotype__dm
  foreach my $direction( 'Ancestor','Descendent' ){
    $main_conf->{"TBL_${dataset}__Index_${direction}_Phenotype__dm"} = {
      OBJECTS => sub{[ &phenotype_index_data( $_[0], $direction ) ]},
      VAL_phenotype    => &cached_val_sub('name'),
      VAL_primary_name => &cached_val_sub('primary_name'),
      #VAL_short_name   => &cached_val_sub('short_name'), # Can be multiple val
      VAL_label        => &cached_val_sub('label'),
      VAL_description  => &cached_val_sub('description'),
      VAL_info         => &cached_val_sub('info'),
      IDX => ['phenotype','primary_name'],
    };
  }

  # All done!
  return $CONFIG;
}

#----------------------------------------------------------------------
our %PHENOTYPE_INDEX;
sub phenotype_index_data{
  my $phen_obj  = shift;
  my $direction = shift;
  if( $PHENOTYPE_INDEX{$phen_obj}{$direction} ){
    return @{$PHENOTYPE_INDEX{$phen_obj}{$direction}}
  }

  # Avoid circularity=>deep recursion
  if( exists($PHENOTYPE_INDEX{$phen_obj}{$direction} )){
    # CIRCULAR!
    warn( "Already seen $phen_obj. Circular?" );
    return ();
  }
  $PHENOTYPE_INDEX{$phen_obj}{$direction} = undef;

  # Still here - compile indexes
  my @adjacent_phen_objs = 
      ( $direction eq 'Ancestor' 
        ? &cached_at( $phen_obj, 'Related_phenotypes.Specialisation_of' )
        : &cached_at( $phen_obj, 'Related_phenotypes.Generalisation_of' ) );
  
  my @index_phen_data = ( &object_data_cache( $phen_obj ) );
  foreach my $phen( @adjacent_phen_objs ){
    push @index_phen_data, &phenotype_index_data( $phen, $direction );
  }

  # Uniquify;
  my %seen;
  @index_phen_data = grep{ ! $seen{$_->{name}} ++ } @index_phen_data;
  # Update cache;
  $PHENOTYPE_INDEX{$phen_obj}{$direction} = [@index_phen_data];
  return @index_phen_data;
}

#---
1;
