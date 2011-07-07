package PhenotypeLoader;

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
my $dataset     = 'wormbase_' . lc( $acedb_class );

my $ACE_QUERY   = 'WBPhenotype*'; # WBPhenotype only, i.e. ignore old          
#$ACE_QUERY = 'WBPhenotype0000583'; # Dpy Test case
#$ACE_QUERY = 'WBPhenotype0001179'; # WT Nasty test case
#$ACE_QUERY = 'WBPhenotype000058*';  # A few test cases
#$ACE_QUERY = 'WBPhenotype0000102';

$CONFIG = undef; # Use &get_config instead

sub get_config{
  my $acedb  = shift || die "Need an ACE DB handle";
  $CONFIG = &autogen_ace2mart_config( $acedb, $acedb_class, 
                                      $dataset, $ACE_QUERY );

  #====================
  # Custom alterations to config
  my $main = "TBL_${dataset}__${acedb_class}__main";
  my $main_conf = $CONFIG->{$main};

  #--------------------
  # Process the WT phenotype last; it tends to cause core dumps...
  $main_conf->{OBJECTS} = sub{
    my $ace_handle = shift;
    my @last_objs; 
    #my @objs = ( sort{ ( $b eq 'WBPhenotype0001179' ) ? -1 : 1 } 
    #             $ace_handle->fetch( $acedb_class => $ACE_QUERY ) );
    return [ grep{$_ ne 'WBPhenotype0001179'} 
             $ace_handle->fetch( $acedb_class => $ACE_QUERY ) ];
  };


  #--------------------
  # Create indexes
  # Index.Ancestor   -> phenotype__Index_Ancestor_Phenotype__dm
  # Index.Descendent -> phenotype__Index_Descendent_Phenotype__dm
  # Index_Ancestor_Phenotype_count
  # Index_Ancestor_Phenotype_dminfo
  # Index_Ancestor_Phenotype_dmlist
  # Index_Descendent_Phenotype_count
  # Index_Descendent_Phenotype_dminfo
  # Index_Descendent_Phenotype_dmlist
  foreach my $direction( 'Ancestor','Descendent' ){
    $main_conf->{"TBL_${dataset}__Index_${direction}_Phenotype__dm"} = {
      OBJECTS => sub{[ &phenotype_index_data( $_[0], $direction ) ]},
      VAL_phenotype    => &cached_val_sub('name'),
      VAL_primary_name => &cached_val_sub('primary_name'),
      VAL_short_name   => &cached_val_sub('short_name'),
      VAL_label        => &cached_val_sub('label'),
      VAL_description  => &cached_val_sub('description'),
      VAL_info         => &cached_val_sub('info'),
      IDX => ['phenotype','primary_name'],
    };
    $main_conf->{"VAL_Index_${direction}_Phenotype_dmlist"} = sub{
      join( $WormMartTools::LIST_SEPARATOR,
            map{ $_->{phenotype} || () } 
            &phenotype_index_data( $_[0], $direction ))
    };
    $main_conf->{"VAL_Index_${direction}_Phenotype_dminfo"} = sub{
      join( $WormMartTools::LIST_SEPARATOR,
            map{ $_->{info} || () }
            &phenotype_index_data( $_[0], $direction ))
    };
  }

  #--------------------
  # Set some counts to 0, not NULL (the default).
  $main_conf->{"VAL_AttributeOf_RNAi_count"} = sub{
    return scalar(cached_data_at($_[0],'Attribute_of.RNAi'));
  };
  $main_conf->{"VAL_AttributeOf_Variation_count"} = sub{
    return scalar( &names_at($_[0],'Attribute_of.Variation' ) );
  };
  $main_conf->{"VAL_AssociatedWith_GOTerm_count"} = sub{
    return scalar( &names_at($_[0],'Associated_with.GO_term' ) );
  };
    



  #--------------------
  # Get observed/unobserved info from RNAi
  $main_conf->{"VAL_AttributeOf_RNAi_observed_count"} = sub{
    my @obs_rnai;
    foreach my $rnai( cached_data_at($_[0],'Attribute_of.RNAi') ){
      my $phen = &_get_phen_info( $_[0], $rnai );
      push @obs_rnai, $rnai if $phen->{PhenotypeInfo_Observed};
    }
    return scalar( @obs_rnai );
  };
  $main_conf->{"VAL_AttributeOf_RNAi_observed_dmlist"} = sub{
    my @obs_rnai;
    foreach my $rnai( cached_data_at($_[0],'Attribute_of.RNAi') ){
      my $phen = &_get_phen_info( $_[0], $rnai );
      push @obs_rnai, $rnai if $phen->{PhenotypeInfo_Observed};
    }
    return join( $WormMartTools::LIST_SEPARATOR,
                 map{ $_->{name} || () } @obs_rnai );
  };
  $main_conf->{"VAL_AttributeOf_RNAi_observed_dminfo"} = sub{
    my @obs_rnai;
    foreach my $rnai( cached_data_at($_[0],'Attribute_of.RNAi') ){
      my $phen = &_get_phen_info( $_[0], $rnai );
      push @obs_rnai, $rnai if $phen->{PhenotypeInfo_Observed};
    }
    return join( $WormMartTools::LIST_SEPARATOR,
                 map{ $_->{info} || () } @obs_rnai );
  };

  $main_conf->{"VAL_AttributeOf_RNAi_unobserved_count"} = sub{
    my @obs_rnai;
    foreach my $rnai( cached_data_at($_[0],'Attribute_of.RNAi') ){
      my $phen = &_get_phen_info( $_[0], $rnai );
      push @obs_rnai, $rnai unless $phen->{PhenotypeInfo_Observed};
    }
    return scalar( @obs_rnai );
  };
  $main_conf->{"VAL_AttributeOf_RNAi_unobserved_dmlist"} = sub{
    my @obs_rnai;
    foreach my $rnai( cached_data_at($_[0],'Attribute_of.RNAi') ){
      my $phen = &_get_phen_info( $_[0], $rnai );
      push @obs_rnai, $rnai unless $phen->{PhenotypeInfo_Observed};
    }
    return join( $WormMartTools::LIST_SEPARATOR,
                 map{ $_->{name} || () } @obs_rnai );
  };
  $main_conf->{"VAL_AttributeOf_RNAi_unobserved_dminfo"} = sub{
    my @obs_rnai;
    foreach my $rnai( cached_data_at($_[0],'Attribute_of.RNAi') ){
      my $phen = &_get_phen_info( $_[0], $rnai );
      push @obs_rnai, $rnai unless $phen->{PhenotypeInfo_Observed};
    }
    return join( $WormMartTools::LIST_SEPARATOR,
                 map{ $_->{info} || () } @obs_rnai );
  };

  # ...and the dimension table
  my $rnai_tbl = $main_conf->{"TBL_${dataset}__AttributeOf_RNAi__dm"};
  $rnai_tbl->{"VAL_observed"} = sub{
    my $rnai_data = $_[0];
    my $phen_obj = $_[1];
    my $phen_data = &_get_phen_info( $phen_obj, $rnai_data );
    return $phen_data->{PhenotypeInfo_Observed} ? 'observed' : 'unobserved';
  };

  #--------------------
  # Create info and label fields in the main table for dropdown
  $main_conf->{"VAL_label"} = sub{ map{return $_->{label} } 
                                   &object_data_cache($_[0]) };
  $main_conf->{"VAL_info"}  = sub{ map{return $_->{info} } 
                                   &object_data_cache($_[0]) };

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

#----------------------------------------------------------------------
# Gets the phenotype info data corresponding to the given phenotype from the 
# given RNAi data hash.
sub _get_phen_info{
  my $phen_obj = shift;
  my $rnai_data = shift;  
  foreach my $phen_data( @{ $rnai_data->{phenotype} || [] } ) {
    if( $phen_data->{name} eq $phen_obj ){
      return $phen_data;
    }
  }
  return {};
}

#---
1;
