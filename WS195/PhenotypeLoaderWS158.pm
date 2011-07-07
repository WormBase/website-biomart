package PhenotypeLoader;

use strict;
use warnings;
use Data::Dumper qw( Dumper );

use WormMartTools;

use vars qw( @ISA @EXPORT_OK $CONNECTION $CONFIG );

@ISA = qw( Exporter );

@EXPORT_OK = qw( $CONNECTION $CONFIG );            

$CONNECTION->{ACE_HOST}    = undef(); # Def localhost
$CONNECTION->{ACE_PORT}    = undef(); # Def 23100
$CONNECTION->{ACE_USER}    = undef(); # Def euid user
$CONNECTION->{ACE_PASS}    = undef(); # Def empty

$CONNECTION->{MART_HOST}   = 'sarek'; # Def localhost
$CONNECTION->{MART_PORT}   = 3307;    # Def 3306
$CONNECTION->{MART_USER}   = 'root';  # Def euid user
$CONNECTION->{MART_PASS}   = undef(); # Def empty
$CONNECTION->{MART_DBNAME} = 'wormmart_146'; # No default


my $dataset    = 'phenotype';

my $focus_object = 'Phenotype';
my $ace_query    = '*'; # All 
#my $ace_query = 'Con'; # Test case
#my $ace_query = 'WT';  # Nasty test case
#my $ace_query = 'E*';  # A few test cases

$CONFIG = {
  "TBL_${dataset}__phenotype__main" => {

    OBJECTS => sub{
      my $ace_handle = shift;
      $ace_handle->fetch($focus_object=>$ace_query);
    },

    VAL_phenotype => sub{ $_[0]->name },


    #==================================================
    # Description
    VAL_description => sub{ &names_at($_[0],'Description') },
    VAL_info => sub{
      join( ' ', "[$_[0]]", (&names_at($_[0],'Description')||()) );
    },

    #==================================================
    # Assay
    VAL_assay => sub{ &names_at($_[0],'Assay') },


    #==================================================
    # Remark
    VAL_remark => sub{ &dmlist_at($_[0],'Remark') },


    #==================================================
    # Related_phenotypes
    
    #--------------------
    # Specialisation_of 
    &insert_dimension( 'Phenotype', 'Related_phenotypes.Specialisation_of',
                       $dataset, 'specialisation_of' ),
        
    #--------------------
    # Generalisation_of 
    &insert_dimension( 'Phenotype', 'Related_phenotypes.Generalisation_of',
                       $dataset, 'generalisation_of' ),

    #--------------------
    # Equivalent_to 
    &insert_dimension( 'Phenotype', 'Related_phenotypes.Equivalent_to',
                       $dataset, 'equivalent_to' ),

    #--------------------
    # Similar_to 
    &insert_dimension( 'Phenotype', 'Related_phenotypes.Similar_to',
                       $dataset, 'similar_to' ),
    
    #==================================================
    # Attribute_of

    #--------------------
    # RNAi
    &insert_dimension('RNAi','Attribute_of.RNAi',$dataset),
    

    #--------------------
    # GO_term
    &insert_dimension('GO_term','Attribute_of.GO_term',$dataset),

    #==================================================
    # DONE
    #==================================================
  }, 
};


#---
1;
