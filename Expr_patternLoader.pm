package Expr_patternLoader;

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

$CONNECTION->{MART_HOST}   = undef(); # Def localhost
$CONNECTION->{MART_PORT}   = undef(); # Def 3306
$CONNECTION->{MART_USER}   = undef(); # Def euid user
$CONNECTION->{MART_PASS}   = undef(); # Def empty
$CONNECTION->{MART_DBNAME} = undef(); # No default


my $dataset    = 'wormbase_expr_pattern';

my $focus_object = 'Expr_pattern';
my $ace_query    = '*'; # All 
#$ace_query = 'Expr1'; # Firs for debug 
#$ace_query = 'Expr?'; # First 9 for debug
#$ace_query = 'Expr??'; # First 99 for debug

$CONFIG = {
  "TBL_${dataset}__expr__main" => {

    OBJECTS => sub{
      my $ace_handle = shift;
      [ $ace_handle->fetch($focus_object=>$ace_query) ];
    },
    
    VAL_expr_pattern  => sub{ &name( $_[0], $focus_object ) },
    
    #==================================================
    # Expression_of

    #--------------------
    # Expression_of.Gene
    &insert_dimension('Gene','Expression_of.Gene',$dataset,'gene'),

    #--------------------
    # Expression_of.CDS
    &insert_simple_dimension('Expression_of.CDS', $dataset,
                             'cds'),

    #--------------------
    # Expression_of.Pseudogene
    &insert_simple_dimension('Expression_of.Pseudogene', $dataset,
                             'pseudogene'),

    #--------------------
    # Expression_of.Protein
    &insert_simple_dimension('Expression_of.Protein', $dataset,
                             'protein'),

    #--------------------
    # Expression_of.Clone
    &insert_simple_dimension('Expression_of.Clone', $dataset,
                             'clone'),

    #--------------------
    # Expression_of.Sequence
    &insert_simple_dimension('Expression_of.Sequence', $dataset,
                             'sequence'),

    #==================================================
    # Expressed_in

    #--------------------
    # Expressed_in.Cell
    &insert_simple_dimension('Expressed_in.Cell',$dataset,
                             'cell'),

    #--------------------
    # Expressed_in.Cell_group
    &insert_simple_dimension('Expressed_in.Cell_group',$dataset,
                             'cell_group'),

    #--------------------
    # Expressed_in.Life_stage
    &insert_simple_dimension('Expressed_in.Life_stage',$dataset,
                             'life_stage'),

    #--------------------
    # Expressed_in.Anatomy_term
    &insert_simple_dimension('Expressed_in.Anatomy_term',$dataset,
                             'anatomy_term'),

    #--------------------
    # Expressed_in.GO_term
    &insert_dimension('GO_term','Expressed_in.GO_term',
                      $dataset,'go_term'),

    #==================================================
    # Subcellular_localization
    &insert_simple_dimension('Subcellular_localization',$dataset,
                             'subcellular_loc'),

    #======================================================================
    # Type
    VAL_type_dmlist => sub{ &dmlist_at($_[0],"Type") },
    VAL_type_dminfo => sub{
      my @info;
      foreach my $type( &names_at($_[0],'Type') ){
        foreach my $text( &names_at($_[0],"Type.$type") ){
          push @info, join( ' ', "[$type]", ($text||()) );
        }
      }
      return join( " | ", @info );
    },
    "TBL_${dataset}__type__dm" => {
      OBJECTS => sub{ 
        my @objs;
        foreach my $type( &names_at($_[0],'Type') ){
          foreach my $text( &names_at($_[0],"Type.$type") ){
            my $info = "[$type] $text";
            push( @objs, {type=>$type,text=>$text,info=>$info} );
          }
        } 
        return [ @objs ];
      },
      VAL_type      => sub{ $_[0] ? ( $_[0]->{type} || undef ) : undef },
      VAL_type_text => sub{ $_[0] ? ( $_[0]->{text} || undef ) : undef },
      IDX => ['type'],
    },


    #==================================================
    # Pattern
    &insert_simple_dimension('Pattern',$dataset),

    #==================================================
    # Picture
    &insert_simple_dimension('Picture',$dataset),

    #==================================================
    # Remark
    &insert_simple_dimension('Remark',$dataset),

    #==================================================
    # Experiment
    &insert_dimension('Experiment','Experiment',$dataset ),

    #==================================================
    # Reference
    &insert_dimension('Paper','Reference',$dataset),

    #==================================================
    # Transgene
    &insert_simple_dimension('Transgene',$dataset),

    #==================================================
    # Antibody
    &insert_simple_dimension('Antibody_info',$dataset,'antibody'),

    #==================================================
    # Curated_by
    VAL_curated_by  => sub{ &names_at($_[0],'Curated_by') },

    #==================================================
    # INDEXES
    IDX => ['expr_pattern'],

  }, # End main
};



#---
1;
