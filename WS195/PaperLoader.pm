package PaperLoader;

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


my $dataset    = 'wormbase_paper';

my $focus_object = 'paper';
my $ace_query    = '*'; # All 
#$ace_query = 'WBPaper0000000?'; # First 9
#$ace_query = 'WBPaper000000??'; # First 99

$CONFIG = {
  "TBL_${dataset}__paper__main" => {
    
    OBJECTS => sub{
      my $ace_handle = shift;
      return[ grep{$_ ne 'WBPaper00030778'} 
              $ace_handle->fetch($focus_object=>$ace_query) ];
    },

    VAL_paper => sub{ &name($_[0]) },


    #==================================================
    # Original_timestamp
    # Skip
    
    #================================================== 
    # Name
    # Assume all UNIQUE (is this correct?)
    VAL_cgc_name         => sub{ &names_at($_[0],'Name.CGC_name') },
    VAL_pmid             => sub{ &names_at($_[0],'Name.PMID') },
    VAL_medline_name     => sub{ &names_at($_[0],'Name.Medline_name') },
    VAL_meeting_abstract => sub{ &names_at($_[0],'Name.Meeting_abstract') },
    VAL_wbg_abstract     => sub{ &names_at($_[0],'Name.WBG_abstract') },
    # Ignore Old_WBPaper and Other_name

    #==================================================
    # Nematode_paper
    # Skip
    
    #==================================================
    # Erratum

    #==================================================
    # Reference
    VAL_title     => sub{ &names_at($_[0],'Reference.Title') },
    VAL_journal   => sub{ &names_at($_[0],'Reference.Journal') },
    VAL_publisher => sub{ &names_at($_[0],'Reference.Publisher') },
    VAL_volume    => sub{ &names_at($_[0],'Reference.Volume') },
    VAL_year      => sub{ &names_at($_[0],'Reference.Year') },
    VAL_page      => sub{ 
      my $page = $_[0]->at('Reference.Page') || return;
      return join( "-", $page->right, ($page->right(2) || () ) );
    },

    #==================================================
    # Author
    VAL_author_dmlist => sub{ &dmlist_at( $_[0],"Author" ) },
    "TBL_${dataset}__author__dm" => {
      OBJECTS => sub{[ &cached_at($_[0],'Author') ]},
      VAL_author => sub{ &name( $_[0], 'Author' ) },
      VAL_person => sub{ &names_at( $_[0], 'Possible_person' ) },
    },

    #==================================================
    # Affiliation
    #VAL_affiliation => sub{ &names_at( $_[0], 'Affiliation' ) },

    #==================================================
    # Brief_citation
    VAL_brief_citation => sub{ &names_at( $_[0], 'Brief_citation' ) },
    
    #==================================================
    # Abstract
    # Skip

    #==================================================
    # Type
    VAL_type => sub{ uc(&names_at( $_[0], 'Type' )) },
    
    #==================================================
    # Contains
    # Skip

    #==================================================
    # Refers_to
    
    #--------------------
    # Gene
    &insert_dimension('Gene','Refers_to.Gene',$dataset,'gene'),

    #--------------------
    # Locus - DEPRECATED
    #&insert_simple_dimension('Refers_to.Locus',$dataset,'locus'),

    #--------------------
    # Allele
    # TODO Allele in WormMartTools
    &insert_simple_dimension('Refers_to.Allele',$dataset,'variation'),

    #--------------------
    # Rearrangement
    &insert_simple_dimension('Refers_to.Rearrangement',$dataset,
                             'rearrangement'),

    #--------------------
    # Sequence
    &insert_simple_dimension('Refers_to.Sequence',$dataset,'sequence'),

    #--------------------
    # CDS
    &insert_simple_dimension('Refers_to.CDS',$dataset,'cds'),

    #--------------------
    # Transcript
    &insert_simple_dimension('Refers_to.Transcript',$dataset,'transcript'),

    #--------------------
    # Pseudogene
    &insert_simple_dimension('Refers_to.Pseudogene',$dataset,'pseudogene'),

    #--------------------
    # Strain
    &insert_dimension('Strain','Refers_to.Strain',$dataset,'strain'),
    
    #--------------------
    # Clone
    &insert_simple_dimension('Refers_to.Clone',$dataset,'clone'),

    #--------------------
    # Protein
    &insert_simple_dimension('Refers_to.Protein',$dataset,'protein'),

    #--------------------
    # Expr_pattern
    &insert_dimension('Expr_pattern','Refers_to.Expr_pattern',
                      $dataset,'expr_pattern'),

    #--------------------
    # Expr_profile
    &insert_simple_dimension('Refers_to.Expr_profile',$dataset,'expr_profile'),

    #--------------------
    # Cell
    &insert_simple_dimension('Refers_to.Cell',$dataset,'cell'),

    #--------------------
    # Cell_group
    &insert_simple_dimension('Refers_to.Cell_group',$dataset,'cell_group'),

    #--------------------
    # Life_stage
    &insert_simple_dimension('Refers_to.Life_stage',$dataset,'life_stage'),

    #--------------------
    # RNAi
    &insert_dimension('RNAi','Refers_to.RNAi', $dataset,'rnai'),

    #--------------------
    # Transgene
    &insert_dimension('Transgene','Refers_to.Transgene', 
                      $dataset,'transgene'),

    #--------------------
    # GO_term
    &insert_dimension('GO_term','Refers_to.GO_term', $dataset,'go_term'),

    #--------------------
    # Operon
    &insert_dimension('Operon','Refers_to.Operon', $dataset,'operon'),

    #--------------------
    # Cluster
    &insert_simple_dimension('Refers_to.Cluster',$dataset,'cluster'),

    #--------------------
    # Feature
    &insert_simple_dimension('Refers_to.Feature',$dataset,'feature'),

    #--------------------
    # Gene_regulation
    &insert_simple_dimension('Refers_to.Gene_regulation',
                             $dataset,'gene_regulation'),

    #--------------------
    # Microarray_experiment
    &insert_simple_dimension('Refers_to.Microarray_experiment',
                             $dataset,'microarray_experiment'),

    #--------------------
    # Anatomy_term
    &insert_simple_dimension('Refers_to.Anatomy_term',
                             $dataset,'anatomy_term'),

    #--------------------
    # Antibody
    &insert_dimension('Antibody','Refers_to.Antibody', $dataset,'antibody'),
    
    #--------------------
    # SAGE_experiment
    &insert_simple_dimension('Refers_to.SAGE_experiment',
                             $dataset,'sage_experiment'),

    #--------------------
    # Y2H
    &insert_simple_dimension('Refers_to.Y2H',
                             $dataset,'y2h'),

    #--------------------
    # Interaction
    &insert_simple_dimension('Refers_to.Interaction',
                             $dataset,'interaction'),

    #==================================================
    # Keyword
    #&insert_simple_dimension('Keyword',
    #                         $dataset,'keyword'),
    "TBL_${dataset}__keyword__dm" => {
      OBJECTS      => sub{[ &names_at($_[0],"Keyword") ]},
      "VAL_keyword" => sub{ ref($_[0]) || return;
                            $_[0] =~ s/,\s*/ /g; return $_[0]; }
    },
    "VAL_keyword_dmlist" => sub{ my $val = &dmlist_at($_[0],"Keyword");
                                 $val =~ s/,\s*/ /g; return $val; },

    #==================================================
    # DONE
    #==================================================
  }, 
};


#---
1;
