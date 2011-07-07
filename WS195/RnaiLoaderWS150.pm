package RnaiLoader;

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


my $dataset    = 'rnai';

my $focus_object = 'RNAi';
my $ace_query    = '*'; # All 
#my $ace_query = 'WBRNAi0000000?'; # First 9
#my $ace_query = 'WBRNAi000000??'; # First 99
#my $ace_query = 'WBRNAi00000???'; # First 999

$CONFIG = {
  "TBL_${dataset}__rnai__main" => {
    
    OBJECTS => sub{
      my $ace_handle = shift;
      $ace_handle->fetch($focus_object=>$ace_query);
    },

    VAL_rnai => sub{ &name($_[0]) },

    #==================================================
    # Evidence
    # Skip
    
    #================================================== 
    # History_name
    VAL_history_name => sub{ &names_at($_[0],'History_name') },
    
    #==================================================
    # Homol
    "TBL_${dataset}__physical_position__dm" => {
      OBJECTS => sub{ &cached_at( $_[0],'Homol.Homol_homol' ) },
      VAL_sequence => sub{
        # Strip any leading 'CHROMOSOME_' prefix from name
        my $homol_data = shift;
        my $name = (&physical_position($homol_data))[0] || return;
        $name =~ s/^CHROMOSOME_//;
        return $name },
      COL_start  => qq| int(10) unsigned default NULL |, # Set explicitly
      COL_end    => qq| int(10) unsigned default NULL |, # Set explicitly
      COL_strand => qq| tinyint(2) default NULL |,       # Set explicitly

      VAL_start  => sub{ (&physical_position($_[0]))[1] },
      VAL_end    => sub{ (&physical_position($_[0]))[2] },
      VAL_strand => sub{ (&physical_position($_[0]))[3] },
    },

    "TBL_${dataset}__homol__dm" => {
      OBJECTS => sub{ 
        my @homols = &cached_at( $_[0],'Homol.Homol_homol' );
        my %rnais = ( map{$_->{name} => $_} 
                      map{&cached_data_at($_,'Homol.RNAi_homol')} @homols );
        return values %rnais;
      },
      VAL_rnai              => sub{$_[0] ? $_[0]->{name} : undef },
      VAL_rnai_name         => sub{$_[0] ? $_[0]->{history_name} : undef },
      #VAL_info              => sub{$_[0] ? $_[0]->{info} : undef },
      VAL_experiment_date   => sub{$_[0] ? $_[0]->{experiment_date} : undef },
      VAL_experiment_strain => sub{$_[0] ? $_[0]->{experiment_strain} : undef},
      VAL_experiment_author => sub{ 
        $_[0] || return;
        join($LIST_SEPARATOR,@{$_[0]->{experiment_author}||[]}) },
      VAL_experiment_laboratory => sub{ 
        $_[0] || return;
        join($LIST_SEPARATOR,@{$_[0]->{experiment_laboratory}||[]}) },
      VAL_phenotype => sub{
        $_[0] || return;
        join($LIST_SEPARATOR,@{$_[0]->{phenotype}||[]}) },
    },

    VAL_homol_dmlist => sub{
      my @homols = &cached_at( $_[0],'Homol.Homol_homol' );
      my %rnais = ( map{$_->{name} => $_} 
                    map{&cached_data_at($_,'Homol.RNAi_homol')} @homols );
      return join( " | ", keys %rnais );
    },
    VAL_homol_name_dmlist => sub{
      my @homols = &cached_at( $_[0],'Homol.Homol_homol' );
      my %rnais = ( map{$_->{history_name} ? ($_->{history_name}=>$_) : () }
                    map{&cached_data_at($_,'Homol.RNAi_homol')} @homols );
      return join( " | ", keys %rnais );
    },
    VAL_homol_dminfo => sub{
      my @homols = &cached_at( $_[0],'Homol.Homol_homol' );
      my %rnais = ( map{$_->{info} => $_}
                    map{&cached_data_at($_,'Homol.RNAi_homol')} @homols );
      return join( " | ", keys %rnais );
    },

    #==================================================
    # Sequence_info

    #--------------------
    # DNA_text
    # Skip 
    # TODO: get from genomic location e.g. like genes?
    #VAL_dna => sub{ &names_at($_[0],'Sequence_info.DNA_text') },
    
    #--------------------
    # Sequence
    &insert_simple_dimension('Sequence_info.Sequence',$dataset,'sequence'),
    
    #--------------------
    # PCR_product
    &insert_simple_dimension('Sequence_info.PCR_product',$dataset,
                             'pcr_product'),
    
    #==================================================
    # Uniquely_mapped
    # Skip
    #VAL_uniquely_mapped => sub{ &at($_[0],'Uniquely_mapped') ? 1 : 0 },
    
    #==================================================
    # Experiment
    &insert_dimension('Experiment','Experiment',$dataset ),
    

    #==================================================
    # Inhibits
    
    #--------------------
    # Gene
    &insert_dimension('Gene','Inhibits.Gene',$dataset,'inhibits_gene'),
          
    #--------------------
    # Predicted_gene
    #&insert_dimension('CDS','Inhibits.Predicted_gene',$dataset,'inhibits_cds');

    #--------------------
    # Transcript
    #&insert_dimension('Transcript','Inhibits.transcript',
    #                  $dataset,'inhibits_transcript');
  
    #--------------------
    # Pseudogene
    #&insert_dimension('Transcript','Inhibits.pseudogene',
    #                  $dataset,'inhibits_pseudogene');

    #==================================================
    # Supporting_data

    #--------------------
    # Movie
    &insert_simple_dimension('Supporting_data.Movie',$dataset,'movie'),

    #--------------------
    # Picture
    &insert_simple_dimension('Supporting_data.Picture',$dataset,'picture'),

    #==================================================
    # Species
    VAL_species => sub{ &names_at( $_[0], "Species" ) },


    #==================================================
    # Reference
    &insert_dimension('Paper','Reference',$dataset),
 
       
    #==================================================
    # Phenotype
    &insert_dimension('Phenotype','Phenotype',$dataset),

    #==================================================
    # Expr_profile
    &insert_simple_dimension('Expr_profile',$dataset),

    #==================================================
    # Remark
    &insert_simple_dimension('Remark',$dataset),

    #==================================================
    # Method
    VAL_method => sub{ &names_at($_[0],'Method') },

    #==================================================
    # DONE
    #==================================================
  }, 
};


#---
1;
