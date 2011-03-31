package VariationLoader;

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

my $acedb_class = 'variation';
my $dataset     = 'wormbase_' . lc( $acedb_class );
my $ace_query    = '*'; # All 
#$ace_query = 'RW#L17';  # Very light Variation obj
#$ace_query = 'a83';     # Allele with Phenotype
#$ace_query = 'ay100';   # Coding exon SNP
#$ace_query = 'ok1100';  # 1400bp deletion
#$ace_query = 'cxTi9100'; # MOS insertion
#$ace_query = 'vs100'; # Unsequenced deletion
#$ace_query = 'cxTi7135'; # Affects Pseudogene
#$ace_query = 'jf61';
#$ace_query = '*100';     # About 100 assorted variations 
#$ace_query = 'hw106593';

$CONFIG = {
  "TBL_${dataset}__${acedb_class}__main" => {

    OBJECTS => sub{
      my $ace_handle = shift;
      $ace_handle->fetch_many($acedb_class=>$ace_query);
    },

    VAL_variation => sub{ $_[0]->name },

    #==================================================
    # Evidence
    # Skip

    #==================================================
    # Name
    
    #--------------------
    # Name.CGC_name
    VAL_cgc_name => sub{ &names_at($_[0],'Name.CGC_name') },

    #--------------------
    # Name.Exelixis_ID
    VAL_exelixis_id => sub{ &names_at($_[0],'Name.Exelixis_ID') },

    #--------------------
    # Name.WashU_ID
    VAL_washu_id => sub{ &names_at($_[0],'Name.WashU_ID') },

    #--------------------
    # Name.Other_name
    &insert_simple_dimension('Name.Other_name',$dataset,'other_name'),

    #--------------------
    # Name.<any_name>
    VAL_name_dmlist => sub{
      my %seen = ();
      return join( " | ", 
                   grep{ ! $seen{$_} ++ }
                   ( "$_[0]",
                     &names_at( $_[0],
                                'Name.CGC_name',
                                'Name.Exelixis_ID',
                                'Name.WashU_ID',
                                'Name.Other_name',
                                'Name.Rearrangement',
                                'Name.Public_name' ) ),
                   );
    },
    "TBL_${dataset}__name__dm" => {
      OBJECTS         => sub{
        my %seen = ();
        return[ grep{ ! $seen{$_} ++ }
                "$_[0]",
                &names_at( $_[0],
                           'Name.CGC_name',
                           'Name.Exelixis_ID',
                           'Name.WashU_ID',
                           'Name.Other_name',
                           'Name.Rearrangement',
                           'Name.Public_name') ];
      },
      VAL_name  => sub{ ref($_[0]) ? undef : $_[0] },
      IDX => ['name'],
    },

    #--------------------
    # Name.Rearrangement
    &insert_simple_dimension('Name.Rearrangement',$dataset,'rearrangement'),

    #--------------------
    # Name.Public_name
    VAL_public_name => sub{ &names_at($_[0],'Name.Public_name') },


    #==================================================
    # Sequence_details

    #--------------------
    # Sequence_details.SMap
    COL_smap_start => qq(int(10) unsigned default NULL),
    COL_smap_end   => qq(int(10) unsigned default NULL),
    COL_smap_strand=> qq(tinyint(2) default NULL),
    VAL_smap_sequence => sub{
      # Strip any leading 'CHROMOSOME_' prefix from name
      my $obj  = shift;
      my $name = (&physical_position($obj))[0] || return;
      $name =~ s/^CHROMOSOME_//;
      return $name },
    VAL_smap_start     => sub{ (&physical_position($_[0]))[1] },
    VAL_smap_end       => sub{ (&physical_position($_[0]))[2] },
    VAL_smap_strand    => sub{ (&physical_position($_[0]))[3] },

    #--------------------
    # Sequence_details.Flanking_sequences
    VAL_flanking_sequence => sub{
      if( my $flank = $_[0]->at('Sequence_details.Flanking_sequences') ){
        return join( 'W', lc($flank->right), lc($flank->right(2)) );
      }
    },

    #--------------------
    # Sequence_details.Type_of_mutation
    VAL_mutation_type => sub{ 
      my @types = &names_at($_[0],'Sequence_details.Type_of_mutation');
      #if( @types > 1 ){ die( "More than one Type_of_mutation" ) }
      $types[0] ||= 'Unknown';      
      return join( '/', @types );
    },
    VAL_allele => sub{
      my $t;
      if( $t = $_[0]->at('Sequence_details.Type_of_mutation.Substitution')){
        return join( "/", uc($t->right||'?'), uc($t->right(2)||'?') );
      }

      elsif( $t = $_[0]->at('Sequence_details.Type_of_mutation.Insertion')){
        #INSERTION
        my $refseq = '-';
        if( my $td = $_[0]->at('Sequence_details.Type_of_mutation.Deletion')){
          #...WITH DELETION
          $refseq = uc($td->right);
          unless( $refseq ){ # get length of deletion based on location
            my( $start, $end ) = (&physical_position($_[0]))[1..2];
            my $len = ( $end - $start + 1 ) if ( $start && $end );
            $refseq = "[${len} BP DELETION]" if $len; 
          }
          $refseq ||= '?';
        }
        my $ins = $t->right;
        unless( $ins ){ # Examine for transposin insertion
          if( scalar( grep{ $_ =~ /transposon/i} 
                      &names_at( $_[0], 'Variation_type' ) ) ){
            $ins = '[TRANSPOSON INSERTION]';
          }
        }
        return join( "/", uc($refseq), uc($ins)||'?');
      }

      elsif( $t = $_[0]->at('Sequence_details.Type_of_mutation.Deletion')){
        #DELETION
        my $refseq = $t->right;
        unless( $refseq ){ # get length of deletion based on location         
          my( $start, $end ) = (&physical_position($_[0]))[1..2];
          my $len = ( $end - $start + 1 ) if ( $start && $end );
          $refseq = "[${len} BP DELETION]" if $len;
        }
        return join( "/", uc($refseq||'?'), '-' );
      }
    },

    VAL_allele_length => sub{
      my @lengths = ();
      my $t;
      if( $t = $_[0]->at('Sequence_details.Type_of_mutation.Substitution')){
        push @lengths, length( $t->right   ||return ), 
                       length( $t->right(2)||return );
      }
      if( $t = $_[0]->at('Sequence_details.Type_of_mutation.Insertion') ){
        my $len = length($t->right||'');
        unless( $len ){ # Is this a transposon_insertion?
          if( scalar( grep{ $_ =~ /transposon/i} 
                      &names_at( $_[0], 'Variation_type' ) ) ){
            $len = 1300;
          }
        }
        push @lengths, $len || return;
      }
      if( $t = $_[0]->at('Sequence_details.Type_of_mutation.Deletion') ){
        my $len = length($t->right||'');
        unless( $len ){ # Infer the length of an insertion based on location
          my( $start, $end ) = (&physical_position($_[0]))[1..2];
          $len = ( $end - $start + 1 ) if ( $start ); 
        }
        push @lengths, $len || return;
      }
      my($max) = sort{ $b<=>$a } @lengths;
      return $max || undef;
    },
    COL_allele_length => qq(int(10) unsigned default NULL),

    #--------------------
    # Sequence_details.PCR_product
    &insert_simple_dimension('Sequence_details.PCR_product',
                             $dataset,'pcr_product'),
    

    #--------------------
    # Sequence_details.SeqStatus
    &insert_val(undef(),'Sequence_details.SeqStatus','seqstatus'),
    #VAL_seqstatus => sub{ &names_at($_[0],'Sequence_details.SeqStatus') }

    #==================================================
    # Variation_type
    &insert_simple_dimension('Variation_type', $dataset, 'variation_type'),

    #==================================================
    # Origin

    #--------------------
    # Origin.Species Defaults to 'unknown'
    VAL_status => sub{
      my $val = &names_at( $_[0], 'Origin.Species' );
      return $val || 'unknown';
    },
    &insert_val('?Species','Origin.Species','species'),

    #--------------------
    # Origin.Strain
    &insert_dimension('Strain','Origin.Strain',$dataset,'strain'),

    #--------------------
    # Origin.Laboratory
    &insert_dimension('Laboratory','Origin.Laboratory',$dataset,'laboratory'),

    #--------------------
    # Origin.Author
    &insert_simple_dimension('Origin.Author',$dataset,'author'),

    #--------------------
    # Origin.Person
    # Skip. TODO: fold into Author dimension?

    #--------------------
    # Origin.KO_consortium_allele
    VAL_ko_consortium => sub{ 
      $_[0]->at('Origin.KO_consortium_allele') ? 1 : undef;
    },

    #--------------------
    # Origin.NBP_allele
    VAL_nbp_allele => sub{
      $_[0]->at('Origin.NBP_allele') ? 1 : undef;
    },

    #--------------------
    # Origin.NemaGENETAG_consortium_allele

    #--------------------
    # Origin.Detection_method
    VAL_detection_method => sub{ &dmlist_at($_[0],'Origin.Detection_method') },

    #--------------------
    # Origin.Positive_clone
    &insert_simple_dimension('Origin.Positive_clone',$dataset,
                             'positive_clone'),

    #--------------------
    # Origin.Nature_of_variation
    VAL_nature_of_variation => sub{
      if( $_[0]->at('Origin.Nature_of_variation') ){
        warn( "Have nature_of_variation: $_[0]" );
        die;
      }
    },
      #&insert_val('Origin.Nature_of_variation', 'nature_of_variation'),

    #--------------------
    # Origin.Status. Defaults to 'Live' if unset
    VAL_status => sub{
      my $status = &names_at( $_[0], 'Origin.Status' );
      return $status || 'Live';
    },



    #==================================================
    # Linked_to
    &insert_simple_dimension('Linked_to',$dataset,'linked_to'),

    
    #==================================================
    # Affects

    #--------------------
    # Affects.Gene
    #&insert_dimension('Gene','Affects.Gene',$dataset,'gene'),
    &_insert_dimension_with_mol_change('Gene',$dataset),

    #--------------------
    # Affects.Predicted_CDS
    #&insert_simple_dimension('Affects.Predicted_CDS',$dataset,'cds'),
    &_insert_dimension_with_mol_change('CDS',$dataset),

    #--------------------
    # Affects.Transcript
    #&insert_simple_dimension('Affects.Transcript',$dataset,
    #                         'transcript'),
    &_insert_dimension_with_mol_change('Transcript',$dataset),
    
    #--------------------
    # Affects.Pseudogene
    #&insert_simple_dimension('Affects.Pseudogene',$dataset,
    #                         'pseudogene'),
    &_insert_dimension_with_mol_change('Pseudogene',$dataset),
    


    #==================================================
    # Isolation
    &insert_val('DateType','Isolation.Date','date'),
    &insert_val('Text', 'Isolation.Mutagen','mutagen'),
    VAL_forward_genetics => sub{ &dmlist_at($_[0],
                                            'Isolation.Forward_genetics')},
    VAL_reverse_genetics => sub{ &dmlist_at($_[0],
                                            'Isolation.Reverse_genetics')},
    &insert_simple_dimension('Variation_type.Transposon_insertion',$dataset,
                             'transposon_family'),
    &insert_simple_dimension('Isolation.Derived_from',$dataset,
                             'derived_from_variation'),
    &insert_simple_dimension('Isolation.Derivative',$dataset,
                             'derivative_variation'),

    #==================================================
    # Genetics

    #----------
    # Genetics.Gene_class
    &insert_dimension('?Gene_class','Genetics.Gene_class',$dataset),

    #----------
    # Map_info.Map
    # Map_info.Interpolated_map_position
    &insert_dimension('Map_position','Genetics',$dataset),

    #VAL_interpolated_map => sub{
    #  if( my $map = &at($_[0],"Genetics.Interpolated_map_position") ){
    #    return $map;
    #  }
    #  #if( my $map = &at($_[0],"Genetics.Map") ){
    #  #  return $map;
    #  #}
    #},
    #VAL_interpolated_map_position => sub{
    #  my( $map_name, $position );
    #  if( my $map = &at($_[0],"Genetics.Interpolated_map_position") ){
    #    $map_name ||= "$map";
    #    $position = &at($_[0],"Genetics.Interpolated_map_position.$map_name");
    #  }
    #  #if( !$position and 
    #  #    my $map = &at($_[0],"Genetics.Map") ){
    #  #  $map_name = "$map";
    #  #  $position = &at($_[0],"Genetics.Map.$map_name.Position"); 
    #  #}
    #  return $position;
    #},
    # COL_map_interpolated_position => qq| float default NULL |,
    
    #----------
    # Genetics.Mapping_data
    # Skip

    #----------
    # Genetics.Rescued_by_transgene
    &insert_dimension( '?Transgene','Genetics.Rescued_by_transgene',
                       $dataset, 'transgene_rescued'),
    

    #==================================================
    # Description

    #--------------------
    # Description.Phenotype
    # Need to examine Phenotype_info 
    &insert_dimension('Phenotype_info',undef, $dataset,'Phenotype'),

    VAL_phenotype_remark => sub{ &dmlist_at($_[0],
                                            'Description.Phenotype_remark') },

    #--------------------
    # Description.Recessive,Semi_dominant,Dominant
    # now in Phenotype_info hash

    #--------------------
    # Description.Partially_penetrant,Completely_penetrant
    # now in Phenotype_info hash

    #--------------------
    # Description.Temperature_sensitive
    # now in Phenotype_info hash

    #--------------------
    # Description.Loss_of_function
    # now in Phenotype_info hash

    #--------------------
    # Description.Gain_of_function
    # now in Phenotype_info hash

    #--------------------
    # Sense
    # Description.Nonsense
    # Description.Missense
    # Description.Silent
    # now in Molecular_change hash.

    #--------------------
    # Description.Splice_site
    # now in Molecular_change hash.

    #--------------------
    # Description.Frameshift
    # now in Molecular_change hash.


    #==================================================
    # Reference
    &insert_dimension('Paper','Reference',$dataset),


    #==================================================
    # Remark
    VAL_remark => sub{ &dmlist_at($_[0],'Remark') },

    #==================================================
    # Method 
    &insert_val('?Method', 'Method', 'method'),
    
    #==================================================    
    # Sort out confusion/overlap surrounding [Variation] Method and 
    # [Variation] Type.
    #
    #    Add a new Filters section, 'Allele', containing these filters;
    #   
    #    'Allele Type', select one from;
    #    'SNP',
    #    'Insertion (Non-Transposon)',
    #    'Insertion (SNP)
    #    'Insertion (Transposon)',
    #    'Deletion',
    #    'Substitution',
    #    'RFPL'
    #    'Unknown'.
    #   
    #    'Allele Status', select one from;
    #    'Confirmed',
    #    'Predicted (SNP only)',
    #    'Unsequenced',
    #    'Pending Curation',
    #    'Unknown (hope there will be none of these).
    #
    # These options will need to be generated from the existing type, 
    # method and seq_status attributes.
    #VAL_allele_type => sub{ &get_allele_type($_[0]) },
    #VAL_allele_status => sub{},

    
    #==================================================
    # DONE
    #==================================================
    IDX => ['public_name',
            'smap_sequence',
            'smap_start',
            'smap_end',
            'detection_method',
            'mutation_type',
            'seqstatus',
            'species',
            'splice_site',
            'status'],
  }, 
};


#======================================================================

sub get_allele_type{
  my $variation_obj = shift;
  warn( $variation_obj );

  my %var_types = map{ $_ => 1 } $variation_obj->at('Variation_type');
  warn "  VAR TYPES:  ". join( ", ", keys %var_types ) ."\n";

  my ($method) = &names_at( $variation_obj, 'Method' );
  warn "  METHOD:     " . ($method || 'undefined') ."\n";
  
  my %mut_types = map{ $_=>1} 
    &names_at( $variation_obj, 'Sequence_details.Type_of_mutation' );
  warn "  MUT TYPES:  ". join( ", ", keys %mut_types ) ."\n";

  my ($seq_status) = &names_at( $variation_obj, 'Sequence_details.SeqStatus');
  warn "  SEQ STATUS: ". ( $seq_status || 'undefined' ) ."\n";

  my $allele_type = '';
  
  # Determine the allele type
  if( $var_types{SNP} || 
      $var_types{Confirmed_SNP} || 
      $var_types{Predicted_SNP}){
    $allele_type = 'SNP';
  }
  if( $var_types{Transposon_insertion} or $method eq 'Mos_insertion' ){
    $allele_type = 'Transposon';
  }

  if( $mut_types{Substitution} ){
    $allele_type = join('_', 'Substitution', $allele_type || () );
  }
  elsif( $mut_types{Deletion} ){
    $allele_type = join('_', 'Deletion', $allele_type || () );
  }
  elsif( $mut_types{Insertion} ){
    $allele_type = join('_', 'Insertion', $allele_type || () );
  }
  elsif( $mut_types{Inversion} ){
    $allele_type = 'Inversion';
  }



  $allele_type ||= 'Unknown';
  warn "**ALE TYPE:   $allele_type\n";

  return undef;

}

#----------------------------------------------------------------------
# Dimension for gene/CDS/transcript with molecular change fields
sub _insert_dimension_with_mol_change{
  my $class = shift;
  my $dataset = shift;
  my $col     = lc( $class );
  
  my %dimension;
  $class eq 'Gene'       and %dimension 
      = &insert_dimension($class,'Affects.Gene',$dataset,$col);
  $class eq 'CDS'        and %dimension 
      = &insert_dimension($class,'Affects.Predicted_CDS',$dataset,$col);
  $class eq 'Transcript' and %dimension
      = &insert_dimension($class,'Affects.Transcript',$dataset,$col);
  $class eq 'Pseudogene' and %dimension
      = &insert_dimension($class,'Affects.Pseudogene',$dataset,$col);

  $dimension{"TBL_${dataset}__${col}__dm"}->{"OBJECTS"}
      = sub{[ @{ &object_data_cache($_[0])->{$col} || [] } ]};

  my %dm_values;
  foreach my $tag qw( Missense
                      Silent
                      Nonsense 
                      Splice_site
                      Frameshift
                      Intron
                      Coding_exon
                      Noncoding_exon
                      Promoter
                      UTR_3
                      UTR_5
                      Regulatory_feature
                      Genomic_neighbourhood ){
    my $key = 'MolecularChange_'.$tag;
    $dimension{"TBL_${dataset}__${col}__dm"}->{"VAL_$key"}
       = &cached_val_sub($key);
  }

  return %dimension;
}

#---
1;
