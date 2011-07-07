package SequenceLoader;

use strict;
use warnings;
use Data::Dumper qw( Dumper );
use Date::Calc;
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
$CONNECTION->{MART_DBNAME} = ''; # No default


my $dataset    = 'wormmart_sequence';

my $focus_object = 'Sequence';
my $ace_query    = '*'; # All 
# Examples?
#$ace_query    = 'ZK1098*'; # Clone and it's descendents
#$ace_query    = '*.Contig*'; # Briggsae contigs
#$ace_query    = 'ZK*'; # A few example elegans clones + descendents


$CONFIG = {
  "TBL_${dataset}__${dataset}__main" => {
    
    OBJECTS => sub{
      my $ace_handle = shift;
      $ace_handle->fetch_many($focus_object=>$ace_query);
    },

    
    VAL_sequence => sub{ 
      &clear_large_caches; # Wipe out large WormMartTools caches 
      &name($_[0]); 
    },

    #==================================================
    # DNA
    VAL_dna => sub{ &names_at($_[0],'DNA') },
    VAL_dna_length => sub{
      my $t = &at($_[0],'DNA');
      return $t ? $t->right : undef;
    },

    #==================================================
    # SMap
    VAL_smap => &has_val_sub('SMap'),
    
    #----------
    # Top-level coordinates
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

    #----------
    # S_parent
    VAL_parent_canonical => sub{ &names_at($_[0], $dataset, 
                                           'SMap.S_parent.Canonical_parent') },
    VAL_parent_genomic   => sub{ &names_at($_[0], $dataset,
                                           'SMap.S_parent.Genomic_parent') },
    VAL_parent_agp       => sub{ &names_at($_[0], $dataset,
                                           'SMap.S_parent.AGP_parent') },
    
    #----------
    # S_child
    # TODO: #SMap_info?
    VAL_smap_child => &has_val_sub('SMap.S_child'),
    
    &insert_coord_dimension('SMap.S_child.Gene_child',$dataset,
                             'child_gene'),
    &insert_coord_dimension('SMap.S_child.CDS_child',$dataset,
                             'child_cds'),
    &insert_coord_dimension('SMap.S_child.Transcript',$dataset,
                             'child_transcript'),
    &insert_coord_dimension('SMap.S_child.Pseudogene',$dataset,
                             'child_pseudogene'),
    &insert_coord_dimension('SMap.S_child.Transposon',$dataset,
                             'child_transposon'),
    &insert_coord_dimension('SMap.S_child.Genomic_non_canonical',$dataset,
                             'child_genomic_non_canonical'),
    &insert_coord_dimension('SMap.S_child.Nongenomic',$dataset,
                             'child_nongenomic'),
    &insert_coord_dimension('SMap.S_child.PCR_product',$dataset,
                             'child_pcr_product'),
    &insert_coord_dimension('SMap.S_child.Operon',$dataset,
                             'child_operon'),
    &insert_coord_dimension('SMap.S_child.AGP_fragment',$dataset,
                             'child_agp_fragment'),
    &insert_coord_dimension('SMap.S_child.Allele',$dataset,
                             'child_variation'),
    &insert_coord_dimension('SMap.S_child.Oligo_set',$dataset,
                             'child_oligo_set'),
    &insert_coord_dimension('SMap.S_child.Feature_object',$dataset,
                             'child_feature'),
    &insert_coord_dimension('SMap.S_child.Feature_data',$dataset,
                             'child_feature_data'),
    &insert_coord_dimension('SMap.S_child.Homol_data',$dataset,
                             'child_homol_data'),
    &insert_coord_dimension('SMap.S_child.SAGE_tag',$dataset,
                             'child_sage_tag'),
    &insert_coord_dimension('SMap.S_child.SAGE_transcript',$dataset,
                             'child_sage_transcript'),


    #==================================================
    # Structure

    #----------
    # Structure.From
    VAL_source => sub{ &at($_[0],'Structure.From.Source') },
    # TODO - Source_exons?

    #----------
    #  Structure.Subsequence
    &insert_simple_dimension('Structure.Subsequence',$dataset,'subsequence'),

    #----------
    #  Structure.Overlap_right/left
    VAL_overlap_right => sub{ &at($_[0],'Structure.Overlap_right') },
    VAL_overlap_right_val => sub{
      my $s = &at($_[0],'Structure.Overlap_right') || return;
      return &at($_[0],"Structure.Overlap_right.$s");
    },
    COL_overlap_right_val => qq(int(10) unsigned default NULL),

    VAL_overlap_left  => sub{ &at($_[0],'Structure.Overlap_left') },
    VAL_overlap_left_val => sub{
      my $s = &at($_[0],'Structure.Overlap_left') || return;
      return &at($_[0],"Structure.Overlap_left.$s");
    },
    COL_overlap_left_val  => qq(int(10) unsigned default NULL),

    #----------
    # Structure.Gap_right 
    VAL_gap_right_val => sub{
      my $t = $_[0]->at('Structure.Gap_right') || return;
      return $t->right(1);
    },
    VAL_gap_right_text => sub{
      my $t = $_[0]->at('Structure.Gap_right') || return;
      return $t->right(2);
    },

    #----------
    # Structure.Clone_right/left
    &insert_simple_dimension('Structure.Clone_left_end',
                             $dataset,'clone_left'),
    &insert_simple_dimension('Structure.Clone_right_end',
                             $dataset,'clone_right'),
    

    #----------
    # Structure.Flipped
    VAL_flipped => sub{ $_[0]->at('Structure.Flipped') ? 1 : 0 },
    
    #==================================================
    # DB_info
    
    #----------
    # DB_info.Database
    &insert_dimension( 'DB_info.Database','DB_info.Database',
                       $dataset, 'db_info_database' ),

    #----------
    # DB_info.Protein_id
    "TBL_${dataset}__protein_id__dm" => {
      OBJECTS => sub{
        my $seq = $_[0];
        my @hashrefs = ();
        foreach my $pseq( $seq->at("DB_info.Protein_id") ){
          push( @hashrefs, { 
            sequence => "$pseq",
            text     => $pseq->right(1) || undef,
            id       => $pseq->right(2) || undef,
          } );
        }
        return[ @hashrefs ];
      },
      VAL_sequence => &cached_val_sub('sequence'),
      VAL_text     => &cached_val_sub('text'),
      VAL_id       => &cached_val_sub('id'),
    },
    # TODO: dmlist, dminfo?

    #----------
    # DB_info.Secondary_accession
    &insert_simple_dimension('DB_info.Secondary_accession',
                             $dataset,'secondary_accession'),
    
    #----------
    # DB_info.DB_remark
    VAL_db_remark     => sub{&dmlist_at($_[0],'DB_info.DB_remark')},

    #----------
    # DB_info.keyword
    &insert_simple_dimension( 'DB_info.keyword',
                              $dataset,'keyword' ),

    #----------
    # DB_info.DB_annotation
    "TBL_${dataset}__db_annotation__dm" => {
      OBJECTS => sub{
        my $seq = $_[0];
        my @hashrefs = ();
        foreach my $db( $seq->at("DB_info.DB_annotation") ){
          push( @hashrefs, {
            db => "$db",
            text => ( $db->right || undef ),
          } );
        }
        return[ @hashrefs ];
      },
      VAL_db   => &cached_val_sub('db'),
      VAL_text => &cached_val_sub('text'),
    },
    VAL_db_annotation_dminfo => sub{
      my @vals;
      foreach my $db( $_[0]->at("DB_info.DB_annotation") ){
        push @vals, join( " ", "[$db]", $db->right );
      }
      return join( " | ", @vals );
    },
    
    #==================================================
    # Origin
    
    #----------
    # Origin.From_database
    VAL_origin_database => sub{ &names_at( $_[0],'Origin.From_database') },
    VAL_origin_database_val => sub{
      my $t = $_[0]->at('Origin.From_database') || return;
      return $t->right;
    },

    #----------
    # Origin.From_author
    &insert_simple_dimension('Origin.From_author',
                             $dataset,'origin_author'),

    #----------
    # Origin.From_laboratory
    VAL_origin_laboratory => sub{ &names_at( $_[0],'Origin.From_laboratory') },
    VAL_origin_laboratory_info => sub{
      my $l = &cached_at( $_[0],'Origin.From_laboratory') || return;
      return join( " ", "[$l]", &names_at($l,'Address.Mail') );
    },

    #----------
    # Origin.Genetic_code
    VAL_origin_genetic_code => sub{ &names_at( $_[0],'Origin.Genetic_code') },

    #----------
    # Origin.Date
    VAL_origin_date_type => sub{ &names_at( $_[0],'Origin.Date' ) },
    VAL_origin_date_text => sub{
      my $t = $_[0]->at('Origin.Date') || return;
      return $t->right;
    },

    #----------
    # Origin.Date_directory
    VAL_origin_date_directory => sub{&names_at($_[0],'Origin.Date_directory')},

    #----------
    # Origin.Life_stage
    VAL_origin_life_stage => sub{&names_at($_[0],'Origin.Life_stage')},
    
    #----------
    # Origin.Species
    VAL_origin_species => sub{&names_at($_[0],'Origin.Species')},
    
    #----------
    # Origin.Library
    VAL_origin_library => sub{&names_at($_[0],'Origin.Library')},


    #==================================================
    # Visible
    
    #----------
    # Visible.Title
    VAL_visible_title => sub{&names_at($_[0],'Visible.Title')},
    
    #----------
    # Visible.Matching_CDS/transcript/pseudogene
    &insert_simple_dimension('Visible.Matching_CDS',
                             $dataset,'visible_cds'),
    &insert_simple_dimension('Visible.Matching_transcript',
                             $dataset,'visible_transcript'),
    &insert_simple_dimension('Visible.Matching_pseudogene',
                             $dataset,'visible_pseudogene'),

    #----------
    # Visible.Clone/Paired_read
    &insert_simple_dimension('Visible.Clone',
                             $dataset,'visible_clone'),
    &insert_simple_dimension('Visible.Paired_read',
                             $dataset,'visible_paired_read'),

    #----------
    # Visible.GO_term/Gene/Reference/Expr_pattern/RNAi
    &insert_dimension('GO_term','Visible.GO_term',
                      $dataset,'visible_go_term'),
    &insert_dimension('Gene','Visible.Gene',
                      $dataset,'visible_gene'),
    &insert_dimension('Paper','Visible.Reference',
                      $dataset,'visible_paper'),
    &insert_dimension('Expr_pattern','Expr_pattern',
                      $dataset,'visible_expr_pattern'),
    &insert_dimension('RNAi','Visible.RNAi',
                      $dataset,'visible_rnai'),

    # Visible.Remark/Confidential_remark
    VAL_visible_remark => sub{ &dmlist_at($_[0],'Visible.Remark') },
    VAL_visible_remark_confidential => sub{ 
      &dmlist_at($_[0], 'Visible.Confidential_remark');
    },


    #==================================================
    # Properties

    #----------
    # Properties.Genomic_canonical
    VAL_prop_genomic_canonical => sub{
      $_[0]->at('Properties.Genomic_canonical') ? 1 : undef;
    },
    VAL_prop_genomic_canonical_gene_count => sub{
      my $t = $_[0]->at('Properties.Genomic_canonical');
      return $t ? $t->right : undef;
    },

    #----------
    # Properties.Briggsae_canonical/Genomic
    VAL_prop_briggsae_canonical => sub{
      $_[0]->at('Properties.Briggsae_canonical') ? 1 : undef;
    },
    VAL_prop_genomic => sub{
      $_[0]->at('Properties.Genomic') ? 1 : undef;
    },

    #----------
    # Properties.cDNA
    VAL_prop_cdna => sub{
      $_[0]->at('Properties.cDNA') ? 1 : undef;
    },
    VAL_prop_cdna_est => sub{
      $_[0]->at('Properties.cDNA.cDNA_EST') ? 1 : undef;
    },
    VAL_prop_cdna_est_5 => sub{
      $_[0]->at('Properties.cDNA.EST_5') ? 1 : undef;
    },
    VAL_prop_cdna_est_3 => sub{
      $_[0]->at('Properties.cDNA.EST_3') ? 1 : undef;
    },
    VAL_prop_cdna_capped_5 => sub{
      $_[0]->at('Properties.cDNA.Capped_5') ? 1 : undef;
    },
    VAL_prop_cdna_tsl_tag => sub{
      $_[0]->at('Properties.cDNA.TSL_tag') ? 1 : undef;
    },

    #----------
    # Properties.EST_consensus
    VAL_prop_est_consensus => sub{
      $_[0]->at('Properties.EST_consensus') ? 1 : undef; 
    },
    

    #----------
    # Properties.RNA
    VAL_prop_rna => sub{
      my $t = $_[0]->at('Properties.RNA');
      return $t ? $t->right : undef;
    },
    VAL_prop_mrna => sub{
      my $t = $_[0]->at('Properties.RNA.mRNA');
      return $t ? $t->right : undef;
    },
    VAL_prop_trna_type => sub{
      &names_at($_[0],
                 'Properties.RNA.tRNA.Type');
    },
    VAL_prop_trna_anticodon => sub{
      &names_at($_[0],
                 'Properties.RNA.tRNA.Anticodon');
    },
    VAL_prop_rna_text => sub{
      my $t = $_[0]->at('Properties.RNA') || return;
      if( $t->right eq 'tRNA' ){
        return $t->right(3);
      }
      return $t->right(2);
    },

    #----------
    # Properties.Ignore - skip Evidence
    VAL_prop_ignore => sub{
      $_[0]->at('Properties.Ignore') ? 1 : undef; 
    },

    #----------
    # Properties.Show_in_reverse_orientation
    VAL_prop_show_reversed => sub{
      $_[0]->at('Properties.Show_in_reverse_orientation') ? 1 : undef;
    },

    #----------
    # Properties.Status
    "TBL_${dataset}__status__dm" => {
      OBJECTS => sub{
        my @refs;
        foreach my $t( &at( $_[0], 'Properties.Status' ) ){
          my $date_str = $t->right || '';
          $date_str =~ s/\d\d\:\d\d\:\d\d//;
          my ($y,$m,$d) = Date::Calc::Decode_Date_EU($date_str);
          $y||=0; $m||=0; $d||=0;
          $date_str = sprintf( "%4.4d-%2.2d-%2.2d", $y,$m,$d );
          push @refs, {
            status => $t,
            date   => $date_str,
          };
        }
        return [@refs];
      },
      VAL_status => &cached_val_sub('status'),
      VAL_date   => &cached_val_sub('date'),
      COL_date => qq|date default NULL|,
    },
    VAL_status_dmlist => sub{
      my @vals;
      foreach my $t( &at( $_[0], 'Properties.Status' ) ){
        my $date_str = $t->right || '';
        $date_str =~ s/\d\d\:\d\d\:\d\d//;
        my ($y,$m,$d) = Date::Calc::Decode_Date_EU($date_str);
        $y||=0; $m||=0; $d||=0;
        $date_str = sprintf( "%4.4d-%2.2d-%2.2d", $y,$m,$d );
        push @vals, [$t,$date_str];
      }
      join( " | ", map{"[$_->[0]] $_->[1]"} sort{$a->[1] cmp $b->[1]} @vals );
    },
    VAL_status_current => sub{
      my @vals;
      foreach my $t( &at( $_[0], 'Properties.Status' ) ){
        my $date_str = $t->right || '';
        $date_str =~ s/\d\d\:\d\d\:\d\d//;
        my ($y,$m,$d) = Date::Calc::Decode_Date_EU($date_str);
        $y||=0; $m||=0; $d||=0;
        $date_str = sprintf( "%4.4d-%2.2d-%2.2d", $y,$m,$d );
        push @vals, [$t,$date_str];
      }
      my($latest) = map{"$_->[0]"} sort{$b->[1] cmp $a->[1]} @vals;
      return $latest;
    },
    
    #----------
    # Properties.Match_type
    VAL_prop_match_type => sub{ 
      &names_at($_[0],'Properties.Match_type');
    },
    VAL_prop_match_with_function => 
        &has_val_sub('Properties.Match_type.Match_with_function'),
    VAL_prop_match_without_function => 
        &has_val_sub('Properties.Match_type.Match_without_function'),
        
    #----------
    # Properties.Link
    VAL_prop_link => &has_val_sub('Properties.Link'),

    #==================================================
    # Splices
    # TODO: Implement

    #==================================================
    # Cluster_information
    
    #----------
    # Cluster_information.Contains_reads/Contained_in_cluster
    &insert_simple_dimension('Cluster_information.Contains_reads',
                             $dataset,'cluster_contains_reads'),
    &insert_simple_dimension('Cluster_information.Contained_in_cluster',
                             $dataset,'cluster_contained_in'),

    #==================================================
    # Map
    &insert_simple_dimension('Map',$dataset,'map'),
    
    #==================================================
    # Interpolated_map_position
    VAL_map_interpolated => sub{&names_at($_[0],'Interpolated_map_position')},
    VAL_map_interpolated_position => sub{
      my $t = $_[0]->at('Interpolated_map_position');
      return $t ? $t->right(2) : undef;
    },

    #==================================================
    # Oligo
    &insert_simple_dimension('Oligo',$dataset,'oligo'),

    #==================================================
    # Defines_feature
    &insert_simple_dimension('Defines_feature',$dataset,'defines_feature'),

    #==================================================
    # Assembly_tags
    # SKIP. TODO: implement

    #==================================================
    # Gene_regulation
    &insert_simple_dimension('Gene_regulation.Cis_regulator',
                             $dataset,'gene_regulation_cis_regulator'),

    #==================================================
    # Y2H_bait/target
    &insert_simple_dimension('Y2H_bait',$dataset,
                             'y2h_bait'),
    &insert_simple_dimension('Y2H_target',$dataset,
                             'y2h_target'),

    #==================================================
    # Homol. TODO: Implement alignment data
    &insert_simple_dimension('Homol.DNA_homol',  $dataset,'homol_dna'),
    &insert_simple_dimension('Homol.Pep_homol',  $dataset,'homol_pep'),
    &insert_simple_dimension('Homol.Motif_homol',$dataset,'homol_motif'),
    &insert_simple_dimension('Homol.Homol_homol',$dataset,'homol_homol'),


    #==================================================
    # Method
    VAL_method => sub{ &names_at($_[0],'Method') },

    #==================================================
    # DONE
    #==================================================
    IDX => ['sequence', 'method', 'origin_species'],
  }, 
};



1;
