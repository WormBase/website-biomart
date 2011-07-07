package GeneLoader;

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
$CONNECTION->{MART_PORT}   = undef();    # Def 3306
$CONNECTION->{MART_USER}   = undef();  # Def euid user
$CONNECTION->{MART_PASS}   = undef(); # Def empty
$CONNECTION->{MART_DBNAME} = undef(); # No default


my $dataset    = 'wormbase_gene';

my $gene_query = '*'; # All 
#$gene_query = 'WBGene0000*';    # 1-9999 for debug
# $gene_query = 'WBGene00000*';   # 1-999 for debug
# $gene_query = 'WBGene000000*';  # 1-99 for debug
# $gene_query = 'WBGene0000000*'; # 1-9  for debug
# $gene_query = 'WBGene00000011'; # First nc gene  
# $gene_query = 'WBGene00000693'; # Single exon gene with 5+3 UTRs
#$gene_query = 'WBGene00000018'; # Transgenes
#$gene_query = 'WBGene00000019'; # Single gene for debug
#$gene_query = 'WBGene00000036'; # Gene with GO_terms with multi evidence
#$gene_query = 'WBGene00022277'; # First coding gene on CHROMOSOME_I
#$gene_query = 'WBGene00009288'; # Last coding gene on CHROMOSOME_I
#$gene_query = 'WBGene00000006';
#$gene_query = 'WBGene00000812'; # + ori, *1 CDS, *3 trans, UTR-only exons 
#$gene_query = 'WBGene00017929'; # - ori, many transc. Has UTR-only exons
#$gene_query = 'WBGene00000829'; # Has CDS with no transcr (mitochondrial)
#$gene_query = 'WBGene00001574'; # Has mixed coding/non-coding transcr
#$gene_query = 'WBGene00000104'; # Genetic location, but no physical loc.
#$gene_query = 'WBGene00013283'; # Pseudogene
# $gene_query = 'WBGene00000307'; # Coding Briggsae gene (+ strand)
#$gene_query = 'WBGene00023527'; # Coding Briggsae gene (- strand)
#$gene_query = 'WBGene00000303'; # 'dodgy' Briggsae gene (+ strand)
#$gene_query = 'WBGene00002245'; # trans-regulator gene
#$gene_query = 'WBGene00015553'; # Selenocysteine
#$gene_query = 'WBGene00017187'; # ?CDS.Method = transposon_CDS
#$gene_query = 'WBGene00045436'; # Remanei
#$gene_query = 'WBGene00119*'; # Pristionchus pacificus, ~200 ids
#$gene_query = 'WBGene0012*'; # Pristionchus pacificus, ~10000 ids
#$gene_query = 'WBGene00119000'; # Pristionchus pacificus
#$gene_query = 'WBGene0010*'; # Pristionchus pacificus
#$gene_query = 'WBGene00004079'; # Pristionchus pacificus test Pp-vul-1
#$gene_query = 'WBGene00114742'; # Pristionchus pacificus test Ppa-aagr-2
#$gene_query = 'WBGene00110476'; # Pristionchus pacificus test Ppa-bath-36
#$gene_query = 'WBGene00102095'; # Pristionchus pacificus test Ppa-cdc-25.1
#$gene_query = 'WBGene00091465'; # Pristionchus pacificus test Ppa-cyp-42A1
#$gene_query = 'WBGene00115724'; # Pristionchus pacificus test Ppa-eef-1A.2
#$gene_query = 'WBGene00182385'; # C_Japonica, for testing sequence, in dna loader
#$gene_query = 'WBGene00103474'; # Ppa, for testing sequence, in dna loader
#$gene_query = 'WBGene00148355'; # Ppa, for testing sequence, in dna loader
#$gene_query = 'WBGene00157406'; # Cbre, for testing Cbre, Cbn-aagr-4
#$gene_query = 'WBGene00156938'; # Cbre, for testing Cbre, Cbn-arf-1.1
#$gene_query = 'WBGene00149557'; # Cbre, for testing Cbre, Cbn-cyp-13A1
#$gene_query = 'WBGene00138871'; # Cbre, for testing CBN00146
#$gene_query = 'WBGene000001*'; # testing homology_group_inParanoid
##$gene_query = 'WBGene00000897'; #testing ncbi_refseq, NM_001027988
##$gene_query = 'WBGene00000001'; #testing ncbi_refseq, NM_059121, sequence_name=Y110A7A.10
##$gene_query = 'WBGene00076480';

$CONFIG = {
  "TBL_${dataset}__gene__main" => {

    OBJECTS => sub{
      my $ace_handle = shift;
      my $iter = $ace_handle->fetch_many( -class => 'Gene',
                                          -name  => $gene_query,
                                          -fill  => 1 );
      return $iter;
    },

    # This subroutine is called foe each gene, and removes the ?Gene, ?CDS and 
    # ?Protein from the ACE and WormMartTools caches
    DESTROY => sub{ 
      my $gene = $_[0];
      my $ace_db = $gene->db;
      foreach my $cds( &cached_at($gene,'Molecular_info.Corresponding_CDS') ){
        my( $prot ) = &cached_at($cds,'Visible.Corresponding_protein' );
        $ace_db->memory_cache_delete($cds);
        &object_cache_delete($cds);
        if( $prot ){
          $ace_db->memory_cache_delete($prot);
          &object_cache_delete($prot);
        }
      }
      $ace_db->memory_cache_delete($gene);
      &object_cache_delete($gene);
    },
    
    VAL_wb_gene_id  => sub{ &name( $_[0], 'Gene' ) },

    #==================================================
    # SMap
    #### SMap ####
    COL_chromosome_start => qq(int(10) unsigned default NULL),
    COL_chromosome_end   => qq(int(10) unsigned default NULL),
    COL_chromosome_strand=> qq(tinyint(2) default NULL),
    VAL_chromosome_name => sub{
      # Strip any leading 'CHROMOSOME_' prefix from name
      my $gene = shift;
      my $name = (&physical_position($gene))[0] || return;
      $name =~ s/^CHROMOSOME_//;
####test by RF_010410
      ##print "L101_geneLoader_name=$name\n";
#################################
      return $name },
    VAL_chromosome_start     => sub{ (&physical_position($_[0]))[1] },
    VAL_chromosome_end       => sub{ (&physical_position($_[0]))[2] },
    VAL_chromosome_strand    => sub{ (&physical_position($_[0]))[3] },
    VAL_source_clone   => sub{ &names_at($_[0],'SMap.S_parent.Sequence') },
    VAL_source_genbank => sub{ 
      if( my $sequence = &cached_at($_[0],'SMap.S_parent.Sequence') ){
        foreach my $db ( &names_at( $sequence, 'DB_info.Database' ) ){
          my $id = &names_at( $sequence, "DB_info.Database.$db.NDB_AC" );
          return $id if $id;
        }
      }
      return undef;
    },

    #==================================================
    # Identity
    VAL_version         => sub{ &names_at($_[0],'Identity.Version') },
    VAL_species         => sub{
	&names_at($_[0],'Identity.Species'); 
	####test by RF_010711
	##my $t_val_species = &names_at($_[0],'Identity.Species');
	##print "GeneLoader_L141_t_val_species=$t_val_species\n";
####################################
	},
    VAL_identity_status => sub{ &names_at($_[0],'Identity.Status') },

    # Unique names
    VAL_cgc_name      => sub{ &names_at($_[0],'Identity.Name.CGC_name' ) },
    VAL_sequence_name => sub{ &names_at($_[0],'Identity.Name.Sequence_name') },
    VAL_intronerator  => sub{ &names_at($_[0],'Identity.Name.Sequence_name') },
    VAL_eugenes       => sub{ &names_at($_[0],'Identity.Name.Sequence_name') },
    VAL_public_name   => sub{ &names_at($_[0],'Identity.Name.Public_name' ) },

    # All names
    VAL_name_dmlist => sub{
      my $gene_d = &object_data_cache($_[0]);
      return join( " | ", @{$gene_d->{all_names}} );
    },
    "TBL_${dataset}__name__dm" => {
      OBJECTS => sub{
        my $gene_d = &object_data_cache($_[0]);
        return[ @{$gene_d->{all_names}} ];
      },
      VAL_name => sub{ ref($_[0]) ? undef : $_[0] },
    },

    # Molecular_name
    VAL_molecular_name_dmlist => sub{
      my $gene_d = &object_data_cache($_[0]);
      return join( " | ", @{$gene_d->{molecular_names}} );
    },
    "TBL_${dataset}__molecular_name__dm" => {
      OBJECTS => sub{
        my $gene_d = &object_data_cache($_[0]);
        return[ @{$gene_d->{molecular_names}} ];
      },
      VAL_molecular_name  => sub{ ref($_[0]) ? undef : $_[0] },
    },

    # Other_name
    VAL_other_name_dmlist => sub{
      my $gene_d = &object_data_cache($_[0]);
      return join( " | ", @{$gene_d->{other_names}} );
    },
    "TBL_${dataset}__other_name__dm" => {
      OBJECTS         => sub{
        my $gene_d = &object_data_cache($_[0]);
        return[ @{$gene_d->{other_names}} ];
      },
      VAL_other_name  => sub{ ref($_[0]) ? undef : $_[0] },
    },

    #----------
    # History
    "TBL_${dataset}__history__dm" => {
      OBJECTS => sub{
        my $gene = shift;
        my @histories = ();
        foreach my $hist( &at( $gene,'Identity.History.Version_change' ) ){
          my %hdata;
          $hdata{version}  = $hist->name;
          $hdata{date}     = $hist->right(1) ? $hist->right(1)->name : undef;
          $hdata{person}   = $hist->right(2) ? $hist->right(2)->name : undef;
          $hdata{action}   = $hist->right(4) ? $hist->right(4)->name : undef;
          $hdata{acted_on} = $hist->right(5) ? $hist->right(5)->name : undef;
          if( my $person = $hist->right(2) ){
            $person = &object_cache($person);
            $hdata{person_name} = &at($person,"Name.Standard_name");
          }
          push( @histories, {%hdata});
        }
        return[ @histories ];
      },
      VAL_version     => &cached_val_sub('version'),
      VAL_date        => &cached_val_sub('date'),
      VAL_person      => &cached_val_sub('person'),
      VAL_person_name => &cached_val_sub('person_name'),
      VAL_action      => &cached_val_sub('action'),
      VAL_acted_on    => &cached_val_sub('acted_on'),
    },
    "VAL_history_dminfo" => sub{
      join( $LIST_SEPARATOR,
            map{ 
              '['.($_->right(1) ? $_->right(1)->name : '').'] '.
                  ($_->right(4) ? $_->right(4)->name : '') }
            &at( $_[0],'Identity.History.Version_change' ) );
    },


    #==================================================
    # Gene_info 

    #--------------------
    # Gene_class
    VAL_gene_class       => sub{&names_at($_[0],'Gene_info.Gene_class') },
    VAL_gene_class_description => sub{
      my $class_obj = cached_at($_[0],'Gene_info.Gene_class');
      return $class_obj ? &names_at($class_obj,'Description') : undef;
    },

    #--------------------
    # Laboratory
    &insert_dimension('Laboratory','Gene_info.Laboratory',$dataset),
    
    #------------------------------
    # Allele
    VAL_reference_allele => sub{&names_at($_[0],'Gene_info.Reference_Allele')},
    &insert_simple_dimension('Gene_info.Allele',$dataset,'allele'),

    #--------------------
    # Phenotype
    VAL_phenotype => sub{ &dmlist_at($_[0],'Gene_info.Phenotype') },

    #--------------------
    # Complementation_data
    &insert_simple_dimension('Gene_info.Complementation_data',$dataset,
                             'complementation_data'),

    #--------------------
    # Strain
    &insert_dimension('Strain','Gene_info.Strain',$dataset),

    #--------------------
    # In_cluster
    &insert_simple_dimension('Gene_info.In_cluster',$dataset,'gene_cluster'),

    #--------------------
    # GO_terms.
    "TBL_${dataset}__go_term__dm"    => { # Evidence codes come from the CDS
      OBJECTS => sub{ 
        # Use a gene-specific method that adds the evidence codes to
        # the object_data_cache for the go terms. 
        return[ &_go_data($_[0]) ];
      },
      VAL_go_term        => &cached_val_sub('name'),
      VAL_term           => &cached_val_sub('term'),
      VAL_type           => &cached_val_sub('type'),
      VAL_info           => &cached_val_sub('info'),
      VAL_evidence_code  => &cached_val_sub('evidence'),
      VAL_paper          => &cached_val_sub('paper'),
      VAL_paper_info     => &cached_val_sub('paper_info'),
    },
    "VAL_go_term_dmlist" => sub{ 
      &dmlist_at($_[0],'Gene_info.GO_term');
    },
    "VAL_go_term_dminfo" => sub{
      join( $LIST_SEPARATOR,
            map{ $_->{info} }
            &_go_data($_[0]) );
    },
    
    #--------------------
    # Contained_in_operon
    &insert_dimension('Operon','Gene_info.Contained_in_operon',
                      $dataset,'operon'),

    #--------------------
    # Ortholog
    &insert_dimension('Gene','Gene_info.Ortholog',$dataset,'ortholog'),
####added by RF_011211
     #--------------------
    # Paralog
    &insert_dimension('Gene','Gene_info.Paralog',$dataset,'paralog'),
##############################################

    #--------------------
    # Structured_description
    VAL_provisional_description => sub{
      &dmlist_at($_[0],
                 'Gene_info.Structured_description.Provisional_description');
    },
    VAL_concise_description => sub{
      &dmlist_at($_[0],
                 'Gene_info.Structured_description.Concise_description');
    },
    VAL_other_description => sub{
      &dmlist_at($_[0],
                 'Gene_info.Structured_description.Other_description');
    },
    VAL_sequence_feature_description => sub{
      &dmlist_at($_[0],
                 'Gene_info.Structured_description.Sequence_features');
    },
    VAL_functional_pathway_description => sub{
      &dmlist_at($_[0],
                 'Gene_info.Structured_description.Functional_pathway');
    },
    VAL_functional_pathway_interaction => sub{
      &dmlist_at($_[0],
           'Gene_info.Structured_description.Functional_physical_interaction');
    },
    VAL_biological_process_description => sub{
      &dmlist_at($_[0],
                 'Gene_info.Structured_description.Biological_process');
    },
    VAL_molecular_function_description => sub{
      &dmlist_at($_[0],
                 'Gene_info.Structured_description.Molecular_function');
    },
    VAL_expression_description => sub{
      &dmlist_at($_[0],
                 'Gene_info.Structured_description.Expression');
    },

    #==================================================
    # Molecular_info
    # Mainly handled by CDS main table, other than counts etc
    VAL_cds_count => sub{ 
      scalar( &names_at($_[0],'Molecular_info.Corresponding_CDS') );
    },
    VAL_transcript_count => sub{
      scalar( &names_at($_[0], 'Molecular_info.Corresponding_transcript'),
              map{ &names_at($_, 'Visible.Corresponding_transcript') }
              &cached_at($_[0],'Molecular_info.Corresponding_CDS') );
    },
    VAL_cds_dmlist => sub{
      &dmlist_at($_[0],'Molecular_info.Corresponding_CDS');
    },
    VAL_protein_dmlist => sub{
      join( $LIST_SEPARATOR, 
            map{ &names_at($_, 'Visible.Corresponding_protein') }
            &cached_at($_[0],'Molecular_info.Corresponding_CDS') );
    },
    VAL_transcript_dmlist => sub{
      my @trans = &names_at($_[0], 'Molecular_info.Corresponding_transcript');
      join( $LIST_SEPARATOR,
            &names_at($_[0], 'Molecular_info.Corresponding_transcript'),
            map{ &names_at($_, 'Visible.Corresponding_transcript') }
            &cached_at($_[0],'Molecular_info.Corresponding_CDS') );
    },

    #==================================================
    # Experimental_info

    #--------------------
    # RNAi_result
    "TBL_${dataset}__rnai__dm" => {
      # Same as basic RNAi dimension, but adds evidence (primary/secondary)
      OBJECTS => sub{
        my $gene = shift;
        my @rows;
        foreach my $row( &cached_data_at
                         ( $gene, 'Experimental_info.RNAi_result' ) ){
          my %this_row = %$row; # Dereference as adding gene-specific data
          if( my $root 
              = &at($gene, "Experimental_info.RNAi_result.$row->{name}") ){
            $this_row{evidence} = $root->right;
####test by RF_011211
	    ##print "GeneLoader_L388_rnai__dm_after_sub_at_call\n";
#######################################################
          }
          push @rows, {%this_row};
        }
        return[ @rows ];
      },
      VAL_rnai                  => &cached_val_sub('name'),
      VAL_history_name          => &cached_val_sub('history_name'),
      VAL_experiment_date       => &cached_val_sub('experiment_date'),
      VAL_experiment_strain     => &cached_val_sub('experiment_strain'),
      VAL_experiment_author     => &cached_val_sub('experiment_author'),
      VAL_experiment_laboratory => &cached_val_sub('experiment_laboratory'),
      VAL_info                  => &cached_val_sub('info'),
      VAL_evidence              => &cached_val_sub('evidence'),

      VAL_phenotype_count       => sub{ 
        scalar( @{$_[0]->{'phenotype'}||[]} ) || undef },

      VAL_phenotype             => sub{ 
        join( $LIST_SEPARATOR, 
              map{$_->{'name'}} 
              @{$_[0]->{'phenotype'}||[]} ) },

      VAL_phenotype_info        => sub{ 
        join( $LIST_SEPARATOR, 
              map{$_->{'info'}} 
              @{$_[0]->{'phenotype'}||[]} ) },

      VAL_observed_phenotype_count => sub{
        scalar( grep{$_->{PhenotypeInfo_Observed}} 
                @{$_[0]->{'phenotype'}||[]} ) || undef },

      VAL_observed_phenotype    => sub{ 
        join( $LIST_SEPARATOR, 
              map{$_->{'name'}}
              grep{$_->{PhenotypeInfo_Observed}} 
              @{$_[0]->{'phenotype'}||[]} ) },

      VAL_observed_phenotype_info => sub{ 
        join( $LIST_SEPARATOR, 
              map{$_->{'info'}}
              grep{$_->{PhenotypeInfo_Observed}} 
              @{$_[0]->{'phenotype'}||[]} ) },

#      VAL_phenotype_name        => sub{
#        join( $LIST_SEPARATOR, 
#              map{$_->{'primary_name'}} 
#              @{$_[0]->{'phenotype'}||[]} ) },

      VAL_inhibits_gene_count   => sub{ 
        scalar( @{$_[0]->{'inhibits_gene'}||[]} ) || undef },
      
      VAL_inhibits_gene         => sub{
        join( $LIST_SEPARATOR, 
              map{$_->{'name'}} 
              @{$_[0]->{'inhibits_gene'}||[]} ) },

      #VAL_phenotype_desc        => &cached_val_sub('phenotype_desc'),
      #VAL_phenotype_info        => &cached_val_sub('phenotype_info'),
      #VAL_inhibits_gene         => &cached_val_sub('inhibits_gene'),
      #VAL_inhibits_gene_primary => &cached_val_sub('inhibits_gene_primary'),
      #VAL_inhibits_gene_secondary=>&cached_val_sub('inhibits_gene_secondary'),
      IDX => ['rnai','history_name','evidence','inhibits_gene_count'],
    },
    "VAL_rnai_dmlist" => sub{ 
      &dmlist_at($_[0],'Experimental_info.RNAi_result');
    },
    "VAL_rnai_name_dmlist" => sub{
      join( $LIST_SEPARATOR,
            map{ $_->{history_name} || () } 
            &cached_data_at($_[0],'Experimental_info.RNAi_result') );
    },
    "VAL_rnai_dminfo" => sub{
      join( $LIST_SEPARATOR,
            map{ $_->{info} } 
            &cached_data_at($_[0],'Experimental_info.RNAi_result') );
    },
    
    
    "TBL_${dataset}__rnai_phenotype__dm" => {
      # Unique list of all rnai phenotypes that target this gene. 
      OBJECTS => sub{
        return[ &_rnai_phenotypes_by_gene( $_[0] ) ];

      },
      VAL_phenotype               => &cached_val_sub('name'),
      VAL_primary_name            => &cached_val_sub('primary_name'),
      VAL_short_name              => &cached_val_sub('short_name'),
      VAL_label                   => &cached_val_sub('phenotype_label'),
      VAL_info                    => &cached_val_sub('phenotype_info'),
      VAL_rnai_count              => &cached_val_sub('rnai_count'),
      VAL_rnai                    => &cached_val_sub('rnai'),
      VAL_rnai_info               => &cached_val_sub('rnai_info'),
      VAL_rnai_observed_count     => &cached_val_sub('rnai_observed_count'),
      VAL_rnai_observed           => &cached_val_sub('rnai_observed'),
      VAL_rnai_observed_info      => &cached_val_sub('rnai_observed_info'),
      VAL_rnai_unobserved_count   => &cached_val_sub('rnai_unobserved_count'),
      VAL_rnai_unobserved         => &cached_val_sub('rnai_unobserved'),
      VAL_rnai_unobserved_info    => &cached_val_sub('rnai_unobserved_info'),
      VAL_rnai_primary_count      => &cached_val_sub('rnai_primary_count'),
      VAL_rnai_primary            => &cached_val_sub('rnai_primary'),
      VAL_rnai_primary_info       => &cached_val_sub('rnai_primary_info'),
      VAL_rnai_secondary_count    => &cached_val_sub('rnai_secondary_count'),
      VAL_rnai_secondary          => &cached_val_sub('rnai_secondary'),
      VAL_rnai_secondary_info     => &cached_val_sub('rnai_secondary_info'),
      VAL_rnai_specific_count     => &cached_val_sub('rnai_specific_count'),
      VAL_rnai_specific           => &cached_val_sub('rnai_specific'),
      VAL_rnai_specific_info      => &cached_val_sub('rnai_specific_info'),
      VAL_rnai_nonspecific_count  => &cached_val_sub('rnai_nonspecific_count'),
      VAL_rnai_nonspecific        => &cached_val_sub('rnai_nonspecific'),
      VAL_rnai_nonspecific_info   => &cached_val_sub('rnai_nonspecific_info'),

      IDX => ['phenotype','primary_name','short_name',
              'rnai_observed_count','rnai_unobserved_count'],
    },
    "VAL_rnai_phenotype_dmlist" => sub{ 
      return join( $LIST_SEPARATOR, 
                   map{$_->{name}} 
                   &_rnai_phenotypes_by_gene($_[0]) );
    },
    "VAL_rnai_phenotype_dminfo" => sub{
      return join( $LIST_SEPARATOR, 
                   map{$_->{phenotype_info}} 
                   &_rnai_phenotypes_by_gene($_[0]) );
    },
    "VAL_rnai_phenotype_observed_count" => sub{ 
      return scalar( grep{$_->{rnai_observed_count}}
                     &_rnai_phenotypes_by_gene($_[0]) );
    },
    "VAL_rnai_phenotype_observed_dmlist" => sub{ 
      return join( $LIST_SEPARATOR, 
                   map{$_->{name}}
                   grep{$_->{rnai_observed_count}}
                   &_rnai_phenotypes_by_gene($_[0]) );
    },
    "VAL_rnai_phenotype_observed_dminfo" => sub{ 
      return join( $LIST_SEPARATOR, 
                   map{$_->{phenotype_info}}
                   grep{$_->{rnai_observed_count}}
                   &_rnai_phenotypes_by_gene($_[0]) );
    },


#    &insert_dimension('RNAi','Experimental_info.RNAi_result',
#                      $dataset,'rnai'),

    #--------------------
    # phenotype
    # Combines the observed variation_phenotypes and rnai_phenotypes
    "TBL_${dataset}__phenotype__dm" => {
      # Unique list of all phenotypes that target this gene. 
      OBJECTS => sub{
        return[ &_phenotypes_by_gene( $_[0] ) ];
      },
      VAL_phenotype               => &cached_val_sub('name'),
      VAL_primary_name            => &cached_val_sub('primary_name'),
      VAL_short_name              => &cached_val_sub('short_name'),
      VAL_label                   => &cached_val_sub('phenotype_label'),
      VAL_info                    => &cached_val_sub('phenotype_info'),
      VAL_rnai_count              => &cached_val_sub('rnai_count'),
      VAL_rnai                    => &cached_val_sub('rnai'),
      VAL_variation_count         => &cached_val_sub('variation_count'),
      VAL_variation               => &cached_val_sub('variation'),
      IDX => ['phenotype','primary_name','short_name',
              'rnai_count','variation_count'],
    },
    "VAL_phenotype_dmlist" => sub{ 
      return join( $LIST_SEPARATOR, 
                   map{$_->{name}} 
                   &_phenotypes_by_gene($_[0]) );
    },
    "VAL_phenotype_dminfo" => sub{
      return join( $LIST_SEPARATOR, 
                   map{$_->{phenotype_info}} 
                   &_phenotypes_by_gene($_[0]) );
    },

    #--------------------
    # Expr_pattern
    &insert_dimension('Expr_pattern','Experimental_info.Expr_pattern',
                      $dataset,'expr_pattern' ),

    #--------------------
    # Transgene
    "TBL_${dataset}__transgene__dm" => {
      OBJECTS      => sub{
        my $gene = $_[0];
        my @transgenes;
        foreach my $type( 'Drives_Transgene', # 'Driven by Gene'
                          'Transgene_product', # 'Reported by Gene'
                          'Rescued_by_transgene', # 'Rescues Gene'
                          ){
          foreach my $tgene( &names_at( $gene,"Experimental_info.$type" ) ){
            push @transgenes, { name => "$tgene", 
                                type => $type,
                                info => "[$tgene] $type" };
          }
        }
        return[ @transgenes ];
      },
      VAL_transgene => &cached_val_sub('name'),
      VAL_info      => &cached_val_sub('info'),
      VAL_type      => &cached_val_sub('type'),
      IDX => ['transgene'],
    },
    VAL_transgene_dmlist=>sub{ 
      join( $LIST_SEPARATOR, 
            &unique_names_at( $_[0],
                              'Experimental_info.Drives_Transgene',
                              'Experimental_info.Transgene_product',
                              'Experimental_info.Rescued_by_transgene') );
    },
    VAL_transgene_dminfo=>sub{
      my $gene = $_[0];
      my @transgenes;
      foreach my $type( 'Drives_Transgene',
                        'Transgene_product',
                        'Rescued_by_transgene' ){
        foreach my $tgene( &names_at( $gene,"Experimental_info.$type" ) ){
          push @transgenes, { name => "$tgene", info => "[$tgene] $type" };
        }
      }
      return join( $LIST_SEPARATOR,
                   map{$_->{info}} @transgenes );
    },

    # DEPRECATED
    &insert_dimension('Transgene','Experimental_info.Drives_Transgene',
                      $dataset,'transgene_drives'),
    &insert_dimension('Transgene','Experimental_info.Transgene_product',
                      $dataset,'transgene_product'),
    &insert_dimension('Transgene','Experimental_info.Rescued_by_transgene',
                      $dataset,'transgene_rescued'),

    #--------------------
    # Gene_regulation
    &insert_dimension('Gene_regulation',
                      'Experimental_info.Gene_regulation.Trans_regulator',
                      $dataset,'trans_regulator'),

    &insert_dimension('Gene_regulation',
                      'Experimental_info.Gene_regulation.Trans_target',
                      $dataset,'trans_target'),

    #--------------------
    # Antibody
    &insert_dimension('Antibody','Experimental_info.Antibody',
                      $dataset,'antibody' ),

    #--------------------
    # Microarray_results
    &insert_simple_dimension('Experimental_info.Microarray_results',$dataset,
                             'microarray_results'),

    #--------------------
    # SAGE_transcript
    # TODO: proper dimension
    &insert_simple_dimension('Experimental_info.SAGE_transcript',$dataset,
                             'sage_transcript'),

    #--------------------
    # Y2H
    # TODO: Collapse into single dimension
    ##&insert_simple_dimension('Experimental_info.Y2H_bait',$dataset,
                             ##'y2h_bait'),
    ##&insert_simple_dimension('Experimental_info.Y2H_target',$dataset,
                             ##'y2h_target'),

    &insert_simple_dimension('Experimental_info.YH_bait',$dataset,
                             'yh_bait'),
    &insert_simple_dimension('Experimental_info.YH_target',$dataset,
                             'yh_target'),

    #--------------------
    # 3d_data
    &insert_simple_dimension('Experimental_info.3d_data',$dataset,
                             'three_d_data'),

    #--------------------
    # Interaction
    &insert_simple_dimension('Experimental_info.Interaction',$dataset,
                             'interaction'),
    

    #==================================================
    # Map_info 

    #----------
    # Map_info.Map
    # SKIP

    #----------
    # Map_info.Well_ordered/Landmark_gene/Pseudo_map_position
    VAL_map_well_ordered => sub{ 
      $_[0]->at('Map_info.Well_ordered') ? 1 : undef;
    },
    VAL_map_landmark_gene => sub{
      $_[0]->at('Map_info.Landmark_gene') ? 1 : undef;
    },
    VAL_map_pseudo_position => sub{
      $_[0]->at('Map_info.Pseudo_map_position') ? 1 : undef;
    },
    
    #----------
    # Map_info.Hide_under/Representative_for
    &insert_simple_dimension('Map_info.Hide_under',$dataset,
                             'map_hide_under_gene'),
    &insert_simple_dimension('Map_info.Representative_for',$dataset,
                             'map_representative_for_gene'),
    
    #----------
    # Map_info.Positive/Negative
    &insert_simple_dimension('Map_info.Positive.Inside_rearr',$dataset,
                             'map_positive_rearrangement'),
    &insert_simple_dimension('Map_info.Positive.Positive_clone',$dataset,
                             'map_positive_clone'),
    &insert_simple_dimension('Map_info.Negative.Outside_rearr',$dataset,
                             'map_negative_rearrangement'),
    &insert_simple_dimension('Map_info.Negative.Negative_clone',$dataset,
                             'map_negative_clone'),
    
    #----------
    # Map_info.Mapping_data
    &insert_simple_dimension('Map_info.Mapping_data.2_point',$dataset,
                             'map_2_point'),
    &insert_simple_dimension('Map_info.Mapping_data.Multi_point',$dataset,
                             'map_multi_point'),
    &insert_simple_dimension('Map_info.Mapping_data.Pos_neg_data',$dataset,
                             'map_pos_neg_data' ),

    #----------
    # Map_info.Map
    # Map_info.Interpolated_map_position
    VAL_map_map => sub{
      if( my $map = &at($_[0],"Map_info.Map") ){
        return $map;
      }
      if( my $map = &at($_[0],"Map_info.Interpolated_map_position") ){
        return $map;
      }
    },
    VAL_map_position => sub{ # Either experimental or interpolated position
      if( my $map_name = &at($_[0],"Map_info.Map") ){ 
        my $position = &at($_[0],"Map_info.Map.$map_name.Position");
#         warn( "==> $position" );
        if( defined( $position ) ){ return $position }
      }
      if( my $map_name = &at($_[0],"Map_info.Interpolated_map_position") ){
        my $position=&at($_[0],"Map_info.Interpolated_map_position.$map_name");
        if( defined( $position ) ){ return $position }
      }
    },
    VAL_map_interpolated_position => sub{ # Interpolated position only
      if( my $map_name = &at($_[0],"Map_info.Interpolated_map_position") ){
        return &at($_[0],"Map_info.Interpolated_map_position.$map_name");
      }
    },
    VAL_map_experimental_position => sub{ # Experimental position only
      if( my $map_name = &at($_[0],"Map_info.Map") ){ 
        return &at($_[0],"Map_info.Map.$map_name.Position");
      }
    },

    COL_map_position => qq| float default NULL |,
    COL_map_experimental_position => qq| float default NULL |,
    COL_map_interpolated_position => qq| float default NULL |,

    #==================================================
    # Reference
    &insert_dimension('Paper','Reference',$dataset),

    #==================================================
    # Remark
    VAL_remark           => sub{ &dmlist_at($_[0],'Remark') },

    #==================================================
    # Other
    COL_empty           => qq| int(1)  unsigned default NULL |,
    VAL_empty           => sub{ }, # Hack for sequence dumping and  
                                   # attribute arrangement. must be NULL!

    #==================================================
    # CDS main 
    #==================================================
    "TBL_${dataset}__cds__main" => {
      OBJECTS      => sub{ 
        # Allow for mixed coding/non-coding/pseudogene transcripts by
        # returning the Gene if it has Corresponding_transcript.
        my $gene = $_[0];
        my @cds;
        my $acedb = $gene->db;
        foreach my $cds( $gene->at('Molecular_info.Corresponding_CDS') ){
          my $cds_obj  = $acedb->fetch( -class=>$cds->class,
                                        -name =>$cds->name,
                                        -fill => 1);
####test by RF_021411
	  ##print "GeneLoader_L788_cds_obj=$cds_obj\n";
	  ##my $temp = 'Visible.Corresponding_protein';
	
	  ##my ($prot);
	  ##if(defined($temp)){
	      
	      ##($prot) = $cds_obj->at($temp);
	  ##}
###########################################
         my( $prot ) = $cds_obj->at('Visible.Corresponding_protein');
          if( $prot ){ # Transposon_CDS have a CDS but no protein!
            my $prot_obj = $acedb->fetch( -class=>$prot->class,
                                          -name =>$prot->name,
                                          -fill => 1);
            &object_cache($prot_obj);
          }
          push @cds, $cds_obj;
          &object_cache($cds_obj);
        }

        if( &at( $gene,'Molecular_info.Corresponding_transcript' ) or
            &at( $gene,'Molecular_info.Corresponding_pseudogene' ) ){
          push @cds, $gene;
        }
        return[ @cds ];
      },
      VAL_cds_name      => sub{&name($_[0],'CDS')},
      VAL_wb_wormpep_id => sub{&names_at($_[0],
                               'Visible.Corresponding_protein')}, 
 
      # Override gene__gene__main's cds_count and cds_dmlist
      VAL_cds_count  => sub{$_[0] ? 1 : undef },
      VAL_cds_dmlist => sub{$_[0] ? "$_[0]" : ''},

      #==================================================
      # DB_info
      # Various CDS/protein identifiers by database name/field
      VAL_swissprot_id  => sub{&names_at($_[0], 
                               'DB_info.Database.SwissProt.SwissProt_ID')},
      VAL_swissprot_ac  => sub{&names_at($_[0],
                               'DB_info.Database.SwissProt.SwissProt_AC')},
      VAL_ndb_gi        => sub{&names_at($_[0],
                               'DB_info.Database.NDB.GI_number')},
      VAL_trembl_entry  => sub{&names_at($_[0],
                               'DB_info.Database.TREMBL')},
      VAL_trembl_ac     => sub{&names_at($_[0],
                               'DB_info.Database.TREMBL.TrEMBL_AC')},
      VAL_ncbi_refseq   => sub{&names_at($_[0],    ##test by RF_011911
                               'DB_info.Database.NCBI.RefSeq')},
      ##VAL_ncbi_refseq   => sub{&names_at_refseq($_[0],  ##test by RF_011911
                              ## 'DB_info.Database')},
      VAL_ncbi_aceview  => sub{&names_at($_[0],
                               'DB_info.Database.NCBI.AceView')},
      VAL_protein_id    => sub{&names_at($_[0],  ##test by RF_011911
                               'DB_info.Protein_id.?Sequence.UNIQUE.Text')},
      ##VAL_protein_id    => sub{&names_at_protein_id($_[0],    ##test by RF_011911
                               ##'DB_info.Protein_id')},

      # Merge CDS/protein identifiers into single dimension
      # TODO - take IDs from Protein object instead?
      # TODO - dmlist and dminfo?
      &insert_dimension( 'DB_info.Database','DB_info.Database',
                         $dataset, 'cds_xref' ),
      
      
      VAL_db_remark     => sub{&dmlist_at($_[0],'DB_info.DB_remark')},
      VAL_cds_remark    => sub{&dmlist_at($_[0],'Visible.Remark')},


      #==================================================
      # Visible

      #--------------------
      # Visible.Gene - SKIP

      #--------------------
      # Visible.Gene_history - SKIP

      #--------------------
      # Visible.Corresponding_transcript
      VAL_transcript_dmlist => sub{
        my $obj = $_[0] || $_[1];
        join( $LIST_SEPARATOR,
              $obj->class eq 'CDS'?
              &names_at($obj,'Visible.Corresponding_transcript') :
              &names_at($obj,'Molecular_info.Corresponding_transcript') );
      },

      "TBL_${dataset}__transcript__dm" => {
        OBJECTS =>sub{ 
          # For coding transcripts, transcript is on CDS,
          # For non-coding, transcript (or pseudogene) is on Gene
          # Transcript dm us used as a proxy for pseudogene
          my $cds = $_[0];
          my $gene = $_[1];

          my @trans = ();
          push @trans, (&at($gene,'Molecular_info.Corresponding_transcript'),
                        &at($gene,'Molecular_info.Corresponding_pseudogene'));
          if( $cds && $cds->class eq 'CDS' ){
            my @ctrans = &at($cds,'Visible.Corresponding_transcript');
            unless( scalar @ctrans ){ @ctrans = ($cds) } # Briggsae CDS?
            push @trans, @ctrans;
          }
          return[ @trans ];
        },
        VAL_transcript    => sub{ &name( $_[0], 'Transcript') ||
                                  &name( $_[0], 'Pseudogene' ) || undef },
        VAL_coding_status => sub{ 
          # Check for Pseudogene
          my $status = 'unknown';
          unless( $_[0] ){
            # Gene with no CDS or transcript
            return $status
          }
          if( $_[0]->class eq 'Transcript' ){
            # This is the nornal case. Check for CDS
            if( $_[0]->at('Visible.Corresponding_CDS') ){ 
              $status = 'coding';
            } else {
              # Check for explicit status
              $status = &at($_[0],'Properties.Transcript') || 'non_coding';
            }
          }
          elsif( $_[0]->class eq 'CDS' ){
            # Probably a briggsae CDS with no transcript
            $status = 'coding';
          }
          elsif( $_[0]->class eq 'Pseudogene' ){ 
            $status = &at($_[0],'Type') || 'pseudogene';
          } 
          # Probably an un-located gene
          $status ||= 'unknown';
          return $status;
        },
        VAL_utr_status    => sub{
          my %utr;
          map{ if( $_->{'5utr_start'} ){ $utr{5} = 'utr5' }
               if( $_->{'3utr_start'} ){ $utr{3} = 'utr3' } }
          grep{ $_->{'transcript_name'} and $_[0]
                    and $_->{'transcript_name'} eq $_[0]}
          &process_structure(@_[1..2]);
          my $str = join( "+", ($utr{5}||()),($utr{3}||()) );
          return $str || 'neither';
        },

        VAL_exon_count => sub{ 
          scalar( grep{ $_->{'transcript_name'} and $_[0] 
                        and $_->{'transcript_name'} eq $_[0] }
                  &process_structure(@_[1..2]) ) ;
        },
        COL_exon_count => qq| int(4) unsigned default NULL |,

        VAL_exon_count_copy => sub{ 
          scalar( grep{ $_->{'transcript_name'} and $_[0] 
                        and $_->{'transcript_name'} eq $_[0] }
                  &process_structure(@_[1..2]) ) ;
        },
        COL_exon_count_copy => qq| int(4) unsigned default NULL |,

        VAL_transcript_length  => sub{ 
          my $l  = 0;
          my @exons = ( grep{ $_->{'transcript_name'} and $_[0]
                              and $_->{'transcript_name'} eq $_[0]} 
                        &process_structure(@_[1..2]) );
          foreach my $e( @exons ){
            if( $e->{exon_end} ){
              $l += ( $e->{exon_end} - $e->{exon_start} + 1 );
            }
          }
          return $l;
        },
        COL_transcript_length => qq| int(8) unsigned default NULL |,

        VAL_cds_length  => sub{ 
          my $l  = 0;
          my @exons = ( grep{ $_->{'transcript_name'} and $_[0]
                              and $_->{'transcript_name'} eq $_[0]} 
                        &process_structure(@_[1..2]) );
          foreach my $e( @exons ){
            if( $e->{coding_end} ){
              $l += ( $e->{coding_end} - $e->{coding_start} + 1 );
            }
          }
          return $l;
        },
        COL_cds_length => qq| int(8) unsigned default NULL |,

        VAL_peptide_length  => sub{ 
          my $l  = 0;
          my @exons = ( grep{ $_->{'transcript_name'} and $_[0]
                              and $_->{'transcript_name'} eq $_[0]} 
                        &process_structure(@_[1..2]) );
          foreach my $e( @exons ){
            if( $e->{coding_end} ){
              $l += ( $e->{coding_end} - $e->{coding_start} + 1 );
            }
          }
          return $l ? ($l/3) - 1 : 0; # Note; -1 to remove stop codon.
        },
        COL_peptide_length => qq| int(8) unsigned default NULL |,


        VAL_utr3_length  => sub{ 
          my $l  = 0;
          my @exons = ( grep{ $_->{'transcript_name'} and $_[0]
                              and $_->{'transcript_name'} eq $_[0]} 
                        &process_structure(@_[1..2]) );
          foreach my $e( @exons ){
            if( $e->{'5utr_end'} ){
              $l += ( $e->{'5utr_end'} - $e->{'5utr_start'} + 1 );
            }
          }
          return $l;
        },
        COL_utr3_length => qq| int(8) unsigned default NULL |,


        VAL_utr5_length  => sub{ 
          my $l  = 0;
          my @exons = ( grep{ $_->{'transcript_name'} and $_[0]
                              and $_->{'transcript_name'} eq $_[0]} 
                        &process_structure(@_[1..2]) );
          foreach my $e( @exons ){
            if( $e->{'5utr_end'} ){
              $l += ( $e->{'5utr_end'} - $e->{'5utr_start'} + 1 );
            }
          }
          return $l;
        },
        COL_utr5_length => qq| int(8) unsigned default NULL |,

        VAL_intron_length  => sub{ 
          my $l  = 0;
          my @exons = ( grep{ $_->{'transcript_name'} and $_[0]
                              and $_->{'transcript_name'} eq $_[0]} 
                        &process_structure(@_[1..2]) );
          foreach my $e( @exons ){
            if( $e->{'intron_end'} ){
              $l += ( $e->{'intron_end'} - $e->{'intron_start'} + 1 );
            }
          }
          return $l;
        },
        COL_intron_length => qq| int(8) unsigned default NULL |,

      },

      #--------------------
      # Visible.Corresponding_protein
      # Override gene__gene__main's dmlist
      VAL_protein_dmlist => sub{ 
        &names_at($_[0], 'Visible.Corresponding_protein');
      },

      #----------
      VAL_brief_ident   => sub{&at($_[0],
                                   'Visible.Brief_identification')},
            
      VAL_prediction_status => sub{&at($_[0],
                                       'Properties.Coding.Prediction_status')},

      # Visible.Homology_group - split COG/InParanoid_group
      "TBL_${dataset}__homology_group_cog__dm" => {
        OBJECTS => sub{ # Get from peptide.
          my($pep) = &at($_[0],'Visible.Corresponding_protein');
          $pep ? return [ grep{ $_->{group_type} eq 'COG' }
                          map{ @{$_->{homology_group} } }
                          &object_data_cache($pep,'Protein_for_gene') ] : [];
        },
        VAL_homology_group => &cached_val_sub('name'),
        VAL_group_type     => &cached_val_sub('group_type'),
        VAL_title          => &cached_val_sub('title'),
        VAL_cog_type       => &cached_val_sub('cog_type'),
        VAL_cog_code       => &cached_val_sub('cog_code'),
        IDX => ['homology_group','group_type','cog_type','cog_code'],
      },
      VAL_homology_group_cog_dmlist => sub{
        my($pep) = &at($_[0],'Visible.Corresponding_protein');
        $pep ? return join( $LIST_SEPARATOR,
                            map{ $_->{name} }
                            grep{ $_->{group_type} eq 'COG' }
                            map{ @{$_->{homology_group} } }
                            &object_data_cache($pep,'Protein_for_gene')) : ();
      },
      VAL_homology_group_cog_dminfo => sub{
        my($pep) = &at($_[0],'Visible.Corresponding_protein');
        $pep ? return join( $LIST_SEPARATOR,
                            map{ $_->{info} }
                            grep{ $_->{group_type} eq 'COG' }
                            map{ @{$_->{homology_group} } }
                            &object_data_cache($pep,'Protein_for_gene')) : ();
      },

      "TBL_${dataset}__homology_group_inparanoid__dm" => {
        OBJECTS => sub{ # Get from peptide.
          my($pep) = &cached_at($_[0],'Visible.Corresponding_protein');
          $pep ? return [ grep{ $_->{group_type} eq 'InParanoid_group' }
                          map{ @{$_->{homology_group} } }
                          &object_data_cache($pep,'Protein_for_gene') ] : [];
        },
        VAL_homology_group => &cached_val_sub('name'),
        VAL_group_type     => &cached_val_sub('group_type'),
        VAL_title          => &cached_val_sub('title'),
        IDX => ['homology_group','group_type'],
      },
      VAL_homology_group_inparanoid_dmlist => sub{
        my($pep) = &at($_[0],'Visible.Corresponding_protein');
        $pep ? return join( $LIST_SEPARATOR,
                            map{ $_->{name} }
                            grep{ $_->{group_type} eq 'InParanoid_group' }
                            map{ @{$_->{homology_group} } }
                            &object_data_cache($pep,'Protein_for_gene')) : ();
      },
      VAL_homology_group_inparanoid_dminfo => sub{
        my($pep) = &at($_[0],'Visible.Corresponding_protein');
        $pep ? return join( $LIST_SEPARATOR,
                            map{ $_->{info} }
                            grep{ $_->{group_type} eq 'InParanoid_group' }
                            map{ @{$_->{homology_group} } }
                            &object_data_cache($pep,'Protein_for_gene')) : ();
      },

      
      "TBL_${dataset}__motif__dm" => {
        OBJECTS => sub{ # Get from peptide. 
          my($pep) = &at($_[0],'Visible.Corresponding_protein');
          $pep ? return [ map{ @{$_->{motif} } }
                          &object_data_cache($pep,'Protein_for_gene') ] : [];
        },
        VAL_motif     => &cached_val_sub('name'),
        VAL_title     => &cached_val_sub('title'),
        VAL_db        => &cached_val_sub('database'),
        VAL_accession => &cached_val_sub('accession'),
        IDX => ['motif','db','accession'],
      },
      "TBL_${dataset}__motif_namelist__dm" => { 
        # Used for searching domain - by WB ID and native id
        OBJECTS => sub{
          my($pep) = &at($_[0],'Visible.Corresponding_protein');
          $pep ? return [ map{ $_->{name},$_->{accession} }
                          map{ @{$_->{motif} } }
                          &object_data_cache($pep,'Protein_for_gene') ] : [];
        },
        VAL_name  => sub{ ref($_[0]) ? '' : $_[0] },
        IDX => ['name'],
      },
      VAL_motif_dmlist => sub{
        my($pep) = &at($_[0],'Visible.Corresponding_protein');
        $pep ? return join( $LIST_SEPARATOR,
                            map{ $_->{name} }
                            map{ @{$_->{motif} } }
                            &object_data_cache($pep,'Protein_for_gene')) : ();
      },
      VAL_motif_dminfo => sub{
        my($pep) = &at($_[0],'Visible.Corresponding_protein');
        $pep ? return join( $LIST_SEPARATOR,
                            map{ $_->{info} }
                            map{ @{$_->{motif} } }
                            &object_data_cache($pep,'Protein_for_gene')) : ();
      },



      #--------------------
      # Visible.GO_term
      # Use ?Gene anotations
      # Can't use create_dimension directly due to evidence code.
      #"TBL_${dataset}__go_term__dm"    => { # Evidence codes come from the CDS
      #  OBJECTS => sub{ [ &cached_data_at($_[0],'Visible.GO_term') ] },
      #  VAL_go_term  => sub{ ref($_[0]) eq 'HASH' ? $_[0]->{name} : undef },
      #  VAL_term     => sub{ ref($_[0]) eq 'HASH' ? $_[0]->{term} : undef },
      #  VAL_type     => sub{ ref($_[0]) eq 'HASH' ? $_[0]->{type} : undef },
      #  VAL_evidence_code  => sub{ 
      #    ref( $_[0] ) eq 'HASH' ? 
      #        &at($_[1],'Visible.GO_term.'.$_[0]->{name}) : undef; 
      #  }, 
      #},
      #"VAL_go_term_dmlist" => sub{ 
      #  &dmlist_at($_[0],'Visible.GO_term');
      #},
      #"VAL_go_term_dminfo" => sub{
      #  join( $LIST_SEPARATOR,
      #        map{ $_->{info} }
      #        &cached_data_at($_[0],'Visible.GO_term') );
      #},

      #--------------------
      # Visible.Corresponding_oligo_set
      &insert_dimension
          ('Oligo_set','Visible.Corresponding_oligo_set',$dataset),

      ####test by RF_020811
      #--------------------
      #Visible, Corresponding_PCR_product
      &insert_dimension
          ('PCR_product', 'Visible.Corresponding_PCR_product',$dataset),

      
      "TBL_${dataset}__structure__dm" =>{
        # Uses a subroutine to generate 'custom' objects, each representing
        # an exon. Each 'object' is simply a hashref containing column-value 
        # pairs 
        OBJECTS              => sub{ [ &process_structure(@_) ] },
        COL_wb_gene_id       => qq| varchar(64)      NOT     NULL |,
        COL_transcript_name  => qq| varchar(64)      default NULL |,
        COL_chromosome_name  => qq| varchar(64)      default NULL |,
        COL_exon_name        => qq| varchar(64)      default NULL |,
        COL_rank             => qq| int(5)  unsigned default NULL |,
        COL_exon_strand      => qq| tinyint(2)       default NULL |,
        COL_exon_start       => qq| int(10) unsigned default NULL |,
        COL_exon_end         => qq| int(10) unsigned default NULL |,
        COL_coding_start     => qq| int(10) unsigned default NULL |,
        COL_coding_end       => qq| int(10) unsigned default NULL |,
        COL_5utr_start       => qq| int(10) unsigned default NULL |,
        COL_5utr_end         => qq| int(10) unsigned default NULL |,
        COL_3utr_start       => qq| int(10) unsigned default NULL |,
        COL_3utr_end         => qq| int(10) unsigned default NULL |,
        COL_5intergenic_start=> qq| int(10) unsigned default NULL |,
        COL_5intergenic_end  => qq| int(10) unsigned default NULL |,
        COL_3intergenic_start=> qq| int(10) unsigned default NULL |,
        COL_3intergenic_end  => qq| int(10) unsigned default NULL |,
        COL_codon_table_id   => qq| int(4)  unsigned default NULL |,
        COL_seq_edit         => qq| varchar(64)      default NULL |,
        VAL_wb_gene_id       => sub{ $_[0]->{'wb_gene_id'  } },
        VAL_transcript_name  => sub{ $_[0]->{'transcript_name'  } },
        VAL_chromosome_name  => sub{
          # Strip any leading 'CHROMOSOME_' prefix from name
          my $name = $_[0]->{'chromosome_name'} || return;
          $name =~ s/^CHROMOSOME_//;
          return $name },
        VAL_exon_name        => sub{ $_[0]->{'exon_name'   } },
        VAL_rank             => sub{ $_[0]->{'rank'        } },
        VAL_exon_strand      => sub{ $_[0]->{'exon_strand' } },
        VAL_exon_start       => sub{ $_[0]->{'exon_start'  } },
        VAL_exon_end         => sub{ $_[0]->{'exon_end'    } },
        VAL_coding_start     => sub{ $_[0]->{'coding_start'} },
        VAL_coding_end       => sub{ $_[0]->{'coding_end'  } },
        VAL_5utr_start       => sub{ $_[0]->{'5utr_start'  } },
        VAL_5utr_end         => sub{ $_[0]->{'5utr_end'    } },
        VAL_3utr_start       => sub{ $_[0]->{'3utr_start'  } },
        VAL_3utr_end         => sub{ $_[0]->{'3utr_end'    } },
        VAL_5intergenic_start=> sub{ $_[0]->{'5intergenic_start' } },
        VAL_5intergenic_end  => sub{ $_[0]->{'5intergenic_end'   } },
        VAL_5intergenic_gene => sub{ $_[0]->{'5intergenic_gene'  } },
        VAL_3intergenic_start=> sub{ $_[0]->{'3intergenic_start' } },
        VAL_3intergenic_end  => sub{ $_[0]->{'3intergenic_end'   } },
        VAL_3intergenic_gene => sub{ $_[0]->{'3intergenic_gene'  } },
        VAL_intron_start     => sub{ $_[0]->{'intron_start'} },
        VAL_intron_end       => sub{ $_[0]->{'intron_end'  } },
        VAL_codon_table_id   => sub{ 
          # Use 'Invertebrate Mitochondrial' codon table (id=5) for MtDNA
          my $seqname = $_[0]->{'chromosome_name'} || '';
          $seqname =~ /mi?t/i ? 5 : undef },
        VAL_seq_edit         => sub{
          # For selenocysteine. Force the second-to-last codon to 'U'
        },
      },

      #=================================================================
      # Visible.Corresponding_protein

      #--------------------
      # Peptide - skip

      #--------------------
      # Display - skip

      #--------------------
      # DB_info.Database - TODO: Check we are mining all of the IDs
      VAL_protein_uniprot_id => sub{
        my $prot_obj = &cached_at($_[0],'Visible.Corresponding_protein');
        return &names_at($prot_obj,'DB_info.Database.UniProt.UniProtID');
      },
      VAL_protein_uniprot_ac => sub{
        my $prot_obj = &cached_at($_[0],'Visible.Corresponding_protein');
        return &names_at($prot_obj,'DB_info.Database.UniProt.UniProtAcc');
      },
      VAL_protein_treefam_id => sub{
        my $prot_obj = &cached_at($_[0],'Visible.Corresponding_protein');
        return &names_at($prot_obj,'DB_info.Database.TREEFAM.TREEFAM_ID');
      },

      # DB_info.Gene_name - skip
      # DB_info.Description - skip
      # DB_info.Molecular_weight
      VAL_molecular_weight => sub{
        my $prot_obj = &cached_at($_[0],'Visible.Corresponding_protein');
        return &names_at($prot_obj,'DB_info.Molecular_weight');
      },
      COL_molecular_weight => qq| FLOAT(7,1) UNSIGNED DEFAULT NULL |,

      #--------------------
      # Origin - skip. TODO: History?
      VAL_genetic_code => sub{&names_at($_[0],'Origin.Genetic_code')},

      #--------------------
      # Visible - skip. TODO: use Homology_group for KOG?

      #--------------------
      # Homol
      # Homol.DNA_homol - skip

      # Homol.Pep_homol
      #--------------------
      # Contains_peptide
      "TBL_${dataset}__mass_spec_peptide__dm" => {
        OBJECTS      => sub{
          my $cds_obj  = $_[0];
          my $gene_obj = $_[1];
          my $prot_obj = &cached_at($cds_obj,'Visible.Corresponding_protein');
          $prot_obj || return [];
          return[ &at( $prot_obj, 'Contains_peptide' ) ]; 
        },
        "VAL_mass_spec_peptide" => sub{ defined($_[0]) ? $_[0] : undef },
        IDX => ['mass_spec_peptide'],
      },

      #----------
      # This dimension sumarises homology/orthology from the following
      # methods: KOG, InParanoid, blastp (similarity) and 
      # orthologs (best-reciprocal blast)
      "TBL_${dataset}__multispecies__dm" => {
        OBJECTS => sub{
          my $cds_obj  = $_[0];
          my $gene_obj = $_[1];
          my $prot_obj = &cached_at($cds_obj,'Visible.Corresponding_protein');
          $prot_obj || return [];
          # Gene orthology
          #my @rows;
          my %rows_by_protein;
          foreach my $orth_gene( &cached_at($gene_obj, 'Gene_info.Ortholog') ){
            my @orth_cdses = 
                &cached_at( $orth_gene, 'Molecular_info.Corresponding_CDS' );
            my @orth_prots = 
                map{&cached_data_at($_,'Visible.Corresponding_protein') } 
                @orth_cdses;
            foreach my $prot( @orth_prots ){
              $rows_by_protein{$prot->{name}} ||= {%$prot};
              $rows_by_protein{$prot->{name}}->{ortholog} = 1;
            }
          }
####added by RF_011211
	   foreach my $para_gene( &cached_at($gene_obj, 'Gene_info.Paralog') ){
            my @para_cdses = 
                &cached_at( $para_gene, 'Molecular_info.Corresponding_CDS' );
            my @para_prots = 
                map{&cached_data_at($_,'Visible.Corresponding_protein') } 
                @para_cdses;
            foreach my $prot( @para_prots ){
              $rows_by_protein{$prot->{name}} ||= {%$prot};
              $rows_by_protein{$prot->{name}}->{paralog} = 1;
            }
          }
#########################################################
          # KOG, InParanoid;
          foreach my $homol(&cached_at($prot_obj,'Visible.Homology_group')){
            my $type = lc(&names_at($homol, 'Group_type'));
            foreach my $prot( &cached_data_at($homol,'Protein') ){
              if( $prot->{name} eq "$prot_obj" ){next} # Skip 'this' protein
              $rows_by_protein{$prot->{name}} ||= {%$prot};
              $rows_by_protein{$prot->{name}}->{$type} 
                = join( $LIST_SEPARATOR, 
                        $rows_by_protein{$prot->{name}}->{$type} || (),
                        "$homol" );
            }
          }
          # BLASTP; lots of objects so avoid caching
          my %data_by_species;
          foreach my $prot( $prot_obj->at('Homol.Pep_homol') ){
            my $homol_prot = { name    => "$prot" };
            ( $homol_prot->{database}, $homol_prot->{accession} )
              = split( ':', "$prot", 2 );
            if( my $root = $prot_obj->at("Homol.Pep_homol.$prot") ){
              if( my $root6 = $root->right(6) ){
                $homol_prot->{species} = &names_at($root6,"Target_species");

              }
              if( my $evalue = $root->right(2) ){
                $evalue = sprintf("%2.2e", ( 10 ** (0-$evalue) ) );
                $homol_prot->{blastp_evalue} = $evalue;
              }
            }
            $homol_prot->{species} ||= 'unknown';
            push @{$data_by_species{$homol_prot->{species}}}, $homol_prot;
          }

          # BAD - uses too much memory
          #foreach my $homol_prot( &cached_data_at($prot_obj,
          #                                        'Homol.Pep_homol')){
          #  $homol_prot = {%$homol_prot}; # Dereference
          #  my $pname = $homol_prot->{name};
          #  my $sp = $homol_prot->{species}||'unknown';
          #  $data_by_species{$sp} ||= [];
          #  if( my $root = $prot_obj->at("Homol.Pep_homol.$pname") ){
          #    my $evalue = sprintf("%2.2e", ( 10 ** (0-$root->right(2)) ) );
          #    $homol_prot->{blastp_evalue} = $evalue;
          #  }
          #  push @{$data_by_species{$sp}}, $homol_prot;
          #}

          foreach my $sp( keys %data_by_species ){
            # Sort per-species hits by evalue, and add a rank.
            # TODO: deal with identical evalues      
            my $rank = 0;
            $data_by_species{$sp} = 
                [map{$_->{blastp_rank} = ++$rank; $_ }
                 sort{($a->{blastp_evalue}||999)<=>($b->{blastp_evalue}||999)}
                 grep{defined($_->{blastp_evalue})}
                 @{$data_by_species{$sp}} ];
            foreach my $prot( @{$data_by_species{$sp}} ){
              $rows_by_protein{$prot->{name}} ||= $prot;
              $rows_by_protein{$prot->{name}}->{blastp_rank} 
                = $prot->{blastp_rank};
              $rows_by_protein{$prot->{name}}->{blastp_evalue} 
                = $prot->{blastp_evalue};
            }
          }
          return[ map{ $rows_by_protein{$_} } keys(%rows_by_protein) ];

        },
        VAL_protein             => &cached_val_sub('name'),
        VAL_species             => &cached_val_sub('species'),
        VAL_info_description    => &cached_val_sub('description'),
        VAL_info_database       => &cached_val_sub('database'),
        VAL_info_accession      => &cached_val_sub('accession'),
        VAL_cog                 => &cached_val_sub('cog'),
        VAL_inparanoid_group    => &cached_val_sub('inparanoid_group'),
        VAL_blastp_evalue       => &cached_val_sub('blastp_evalue'),
        COL_blastp_evalue       => q|DOUBLE DEFAULT NULL|,
        VAL_blastp_rank         => &cached_val_sub('blastp_rank'),
        COL_blastp_rank         => q|SMALLINT(4) UNSIGNED DEFAULT NULL|,
        VAL_ortholog            => &cached_val_sub('ortholog'),
	VAL_paralog            => &cached_val_sub('paralog'),  ##added by RF_011211

        IDX => [
                'protein',
                'species',
                'blastp_rank',
                'blastp_evalue',
                'ortholog',
		'paralog',  ##added by RF_011211
                ],
      },


      IDX => [
              'cds_name',
              'cgc_name',
              'chromosome_end',
              'chromosome_name',
              'chromosome_start',
              'chromosome_strand',
              'gene_class',
              'identity_status',
              'molecular_weight',
              'ndb_gi',
              'prediction_status',
              'public_name',
              'reference_allele',
              'sequence_name',
              'species',
              'swissprot_ac',
              'swissprot_id',
              'trembl_ac',
              'version',
              'wb_gene_id',
              'wb_wormpep_id',
              ],

    }, # End cds__main

      IDX => [
              #'cds_name',
              'cgc_name',
              'chromosome_end',
              'chromosome_name',
              'chromosome_start',
              'chromosome_strand',
              'gene_class',
              'identity_status',
              #'ndb_gi',
              #'prediction_status',
              'phenotype_count',
              'public_name',
              'reference_allele',
              'sequence_name',
              'species',
              'map_map',
              #'swissprot_ac',
              #'swissprot_id',
              #'trembl_ac',
              'version',
              'wb_gene_id',
              #'wb_wormpep_id',
              ],
  }, # End gene__main
};

#----------------------------------------------------------------------
# A subroutine to generate 'custom' AceDB objects, each representing
# an exon. Each 'object' is simply a hashref containing column-value 
# pairs
my %structure_cache; # For performance
sub process_structure{
  my $aceobj = shift || return ();
  my $gene   = shift || $aceobj;

  # Examine cache;
  if( my $cached = $structure_cache{$aceobj} ){ 
      ##print "geneLoader_L1423_examine_cache\n";
      return @$cached 
  }

  my @exons_data; # Each entry is a record in the table
  my( @transcripts, @cds_exons ); 

####test by RF_010410
  ##my $t_obj_class = $aceobj->class;
  ##print "geneLoader_L1432_t_obj_class=$t_obj_class\n";  ##= CDS
########################################

  if( $aceobj->class eq 'CDS' ){
    # Need coding exons for CDS
    @cds_exons = &_exon_physical_positions( $aceobj );
    @transcripts = $aceobj->at('Visible.Corresponding_transcript');
    if( ! @transcripts ){ 
      # Hack for mitochondirial CDS; no transcripts attached! 
      @transcripts = ( $aceobj );
    } 
  } elsif( $aceobj->class eq 'Gene' ){
    # Non-coding, leave coding exon list empty
    foreach my $loc('Corresponding_transcript','Corresponding_pseudogene'){
####test by RF_010410
	##print "geneLoader_L1442_loc=$loc\n";  ##not print out
#########################################################
      push @transcripts, $aceobj->at("Molecular_info.$loc");
    }
  }

  foreach my $tran( @transcripts ){
    # Process each transcript (coding and non-coding)
    $tran = $tran->fetch;
    my @exons = &_exon_physical_positions( $tran );
    my @data  = &_exonic_structure( [@exons],[@cds_exons] );
    #Add the transcript_name attribute
    # and intron location (to next upstream exon)
    my $tran_name = $tran->name;
    @data = sort{ $a->{'rank'} <=> $b->{'rank'} } @data;
    for( my $i=0; $i<@data; $i++ ){
      my $exon = $data[$i];
      $exon->{'transcript_name'} = $tran_name;
      $exon->{'exon_name'}       = $tran_name . ".exon" . $exon->{'rank'};
      if( my $next_exon = $data[$i+1] ){
        if( $exon->{'exon_strand'} > 0 ){
          # +ori
          $exon->{'intron_start'} = $exon->{'exon_end'} + 1;
          $exon->{'intron_end'}   = $next_exon->{'exon_start'} - 1;
        } else {
          # -ori
          $exon->{'intron_start'} = $next_exon->{'exon_end'} + 1;
          $exon->{'intron_end'}   = $exon->{'exon_start'} - 1;
        }
      }
    }
    push @exons_data, @data;
  }

  # Add the wb gene id attribute
  my $wb_gene_id = "$gene";
  map{ $_->{wb_gene_id}=$wb_gene_id } @exons_data;

  # Add the upstream/downstream intergenic regions
####test by RF_010410
  ##print "L1471_adjacent_data=$adjacent_data\n";
###################################################
  if( my( $adjacent_data ) = &_adjacent_genes( $gene ) ){
####test by RF_010410
     
      my $t_gdata_hashRef = &_adjacent_genes( $gene );
      my %t_gdata_hash = %$t_gdata_hashRef;
      my $intergenic_start_5_v = $t_gdata_hash{'5intergenic_start'};
      ##print "L1477_intergenic_start_5_v=$intergenic_start_5_v\n";  ##yes for remanei not for ppa
###################################################
    # Just slap it on the first exon
    map{$exons_data[0]->{$_} = $adjacent_data->{$_} } keys %$adjacent_data;
  }

  # Update cache
  $structure_cache{$aceobj} = [@exons_data];
  #warn Dumper( @exons_data );

  # All done
  return(@exons_data);
}


#----------------------------------------------------------------------
# Gets all exons off transcript/CDS object, and calculates their physical
# start/end/ori coordinates.
sub _exon_physical_positions{
  my $aceobj = shift || return ();
  my( $seq, $phys_start, $phys_end, $ori ) = &physical_position($aceobj);
  my @exons;
  foreach my $exon( $aceobj->at('Structure.Source_exons') ){
    my $start = ( $ori > 0 ?
                  $phys_start + $exon->name - 1:
                  $phys_end   - $exon->right + 1 );
    my $end   = ( $ori > 0 ?
                  $phys_start + $exon->right - 1:
                  $phys_end   - $exon->name + 1 );
    push @exons, [$start, $end, $ori, $seq];
  }
  @exons = sort{ $a->[0] <=> $b->[0] } @exons;
  return @exons;
}

#----------------------------------------------------------------------
# Takes a listref of transcript exon_physical_positions and a listref of 
# CDS exon_physical_positions, and calculates UTRs etc
#
sub _exonic_structure{
  my @trans_exons = @{shift||[]};
  my @cds_exons   = @{shift||[]};
  my @exon_structure = ();
  my $num_exons = @trans_exons;
  my $is_coding = @cds_exons ? 1 : 0;
  my $i = 0;
  my $exon_ori = $trans_exons[0]->[2]; # Assume same for all exons
  my $sorter = sub{ $exon_ori>0 ? $a->[0]<=>$b->[0] : $b->[0]<=>$a->[0] };
  @trans_exons = sort{ &$sorter() } @trans_exons; # Sort 5-3 prime
  @cds_exons   = sort{ &$sorter() } @cds_exons;   # Sort 5-3 prime
  my $seen_coding = 0; # Moving from 5-3, have we passed coding region?

  foreach my $t_exon( @trans_exons ){
    my $exon_start = $t_exon->[0];
    my $exon_end   = $t_exon->[1];
    my %row;
    $row{'rank'}        = ++$i;
    $row{'exon_strand'} = $exon_ori;
    $row{'exon_start'}  = $exon_start;
    $row{'exon_end'}    = $exon_end;
    $row{'chromosome_name'} = $t_exon->[3];

    if( $is_coding ){ # Worry about UTRs
      # Get next coding exon in list
      my $code_exon = shift @cds_exons || [0,0];
      my $code_start = $code_exon->[0];
      my $code_end   = $code_exon->[1];
      if( $exon_start > ( $code_end   ) or
          $exon_end   < ( $code_start ) ){
        # No overlap, put coding exon back on list
        unshift @cds_exons, $code_exon;
        ( $code_start, $code_end ) = ( 0, 0 );
      }

      if( $code_start ){
        $seen_coding++;
        $row{coding_start} = $code_start;
        if( $code_start > $exon_start ){
          # Fill gap at start of exon with UTR
          my $dir = $exon_ori>0 ? 5 : 3;
          $row{$dir.'utr_start'} = $exon_start;
          $row{$dir.'utr_end'  } = $code_start-1;
        }
      }
      if( $code_end ){
        $row{coding_end}   = $code_end;
        if( $code_end < $exon_end ){
          # Fill gap at end of exon with UTR
          my $dir = $exon_ori>0 ? 3 : 5;
          $row{$dir.'utr_start'} = $code_end+1;
          $row{$dir.'utr_end'  } = $exon_end;          
        }
      }
      unless( $code_start || $code_end ){
        # UTR only
        my $dir = $seen_coding ? 5 : 3;
        $row{$dir.'utr_start'} = $exon_start;
        $row{$dir.'utr_start'} = $exon_end;
      }
    }
    #warn Dumper(\%row);
    push @exon_structure, {%row};
  }
  return @exon_structure;
}

#======================================================================
# Returns the name and physical location of the adjacent
# upstream and downstream genes.
# Coding genes only
my %GENE_RANK_BY_CHROMOSOME;
my %ORDERED_GENES_BY_CHROMOSOME;
sub _adjacent_genes{
  my $gene = $_[0]; # This could be a CDS for briggsae genes

  # Get the CDSes
  my @cdses;
  my $gene_seq_name;

####test by RF_010410
  ##my $t_class=$gene->class;
  ##print "geneLoader_L1616_t_class=$t_class\n";  ##=Gene
########################
  unless( $gene->class eq 'CDS' ){
      ####test by RF_010410

      ##print "geneLoader_L1621\n";  ##yes for ppa
########################
    $gene_seq_name = &at($gene,'Identity.Name.Sequence_name');
    @cdses = &at( $gene, 'Molecular_info.Corresponding_CDS');
  }
  else{ # Briggsae CDS
####test by RF_010410

      ##print "geneLoader_L1625\n";  ##no for ppa
########################
    $gene_seq_name = $gene->name;
    @cdses = ($gene);
  }
  $gene_seq_name || return;
  scalar( @cdses ) || return (); # No CDS, non-coding?
  
  # Get top-level sequence;
  my( $top_seq_name, $gstart, $gend, $gstrand ) 
      = &physical_position($gene);

  $top_seq_name || ( warn( "[WARN] Gene $gene has no physical_position!" ) 
                     && return () );
 
   # Build cache if needed
  $GENE_RANK_BY_CHROMOSOME{$top_seq_name} 
    || &_populate_gene_order($top_seq_name);

####test by RF_010410
   ##my $test = $GENE_RANK_BY_CHROMOSOME{$top_seq_name};
  ##print "L1650=test=$test\n";  
##################################################

  my $gene_rank = $GENE_RANK_BY_CHROMOSOME{$top_seq_name}->{$gene_seq_name};
  my $gene_loc = $GENE_RANK_BY_CHROMOSOME{$top_seq_name}->{$gene_seq_name}
    || ( warn( "Can't find location for $gene_seq_name on $top_seq_name") 
         and return );
  my $rank = $gene_loc->{rank};

  # Get the adjacent gene regardless of strand
  my $lhs_any  = $ORDERED_GENES_BY_CHROMOSOME{$top_seq_name}->[$rank-1];
  my $rhs_any = $ORDERED_GENES_BY_CHROMOSOME{$top_seq_name}->[$rank+1];

  # Get the adjacent gene on the same strand
  my( $lhs_same, $rhs_same );
  my $lhs_rank = $rank-1;
  while( $lhs_rank ){
    my $data = $ORDERED_GENES_BY_CHROMOSOME{$top_seq_name}->[$lhs_rank];
    my $tstrand = $data->{strand};
    if( $tstrand == $gstrand # Same strand
        or ! $tstrand        # No strand info. At end of top_level seq
        ){ $lhs_same = $data; last }
    $lhs_rank --;
  }
  my $rhs_rank = $rank+1;
  while( $rhs_rank ){
    my $data = $ORDERED_GENES_BY_CHROMOSOME{$top_seq_name}->[$rhs_rank];
    my $tstrand = $data->{strand} || 0;
    if( $tstrand == $gstrand # Same strand
        or ! $tstrand        # No strand info. At end of top_level seq
        ){ $rhs_same = $data; last }
    $rhs_rank ++;
  }
 
  my $gdata;
  if( $gstrand eq 1 ){
####test by RF_010410
      ##my $t_5_s = $lhs_same->{end};
      ##my $t_5_e = $gene_loc->{start};
      ##print "L1655_t_5_s=$t_5_s==t_5_e=$t_5_e\n";
#####################################
    $gdata = {
      '5intergenic_gene'  => $lhs_same->{gene_seq_name},
      '5intergenic_start' => $lhs_same->{end}+1,
      '5intergenic_end'   => $gene_loc->{start}-1,
      '3intergenic_gene'  => $rhs_same->{gene_seq_name},
      '3intergenic_start' => $gene_loc->{end}+1,
      '3intergenic_end'   => $rhs_same->{start}-1,
    };
  }
  else{
####test by RF_010410
      ##my $t_5_e = $rhs_same->{start};
      ##my $t_5_s = $gene_loc->{end};
      ##print "L1670_t_5_s=$t_5_s==t_5_e=$t_5_e\n";
#####################################
    $gdata = {
      '5intergenic_gene'  => $rhs_same->{gene_seq_name},
      '5intergenic_start' => $gene_loc->{end}+1,
      '5intergenic_end'   => $rhs_same->{start}-1,
      '3intergenic_gene'  => $lhs_same->{gene_seq_name},
      '3intergenic_start' => $lhs_same->{end}+1,
      '3intergenic_end'   => $gene_loc->{start}-1,
    };
  }

  return( $gdata );
}

sub _populate_gene_order{
  my $seq_name    = shift || die( "Need a sequence name! ",
                                  "Called by ", 
                                  join( ', ', (caller(0))[1..2]) );

  my $seq_obj = &object_cache_query( 'Sequence',$seq_name );

####test by RF_010410
  ##print "geneLoader_L1727_seq_obj=$seq_obj\n";
#############################

  $seq_obj || die( "Sequence $seq_name not in cache!" );
  my( $parent, $tstart, $tend, $tstrand ) = &physical_position( $seq_obj );

#   if( $tstrand != 1 ){ die( 'TODO: Deal with reverse strand subseqs' ) } 

  # This is dealing with reverse strand subseqs
  if( $tstrand !=1 ){
    ($tstart, $tend ) = ($tend, $tstart );
  }
  my $offset = $tstart - 1;
  
  # Process CDSes on this seq
  my %gene_pos;
  
  if( my $root = $seq_obj->at('SMap.S_child.CDS_child') ){
    $root = $root->right;
    while( $root ){
      my $gene_name = "$root";

####test by RF_010410
      ##print "geneLoader_L1750_gene_name=$gene_name\n";  ##for Ppa,gene_name=PPA29519
#####################################

      my $start = $root->right;
      my $end   = $root->right(2);
      if( ( $start and $end ) and
          ( $gene_name !~ /\.tw$/ ) and # Skip twinscan genes
          ( $gene_name =~ /(^\w+\.\d+)(.*)/ or
            $gene_name =~ /(MTCE\.\d+)/    or # Mitochondrial gene
            $gene_name =~ /(C[A-Z]{2}\d+)/ or # Briggsae (CBG) Remanii (CRE), sequence name
	    $gene_name =~ /(PPA\d+)/ or   ##$gene_name =~ /(PPA\d+)/ or #Ppa, added by RF_010410
	    $gene_name =~ /(Ppa{0,1}-[a-z]{3,4}-[\d\.A-Z]+)/ or  ##Ppa added by RF_010410
	    $gene_name =~ /(CBN\d+)/ or   ###Cbre, added by RF_010410
	    $gene_name =~ /(Cbn-[a-z]{3,4}-[\d\.A-Z]+)/ or  ##Cbre added by RF_010410
	    $gene_name =~ /(CJA\d+)/ or  ##Cjaponica added by RF_020710
	    $gene_name =~ /(CJA\d+)/ or  ##Cjaponica added by RF_020710
            $gene_name =~ /(.+wum.+)/ )  # Remanei gene
          ){  
##	  print "GeneLoader_L1779_gene_name=$gene_name\n";
        my $strand = 1;
        if( $start > $end ){ 
          ($start, $end ) = ( $end, $start );
          $strand = -1;
        }
        $start += $offset;
        $end   += $offset;
        $gene_name = $1 || $gene_name; 
        my $suffix = $2;
        $gene_pos{$gene_name}->{gene_seq_name} = $gene_name;
        $gene_pos{$gene_name}->{strand} = $strand;
        if( ! $gene_pos{$gene_name}->{start} or 
            $start < $gene_pos{$gene_name}->{start} ){
          $gene_pos{$gene_name}->{start} = $start;
        }
        if( ! $gene_pos{$gene_name}->{end} or 
            $end > $gene_pos{$gene_name}->{end} ){
          $gene_pos{$gene_name}->{end} = $end;
        }
      }
      $GENE_RANK_BY_CHROMOSOME{$parent} ||= {};
      map{ $GENE_RANK_BY_CHROMOSOME{$parent}{$_} = $gene_pos{$_} }
      keys %gene_pos;
      
      $root = $root->down;
    }
  }
  
  # Loop through each child
  foreach my $subseq( &cached_at($seq_obj,'Structure.Subsequence')){
    # Recurse
    &_populate_gene_order("$subseq");
  }
  
  # If this is the top level seq, then assign rank to the genes, and 
  # create an ordered list
  if( $seq_name eq $parent ){
    
    # Push 'dummy' start/end genes onto list
    $GENE_RANK_BY_CHROMOSOME{$parent}->{FIRST} = { start=>1, end=>1 };
    $GENE_RANK_BY_CHROMOSOME{$parent}->{LAST} = { start=>$tend, end=>$tend };

    # Sort the list
    my $rank = 0;
    my $gene_ref = $GENE_RANK_BY_CHROMOSOME{$parent};
    map{ $gene_ref->{$_}->{rank} = $rank++;
         push @{$ORDERED_GENES_BY_CHROMOSOME{$parent}}, $gene_ref->{$_} }
    sort{ $gene_ref->{$a}->{start} <=> $gene_ref->{$b}->{start} }
    keys %{$gene_ref};
  }
}

#----------------------------------------------------------------------
#
# Takes a gene object and returns arrayrefs of the observed phenotypes
sub _phenotypes_by_gene{
  my $gene = shift;
  my  %phenotypes;

  # Loop for each RNAi
  foreach my $rnai( &cached_data_at
                    ( $gene, 'Experimental_info.RNAi_result' ) ){
    # Skip 'non-specific' RNAis
    unless( scalar(@{$rnai->{inhibits_gene}}) == 1 ){
      next;
    }
    # Skip 'secondary' RNAis
    unless( grep{$_->{name} eq $gene } @{$rnai->{inhibits_gene_primary} } ){
      next;
    }
    # Process each phenotype for this RNAi
    for( my $i=0; $i<@{$rnai->{phenotype}}; $i++ ){
      my $phen_data = $rnai->{phenotype}->[$i];
      my $phen = $phen_data->{name};
      # Skip 'not' phenotypes
      if( $phen_data->{PhenotypeInfo_Not} ){ next }
      # Add to phenotype list
      $phenotypes{$phen}->{name} = $phen;
      $phenotypes{$phen}->{primary_name} = $phen_data->{primary_name};
      $phenotypes{$phen}->{short_name} = $phen_data->{short_name};
      $phenotypes{$phen}->{phenotype_label} = $phen_data->{label};
      $phenotypes{$phen}->{phenotype_info} = $phen_data->{info};
      $phenotypes{$phen}->{rnai_count} ++;
      $phenotypes{$phen}->{rnai} ||= [];
      push @{$phenotypes{$phen}->{rnai}}, $rnai->{name};
    }
  }
  # Loop for each Variation of this gene
  foreach my $vari( &cached_data_at
                         ( $gene, 'Gene_info.Allele' ) ){
    # Skip 'non-specific' Variations
    unless( scalar(@{$vari->{gene}}) == 1 ){
      next;
    }
    # Process each phenotype for this Variation
    for( my $i=0; $i<@{$vari->{phenotype}}; $i++ ){
      my $phen_data = $vari->{phenotype}->[$i];
      my $phen = $phen_data->{name};
      # Skip 'not' phenotypes
      if( $phen_data->{PhenotypeInfo_Not} ){ next }
      # Add to phenotype list
      $phenotypes{$phen}->{name} = $phen;
      $phenotypes{$phen}->{primary_name} = $phen_data->{primary_name};
      $phenotypes{$phen}->{short_name} = $phen_data->{short_name};
      $phenotypes{$phen}->{phenotype_label} = $phen_data->{label};
      $phenotypes{$phen}->{phenotype_info} = $phen_data->{info};
      $phenotypes{$phen}->{variation_count} ++;
      $phenotypes{$phen}->{variation} ||= [];
      push @{$phenotypes{$phen}->{variation}}, $vari->{name};
    }
  }
  return ( sort{$a->{name} cmp $b->{name} } values( %phenotypes ) );
}

#----------------------------------------------------------------------
#
# Takes a geneID and phenotypeID and returns arrayrefs of the observed,
# primary and specific RNAi experiments
our %PHENOTYPES_BY_GENE = ();
sub _rnai_phenotypes_by_gene{
  my $gene = shift;

  if( $PHENOTYPES_BY_GENE{$gene} ){ # Look for cached
    return @{$PHENOTYPES_BY_GENE{$gene}};
  }

  my $gene_name = &names_at($gene,'Identity.Name.Public_name');
  my %phenotypes;
  my %not_phenotypes;
  foreach my $rnai( &cached_data_at
                    ( $gene, 'Experimental_info.RNAi_result' ) ){

    # Process each phenotype for this RNAi
    for( my $i=0; $i<@{$rnai->{phenotype}}; $i++ ){
      my $phen_data = $rnai->{phenotype}->[$i];
      my $phen = $phen_data->{name};

      unless( exists( $phenotypes{$phen} ) ){
        $phenotypes{$phen} = 
        { name              => $phen,
          primary_name      => $phen_data->{primary_name},
          phenotype_label   => $phen_data->{label},
          info              => $phen_data->{info},
          short_name        => $phen_data->{short_name},
          rnais             => [],
          observed_rnais    => [],
          unobserved_rnais  => [],
          primary_rnais     => [],
          secondary_rnais   => [],
          specific_rnais    => [],
          nonspecific_rnais => [],
        };
      }
      push @{$phenotypes{$phen}->{rnais}}, $rnai;

      # Is this a 'not' phenotype
      if( $phen_data->{PhenotypeInfo_Not} ){
        push @{$phenotypes{$phen}->{unobserved_rnais}}, $rnai;
      }
      else{
        push @{$phenotypes{$phen}->{observed_rnais}}, $rnai;
      }

      # Is this a 'primary' RNAi
      if( grep{$_->{name} eq $gene } @{$rnai->{inhibits_gene_primary} } ){
        push @{$phenotypes{$phen}->{primary_rnais}}, $rnai;
      }
      else{
        push @{$phenotypes{$phen}->{secondary_rnais}}, $rnai;
      }
      if( scalar(@{$rnai->{inhibits_gene}}) == 1 ){
        push @{$phenotypes{$phen}->{specific_rnais}}, $rnai;
      }
      else{
        push @{$phenotypes{$phen}->{nonspecific_rnais}}, $rnai;
      }
    }
  }
  # Add some Gene<->phenotype specific info
  foreach my $phen( keys %phenotypes ){
    $phenotypes{$phen}->{phenotype_info}  = $phenotypes{$phen}->{info}
      . sprintf( ': experiments=%s,primary=%s,specific=%s,observed=%s', 
                 scalar(@{$phenotypes{$phen}->{rnais}}),
                 scalar(@{$phenotypes{$phen}->{primary_rnais}}), 
                 scalar(@{$phenotypes{$phen}->{specific_rnais}}), 
                 scalar(@{$phenotypes{$phen}->{observed_rnais}}), );

    $phenotypes{$phen}->{rnai_count} = 
        scalar( @{$phenotypes{$phen}->{rnais}} );
    $phenotypes{$phen}->{rnai_observed_count} = 
        scalar( @{$phenotypes{$phen}->{observed_rnais}} );
    $phenotypes{$phen}->{rnai_unobserved_count} =
        scalar( @{$phenotypes{$phen}->{unobserved_rnais}} );
    $phenotypes{$phen}->{rnai_primary_count} = 
        scalar( @{$phenotypes{$phen}->{primary_rnais}} );
    $phenotypes{$phen}->{rnai_secondary_count} = 
        scalar( @{$phenotypes{$phen}->{secondary_rnais}} );
    $phenotypes{$phen}->{rnai_specific_count} = 
        scalar( @{$phenotypes{$phen}->{specific_rnais}} );
    $phenotypes{$phen}->{rnai_nonspecific_count} = 
        scalar( @{$phenotypes{$phen}->{nonspecific_rnais}} );

    $phenotypes{$phen}->{rnai} = 
        [map{$_->{name}} @{$phenotypes{$phen}->{rnais}}];
    $phenotypes{$phen}->{rnai_observed} = 
        [map{$_->{name}} @{$phenotypes{$phen}->{observed_rnais}}];
    $phenotypes{$phen}->{rnai_unobserved} =
        [map{$_->{name}} @{$phenotypes{$phen}->{unobserved_rnais}}];
    $phenotypes{$phen}->{rnai_primary} = 
        [map{$_->{name}} @{$phenotypes{$phen}->{primary_rnais}}];
    $phenotypes{$phen}->{rnai_secondary} = 
        [map{$_->{name}} @{$phenotypes{$phen}->{secondary_rnais}}];
    $phenotypes{$phen}->{rnai_specific} = 
        [map{$_->{name}} @{$phenotypes{$phen}->{specific_rnais}}];
    $phenotypes{$phen}->{rnai_nonspecific} = 
        [map{$_->{name}} @{$phenotypes{$phen}->{nonspecific_rnais}}];

    $phenotypes{$phen}->{rnai_info} =
        [map{$_->{info}} @{$phenotypes{$phen}->{rnais}}];
    $phenotypes{$phen}->{rnai_observed_info} = 
        [map{$_->{info}} @{$phenotypes{$phen}->{observed_rnais}}];
    $phenotypes{$phen}->{rnai_unobserved_info} = 
        [map{$_->{info}} @{$phenotypes{$phen}->{unobserved_rnais}}];
    $phenotypes{$phen}->{rnai_primary_info} = 
        [map{$_->{info}} @{$phenotypes{$phen}->{primary_rnais}}];
    $phenotypes{$phen}->{rnai_secondary_info} = 
        [map{$_->{info}} @{$phenotypes{$phen}->{secondary_rnais}}];
    $phenotypes{$phen}->{rnai_specific_info} = 
        [map{$_->{info}} @{$phenotypes{$phen}->{specific_rnais}}];
    $phenotypes{$phen}->{rnai_nonspecific_info} = 
        [map{$_->{info}} @{$phenotypes{$phen}->{nonspecific_rnais}}];

  }

  # Update the cache and return
  $PHENOTYPES_BY_GENE{$gene} = [ sort{$a->{name} cmp $b->{name} } 
                                 values( %phenotypes ) ];
  return @{$PHENOTYPES_BY_GENE{$gene}};
}

#----------------------------------------------------------------------
#
# Takes a geneID and anatomyTermID and returns arrayrefs of the
# expression patterns

our %ANATOMY_BY_GENE = ();
sub _anatomy_by_gene{
  # TODO: Implement this
}


#----------------------------------------------------------------------
# A gene-specific method that adds the evidence codes to            
# the object_data_cache for the go terms.
# GO_terms with multiple evidences will be duplicated
sub _go_data{
  my $gene = shift;
  # Get a list of GO_terms 
  my @term_info = &cached_data_at($gene,'Gene_info.GO_term');
  my @new_term_info = ();
  foreach my $term( @term_info ){
    # Get the evidence codes for each term
    my @evidences = &at($gene,'Gene_info.GO_term.'.$term->{name} );
    unless( scalar( @evidences ) ){ @evidences = ('') } # just in case ;)
    foreach my $ev( @evidences ){
      my $paper_info;
      # Get any paper used to assign GO_term, else see if auto
      if( $ev and
          my ($paper) = (&at($gene,'Gene_info.GO_term.'
                             . $term->{name}
                             . ".$ev.Paper_evidence"))){
        $paper_info = &object_data_cache( $paper );              
      } 
      
      my %term_copy = %{$term}; # Dereference; adding gene-specific info
      $term_copy{evidence} = "$ev";
      $term_copy{paper} = $paper_info->{name} || undef;
      $term_copy{paper_info} = $paper_info->{info} || undef;
      $term_copy{info} .= sprintf( ' (%s) %s', $ev,
                                   $paper_info->{inline_name} 
                                   ? "via $paper_info->{inline_name}" : '');
      push @new_term_info, {%term_copy}; # Add to new list
    }
  }
  return @new_term_info;
}

#---
1;
