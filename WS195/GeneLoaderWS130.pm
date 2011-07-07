package GeneLoader;

use strict;
use warnings;
use Data::Dumper qw( Dumper );

use vars qw( @ISA @EXPORT_OK $CONNECTION $CONFIG );

@ISA = qw( Exporter );

@EXPORT_OK = qw( $CONNECTION $CONFIG );            

$CONNECTION->{ACE_HOST}    = undef();
$CONNECTION->{ACE_PORT}    = undef();
$CONNECTION->{ACE_USER}    = undef();
$CONNECTION->{ACE_PASS}    = undef();

$CONNECTION->{MART_HOST}   = undef();
$CONNECTION->{MART_PORT}   = undef();
$CONNECTION->{MART_USER}   = undef();
$CONNECTION->{MART_PASS}   = undef();
$CONNECTION->{MART_DBNAME} = 'wormmart_130';


my $dataset    = 'gene';

#my $gene_query = '*'; # All 
#my $gene_query = 'WBGene0000*';    # 1-9999 for debug
#my $gene_query = 'WBGene00000*';   # 1-999 for debug
my $gene_query = 'WBGene000000*';  # 1-99 for debug
#my $gene_query = 'WBGene0000000*'; # 1-9  for debug
#my $gene_query = 'WBGene00000013'; # Single gene for debug
#my $gene_query = 'WBGene00000812'; # + ori, *1 CDS, *3 trans, UTR-only exons 
#my $gene_query = 'WBGene00017929'; # - ori, many transc. Has UTR-only exons
#my $gene_query = 'WBGene00000829'; # Has CDS with no transcr (mitochondrial)
#my $gene_query = 'WBGene00001574'; # Has mixed coding/non-coding transcr
#my $gene_query = 'WBGene00000307'; # Coding Briggsae gene

$CONFIG = {
  "TBL_${dataset}__gene__main" => {

    OBJECTS => sub{
      my $ace_handle = shift;
      $ace_handle->fetch('Gene'=>$gene_query);
    },
    
    COL_wb_gene_id  => qq(varchar(14) NOT NULL),
    VAL_wb_gene_id      => sub{ &name( $_[0], 'Gene' ) },
    
    COL_species     => qq(varchar(24) NOT NULL),
    VAL_species         => sub{ &at($_[0],'Identity.Species') },
    
    COL_concise_description => qq(text default NULL),
    VAL_concise_description => sub{&at($_[0],
                                       'Gene_info.Concise_description')},
    
    COL_sequence_name    => qq| varchar(64) default NULL |,
    VAL_sequence_name    => sub{ &at($_[0],'Identity.Name.Sequence_name') },
    
    COL_reference_allele => qq| varchar(64) default NULL |,
    VAL_reference_allele => sub{ &at($_[0],'Gene_info.Reference_Allele') },
    
    COL_phenotype        => qq| text        default NULL |,
    VAL_phenotype        => sub{ &at($_[0],'Gene_info.Phenotype') },
    
    COL_remark           => qq| text        default NULL |,
    VAL_remark           => sub{ &at($_[0],'Remark') },
    
    COL_public_name      => qq| varchar(64) default NULL |,
    VAL_public_name      => sub{ &at($_[0],'Identity.Name.Public_name' ) },
    
    COL_cgc_name         => qq| varchar(64) default NULL |,
    VAL_cgc_name         => sub{ &at($_[0],'Identity.Name.CGC_name' ) },
    
    COL_chromosome_name  => qq(varchar(64) default NULL),
    COL_chromosome_start => qq(int(10) unsigned default NULL),
    COL_chromosome_end   => qq(int(10) unsigned default NULL),
    COL_chromosome_strand=> qq(tinyint(2) default NULL),
    VAL_chromosome_name => sub{
      # Strip any leading 'CHROMOSOME_' prefix from name
      my $name = (&physical_position($_[0]))[0] || return;
      $name =~ s/^CHROMOSOME_//;
      return $name },
    VAL_chromosome_start     => sub{ (&physical_position($_[0]))[1] },
    VAL_chromosome_end       => sub{ (&physical_position($_[0]))[2] },
    VAL_chromosome_strand    => sub{ (&physical_position($_[0]))[3] },

    COL_empty           => qq| int(1)  unsigned default NULL |,
    VAL_empty           => sub{ }, # Hack for sequence dumping -  
                                   # must be NULL!
    
    "TBL_${dataset}__reference__dm" => {
      OBJECTS            => sub{ &at($_[0],'Reference') },
      COL_reference      => qq| varchar(64) default NULL |,
      VAL_reference      => sub{ &name($_[0], 'Paper') },
      COL_brief_citation => qq| text default NULL |,
      VAL_brief_citation => sub{ &at($_[0], 'Brief_citation') },        
    },
    
    "TBL_${dataset}__other_name__dm" => {
      OBJECTS         => sub{
        # Uniquify list
        my @names = ( &at($_[0],'Identity.Name.Public_name' ),
                      &at($_[0],'Identity.Name.Other_name' ) );
        my %names = map{ ( "$_" => $_ ) } @names;
        return values %names;
      },
      COL_other_name  => qq| varchar(16) default NULL |,
      VAL_other_name  => sub{ &name( $_[0], 'Gene_name' ) },
    },

    "TBL_${dataset}__ortholog__dm" => {
      OBJECTS         => sub{ &at($_[0],'Gene_info.Ortholog') },
      COL_wb_gene_id  => qq(varchar(14) default NULL),
      VAL_wb_gene_id      => sub{ &name( $_[0], 'Gene' ) },
      COL_species     => qq(varchar(24) default NULL), 
      VAL_species         => sub{ &at($_[0],'Identity.Species') },
    },
    
    "TBL_${dataset}__allele__dm" => {
      OBJECTS         => sub{ &at($_[0],'Gene_info.Allele') },
      COL_allele      => qq| varchar(64) default NULL |,
      VAL_allele      => sub{ &name( $_[0], 'Allele' ) },
    },
    
    "TBL_${dataset}__rnai__dm" => {
      # Uses a subroutine to generate 'custom' objects, each representing     
      # an RNAi phenotype. Each 'object' is simply a hashref containing 
      # column-value pairs                         
      #OBJECTS         => sub{ &at($_[0],'Experimental_info.RNAi_result') },
      OBJECTS => sub{ &process_rnai(@_) },
      COL_rnai                  => qq| varchar(16) default NULL |,
      COL_phenotype             => qq| varchar(16) default NULL |,
      COL_phenotype_description => qq| varchar(128) default NULL |,
      COL_author                => qq| varchar(128) default NULL |,
      VAL_rnai                  => sub{ $_[0]->{'rnai'} },
      VAL_phenotype             => sub{ $_[0]->{'phenotype'} },
      VAL_phenotype_description => sub{ $_[0]->{'phenotype_description'} },
      VAL_author                => sub{ $_[0]->{'author'} },
    },
    
    "TBL_${dataset}__expr_pattern__dm" => {
      OBJECTS => sub{ &at($_[0],'Experimental_info.Expr_pattern') },
      COL_expr_pattern   => qq| varchar(64) default NULL |,
      VAL_expr_pattern   => sub{ &name($_[0],'Expr_pattern') },
      COL_subellular_loc => qq| text default NULL |,
      VAL_subellular_loc => sub{ &at($_[0],'Subcellular_localization') }, 
    },
    
    "TBL_${dataset}__antibody__dm" => { 
      OBJECTS => sub{ &at($_[0],'Experimental_info.Antibody')},
      COL_antibody => qq| varchar(128) default NULL |,
      VAL_antibody => sub{ &name( $_[0], 'Antibody' ) },
      COL_summary  => qq| text default NULL |,
      VAL_summary  => sub{ &at($_[0],'Subcellular_localization') },
    },
    
    "TBL_${dataset}__strain__dm"   => {
      OBJECTS => sub{ &at($_[0],'Gene_info.Strain') },
      COL_strain   => qq| varchar(128) default NULL |,
      VAL_strain   => sub{ &name( $_[0], 'Strain' ) },
      COL_genotype => qq| text default NULL |,
      VAL_genotype => sub{ &at($_[0],'Genotype') },
    },
    
    "TBL_${dataset}__transgene_drives__dm" => {
      OBJECTS => sub{&at($_[0],'Experimental_info.Drives_Transgene')},
      COL_transgene   => qq| varchar(64) default NULL |,
      VAL_transgene   => sub{ &name( $_[0], 'Transgene' ) },
    },
    
    "TBL_${dataset}__transgene_product__dm" => {
      OBJECTS => sub{&at($_[0],'Experimental_info.Transgene_product')},
      COL_transgene   => qq| varchar(64) default NULL |,
      VAL_transgene   => sub{ &name( $_[0], 'Transgene' ) },
    },
    
    "TBL_${dataset}__transgene_rescued__dm" => {
      OBJECTS => sub{&at($_[0],'Experimental_info.Rescued_by_Transgene')},
      COL_transgene   => qq| varchar(64) default NULL |,
      VAL_transgene   => sub{ &name( $_[0], 'Transgene' ) },
    },
    
    "TBL_${dataset}__cds__main" => {
      OBJECTS      => sub{ 
        # Allow for mixed coding/non-coding transcripts by returning the 
        # Gene if it has Corresponding_transcript.
        my $gene = $_[0];
        my @cds = &at($gene,'Molecular_info.Corresponding_CDS');
        if( &at( $gene,'Molecular_info.Corresponding_transcript' ) ){
          push @cds, $gene;
        }
        return( @cds );
      },
      COL_cds_name      => qq(varchar(64) default NULL),
      VAL_cds_name      => sub{&name($_[0],'CDS')},
      COL_wb_wormpep_id => qq(varchar(64) default NULL),
      VAL_wb_wormpep_id => sub{&at($_[0],'Visible.Corresponding_protein')}, 
      COL_swissprot_id  => qq(varchar(64) default NULL),
      VAL_swissprot_id  => sub{&at($_[0],
                                   'DB_info.Database.SwissProt.SwissProt_ID')},
      COL_swissprot_ac  => qq(varchar(64) default NULL),
      VAL_swissprot_ac  => sub{&at($_[0],
                                   'DB_info.Database.SwissProt.SwissProt_AC')},
      COL_ndb_gi        => qq(varchar(64) default NULL),
      VAL_ndb_gi        => sub{&at($_[0],
                                   'DB_info.Database.NDB.GI_number')},
      
      COL_brief_ident   => qq(text default NULL),
      VAL_brief_ident   => sub{&at($_[0],
                                   'Visible.Brief_identification')},
      
      COL_db_remark     => qq(text default NULL),
      VAL_db_remark     => sub{&at($_[0],'DB_info.DB_remark')},
      
      COL_prediction_status => qq| varchar(64) default NULL |,
      VAL_prediction_status => sub{&at($_[0],
                                       'Properties.Coding.Prediction_status')},
      
      "TBL_${dataset}__kog__dm" => {
        OBJECTS => sub{ # Get from peptide. Need to filter Homology_group
          my($pep) = &at($_[0],'Visible.Corresponding_protein');
          $pep ? $pep = $pep->fetch : return;
          my @homols = &at($pep,'Visible.Homology_group');
          @homols = map{ $_->fetch } @homols;
          return( grep{ &at($_,'Group_type') eq 'COG' } @homols );
        },
        COL_kog   => qq| varchar(64) default NULL |,
        VAL_kog   => sub{ &name($_[0],'Homology_group') },
        COL_title => qq| text default NULL |,
        VAL_title => sub{ &at($_[0],'Title') },
      },
      
      "TBL_${dataset}__motif__dm" => {
        OBJECTS => sub{ # Get from peptide
          my($pep) = &at($_[0],'Visible.Corresponding_protein');
          $pep ? $pep = $pep->fetch : return;
          return( &at( $pep, 'Homol.Motif_homol' ) ) },
        COL_motif => qq| varchar(64) default NULL |,
        VAL_motif => sub{ &name($_[0],'Motif') },
        COL_title => qq| text default NULL |,
        VAL_title => sub{ &at($_[0],'Title') },          
      },
      
      "TBL_${dataset}__transcript__dm" => {
        OBJECTS =>sub{ 
          # For coding transcripts, transcript is on CDS,
          # For non-coding, transcript is on Gene
          my @trans = $_[0]->class eq 'CDS' ?
              &at($_[0],'Visible.Corresponding_transcript') :
              &at($_[0],'Molecular_info.Corresponding_transcript');
          return @trans;
        },
        COL_transcript    => qq| varchar(64) default NULL |,
        VAL_transcript    => sub{ &name( $_[0], 'Transcript') },
        COL_coding_status => qq| varchar(10) default NULL |,
        VAL_coding_status => sub{ 
          # If an exon has a coding_start then this is a coding gene 
          map{ $_->{coding_start} && return 'coding' }
          grep{$_->{transcript_name} eq $_[0]} 
          &process_structure(@_[1..2]);
          return 'non-coding';
        },
        COL_utr_status    => qq| varchar(10) default NULL |,
        VAL_utr_status    => sub{
          my %utr;
          map{ if( $_->{'5utr_start'} ){ $utr{5} = 'utr5' }
               if( $_->{'3utr_start'} ){ $utr{3} = 'utr3' } }
          grep{$_->{'transcript_name'} eq $_[0]}
          &process_structure(@_[1..2]);
          my $str = join( "+", ($utr{5}||()),($utr{3}||()) );
          return $str || 'neither';
        }

     },
      "TBL_${dataset}__go_term__dm"    => { # Evidence codes come from the CDS
        OBJECTS        => sub{ &at($_[0],'Visible.GO_term') },
        COL_go_term        => qq| varchar(64) default NULL |,
        COL_definition     => qq| text default NULL |,
        COL_evidence_code  => qq| enum('IC' ,'IDA','IEA',
                                       'IEP','IGI','IMP',
                                       'IPI','ISS','NAS',
                                       'ND' ,'TAS','NR') default NULL |,
        VAL_go_term        => sub{ &name( $_[0], 'GO_term' ) },
        VAL_definition     => sub{ &at($_[0], 'Definition' ) },
        VAL_evidence_code  => sub{ 
          $_[0]->class eq 'GO_term' ? 
              &at($_[1],'Visible.GO_term.'.$_[0]->name) : ''; 
        }, 
      },
      "TBL_${dataset}__oligo_set__dm" => {
        OBJECTS => sub{&at($_[0],'Visible.Corresponding_oligo_set')},
        COL_oligo_set => qq| varchar(64) default NULL |,
        VAL_oligo_set => sub{ &name( $_[0], 'Oligo_set' ) },
        COL_remark    => qq| text default NULL |,
        VAL_remark    => sub{ &at($_[0],'Remark') },
      },
      
      "TBL_${dataset}__structure__dm" =>{
        # Uses a subroutine to generate 'custom' objects, each representing
        # an exon. Each 'object' is simply a hashref containing column-value 
        # pairs 
        OBJECTS             => sub{ &process_structure(@_) },
        COL_wb_gene_id      => qq| varchar(64)      NOT     NULL |,
        COL_transcript_name => qq| varchar(64)      default NULL |,
        COL_chromosome_name => qq| varchar(64)      default NULL |,
        COL_exon_name       => qq| varchar(64)      default NULL |,
        COL_rank            => qq| int(5)  unsigned default NULL |,
        COL_exon_strand     => qq| tinyint(2)       default NULL |,
        COL_exon_start      => qq| int(10) unsigned default NULL |,
        COL_exon_end        => qq| int(10) unsigned default NULL |,
        COL_coding_start    => qq| int(10) unsigned default NULL |,
        COL_coding_end      => qq| int(10) unsigned default NULL |,
        COL_5utr_start      => qq| int(10) unsigned default NULL |,
        COL_5utr_end        => qq| int(10) unsigned default NULL |,
        COL_3utr_start      => qq| int(10) unsigned default NULL |,
        COL_3utr_end        => qq| int(10) unsigned default NULL |,
        VAL_wb_gene_id      => sub{ $_[0]->{'wb_gene_id'  } },
        VAL_transcript_name => sub{ $_[0]->{'transcript_name'  } },
        VAL_chromosome_name => sub{
          # Strip any leading 'CHROMOSOME_' prefix from name
          my $name = $_[0]->{'chromosome_name'} || return;
          $name =~ s/^CHROMOSOME_//;
          return $name },
        VAL_exon_name       => sub{ $_[0]->{'exon_name'   } },
        VAL_rank            => sub{ $_[0]->{'rank'        } },
        VAL_exon_strand     => sub{ $_[0]->{'exon_strand' } },
        VAL_exon_start      => sub{ $_[0]->{'exon_start'  } },
        VAL_exon_end        => sub{ $_[0]->{'exon_end'    } },
        VAL_coding_start    => sub{ $_[0]->{'coding_start'} },
        VAL_coding_end      => sub{ $_[0]->{'coding_end'  } },
        VAL_5utr_start      => sub{ $_[0]->{'5utr_start'  } },
        VAL_5utr_end        => sub{ $_[0]->{'5utr_end'    } },
        VAL_3utr_start      => sub{ $_[0]->{'3utr_start'  } },
        VAL_3utr_end        => sub{ $_[0]->{'3utr_end'    } },
      }
    }, # End cds__main
  }, # End gene__main
};

#----------------------------------------------------------------------
# Convenience AceDB accessor.
# Arg[0]: The AceDB object to interrogate.
# Arg[1]: The attribute to retrieve.
# List context; Retrieves a list of whatever the object has at the attrib,
# Scalar context; the first element, stringified, of the object's attrib.
#
sub at{
  my $obj = shift || return wantarray ? () : ''; # Handle empty objects
  my $key = shift || die( "Need an object accessor for $obj" );
  #warn( "==> ", $obj->class," ",$obj->filled );
  my @vals = $obj->at($key);
  return wantarray ? @vals : ( ref($vals[0]) ? $vals[0]->name : $vals[0] ); 
}

#----------------------------------------------------------------------
# Convenience AceDB accessor for object name attribute.
# Arg[0]: The AceDB object to interrogate.
# Arg[1]: Optional, The expected class of the object. 
# Returns the name of an object, as long as it's class is the same as the 
# requested class
#
sub name{
  my $obj = shift || return wantarray ? () : ''; # Handle empty objects
  my $key = shift || undef;
  if( $key ){ 
    # Test that the object is of the requested class
    if( $obj->class ne $key ){ return wantarray ? () : '' } 
  }
  return wantarray ? ( $obj->name ) : $obj->name ;
}

#----------------------------------------------------------------------
# Screens physical_position for chromosome positions only
sub chromosome_position{
  # No longer used
  my( $name, $start, $end, $strand ) = physical_position(@_);
  return $name =~ /^chr/i ? ( $name, $start, $end, $strand ) : ();
}

#----------------------------------------------------------------------
# Takes a sequence or gene object, or a tag referencing one, and returns
# the name, start (bp), end (bp), and strand of the top-level sequence. 
# Postions are cached to optimise multiple hits.
# Function is recursive due to representation of positions in WormBase schema
my $ASSEMBLY;
sub physical_position{
  my $obj = shift;
  my $class = $obj->class eq 'tag' ? $obj->right->class : $obj->class;
  my $name  = $obj->class eq 'tag' ? $obj->right->name  : $obj->name;

  # Look in cache first, and return if hit
  if( $ASSEMBLY->{$class.$name} ){ return @{$ASSEMBLY->{$class.$name}} }
  
  if( $obj->class eq 'tag' ){ $obj = $obj->fetch }

  # Object's physical pos'n not in cache. Calculate from location in parent.
  # Note: $obj can be either Sequence, Gene, CDS etc; Allow for model diffs.
  if( my $parent_obj = 
      (
       $obj->at("Structure.From.Source")  || 
       $obj->at("SMap.S_parent.Sequence") ||
       $obj->at("Molecular_info.Corresponding_CDS") # Non-SMap gene (briggsae)
       ) ){
    $parent_obj = $parent_obj->fetch;
    my( $pname, $pstart, $pend, $pstrand ) =  &physical_position($parent_obj);
    my $class =  $obj->class;
    my $pos_t;
    if( $class eq 'Sequence'){
      $pos_t = $parent_obj->at("Structure.Subsequence.$name");
    } elsif( $class eq 'Gene'){
      if( $parent_obj->class eq 'CDS' ){
        # Hack for Non-SMap gene (briggsae)
        my $cds = $parent_obj;
        $parent_obj = $parent_obj->at("SMap.S_parent.Sequence")->fetch;
        $pos_t = $parent_obj->at("SMap.S_child.CDS_child.$cds");
      } else {
        $pos_t = $parent_obj->at("SMap.S_child.Gene_child.$name");
      }
    } elsif( $class eq 'CDS'){
      ($pos_t) = grep{$_ eq $name} $parent_obj->at("SMap.S_child.CDS_child");
    } elsif( $class eq 'Transcript'){
      ($pos_t) = grep{$_ eq $name} $parent_obj->at("SMap.S_child.Transcript");
    } else{ 
      warn( "Cannot get physical_position for $class objects" );
      return();
    }
    if( ! $pos_t ){
      warn( "Failed to get physical_position for $class $name" );
      return();
    }
    my $start  = $pos_t->right    + $pstart - 1;
    my $end    = $pos_t->right(2) + $pstart - 1;
    my $strand = $pstrand;my $aceobj = shift;
    if( $start > $end ){
      my $s = $start;
      $start = $end;
      $end   = $s;
      $strand = $strand * -1;
    }
    $ASSEMBLY->{$class.$name} = [$pname, $start,$end,$strand]; # Update cache
  }
  else{
    if( $class eq "Sequence" ){ # top-level sequence (e.g. chromosome)
      $ASSEMBLY->{$class.$name} = [$name, 1,undef,1]; # Update cache
    } else { # Unmapped e.g. gene object
      $ASSEMBLY->{$class.$name} = [];
    }
  }
  return @{$ASSEMBLY->{$class.$name}};
}

#----------------------------------------------------------------------
# A subroutine to generate 'custom' objects, each representing       
# an RNAi phenotype. Each 'object' is simply a hashref containing         
# column-value pairs                                                      
my %phenotype_descriptions;
sub process_rnai{
  my $gene_obj = shift;
  my @all_data; # Each entry is a record in the table
  foreach my $rnai_obj( &at($gene_obj,'Experimental_info.RNAi_result') ){
    $rnai_obj = $rnai_obj->fetch;
    # Parse the author
    my( $first, @others ) = &at($rnai_obj,"Experiment.Author");
    $first ||= '';
    my $author = "$first";
    if( @others == 1 ){ $author .= ", $others[0]" }
    elsif( @others ){ $author .= " et. al." }
    # Create 'global' rnai data and copy for each phenotype
    my %rnai_data = ( 'rnai'   => "$rnai_obj",
                      'author' => $author );
    foreach my $phen_obj( &at( $rnai_obj, "Phenotype") ){
      # Create a cache for phenotypes to prevent repeated acedb fetches
      my $description;
      if( $phenotype_descriptions{"$phen_obj"} ){
        # Take from cache
        $description = $phenotype_descriptions{"$phen_obj"};
      } else {
        # Fetch from DB
        $phen_obj = $phen_obj->fetch;
        ( $description ) = &at($phen_obj,'Description' );
        $phenotype_descriptions{"$phen_obj"} = $description || '';
      }
      my %phen_data = ( %rnai_data,
                        'phenotype' => "$phen_obj",
                        'phenotype_description' => $description );
      push @all_data, {%phen_data};
    }
  }
  return @all_data;
}

#----------------------------------------------------------------------
# A subroutine to generate 'custom' AceDB objects, each representing
# an exon. Each 'object' is simply a hashref containing column-value 
# pairs
my %structure_cache; # For performance
sub process_structure{
  my $aceobj = shift;
  my $gene   = shift;
  
  # Examine cache;
  if( my $cached = $structure_cache{$aceobj} ){ return @$cached }

  my @exons_data; # Each entry is a record in the table
  my( @transcripts, @cds_exons ); 

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
    @transcripts = $aceobj->at('Molecular_info.Corresponding_transcript');
  }

  foreach my $tran( @transcripts ){
    # Process each transcript (coding and non-coding)
    $tran = $tran->fetch;
    my @exons = &_exon_physical_positions( $tran );
    my @data  = &_exonic_structure( [@exons],[@cds_exons] );
    #Add the transcript_name attribute
    my $tran_name = $tran->name;
    map{ 
      $_->{'transcript_name'} = $tran_name;
      $_->{'exon_name'}       = $tran_name . ".exon" . $_->{'rank'};
    } @data;
    # Push these onto all
    push @exons_data, @data;
  }

  # Add the wb gene id attribute
  my $wb_gene_id = "$gene";
  map{ $_->{wb_gene_id}=$wb_gene_id } @exons_data;
  #warn Dumper(\@exons_data);

  # Update cache
  $structure_cache{$aceobj} = [@exons_data];

  # All done
  return(@exons_data);
}


#----------------------------------------------------------------------
# Gets all exons off transcript/CDS object, and calculates their physical
# start/end/ori coordinates.
sub _exon_physical_positions{
  my $aceobj = shift;
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
      if( $code_start ){ $row{coding_start} = $code_start }
      if( $code_end   ){ $row{coding_end}   = $code_end   }

      if( $code_start == $exon_start and $code_end == $exon_end ){
        # coding only 
        #warn( "  > 100% coding" );
        $seen_coding++;
      } elsif( $code_start || $code_end ){
        # Mixed coding/UTR
        #warn( "  > Mixed coding/UTR" );
        unless( $seen_coding ){ # 5-utr 
          $row{'5utr_start'} = $exon_ori>0 ? $exon_start   : $code_end+1;
          $row{'5utr_end'}   = $exon_ori>0 ? $code_start-1 : $exon_end;
        } else {
          # 3-utr
          $row{'3utr_start'} = $exon_ori>0 ? $code_end+1 : $exon_start;
          $row{'3utr_end'}   = $exon_ori>0 ? $exon_end   : $code_start-1;
        }
        $seen_coding++
      } else {
        # UTR only
        #warn( "  > 100% UTR" );
        unless( $seen_coding ){ # 5-utr
          $row{'5utr_start'} = $exon_start;
          $row{'5utr_end'}   = $exon_end;
        } else {                # 3-utr
          $row{'3utr_start'} = $exon_start;
          $row{'3utr_end'}   = $exon_end;
        }
      }
    }
    #warn Dumper(\%row);
    push @exon_structure, {%row};
  }
  return @exon_structure;

}

#---
1;
