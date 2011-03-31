package RnaiLoader;

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
$CONNECTION->{MART_DBNAME} = 'wormmart_146'; # No default

my $acedb_class = 'rnai';
my $dataset     = 'wormbase_' . lc( $acedb_class );
my $ACE_QUERY   = '*'; # All 

#$ACE_QUERY = 'WBRNAi00021695'; # All phenotypes are 'Not'
#$ACE_QUERY = 'WBRNAi00025786';
#ACE_QUERY = 'WBRNAi00000001'; # First 1 
#$ACE_QUERY = 'WBRNAi0000000?'; # First 9
#$ACE_QUERY = 'WBRNAi000000??'; # First 99
#$ACE_QUERY = 'WBRNAi00000???'; # First 999

$CONFIG = undef;

sub get_config{
  my $acedb  = shift || die "Need an ACE DB handle";
  my $CONFIG = &autogen_ace2mart_config( $acedb, $acedb_class, 
                                         $dataset, $ACE_QUERY );
  
  #====================
  # Custom alterations to config
  my $main = "TBL_${dataset}__${acedb_class}__main";
  my $main_conf = $CONFIG->{$main};

  #----------
  # Populate phenotype table with contents of Phenotype_info hash
  # Code shared with Variation dataset
  my %phenotype_dimension = &insert_dimension
      ('Phenotype_info',undef, $dataset,'Phenotype');
  foreach my $component( keys %phenotype_dimension ){
    $main_conf->{ $component } = $phenotype_dimension{$component};
  }
  

  #----------
  # Create count, dmlist and dminfo columns for primary/secondary genes  
  foreach my $type qw( primary secondary ){
    $main_conf->{"VAL_Inhibits_Gene_${type}_count"} = sub{
      my $rnai = shift;
      my $rnai_data = &object_data_cache( $rnai );
      return scalar @{$rnai_data->{"inhibits_gene_${type}"}};
    };
    $main_conf->{"VAL_Inhibits_Gene_${type}_dmlist"} = sub{
      my $rnai = shift;
      my $rnai_data = &object_data_cache( $rnai );
      join( $LIST_SEPARATOR, 
            map{$_->{public_name}} @{$_->{"inhibits_gene_${type}"}} );
    };
    $main_conf->{"VAL_Inhibits_Gene_${type}_dmlist"} = sub{
      my $rnai = shift;
      my $rnai_data = &object_data_cache( $rnai );
      join( $LIST_SEPARATOR, 
            map{$_->{info}} @{$_->{"inhibits_gene_${type}"}} );
    };
  }


  #----------------------------------------------------------------------
  # Process RNAi.Homol.Homol_Homol for SMap and RNAi homology info

  # Remove auto-generated homol gubbins
  delete( $main_conf->{"TBL_${dataset}__Homol_HomolHomol_HomolData__dm"} );
  delete( $main_conf->{"VAL_Homol_HomolHomol_HomolData_count"} );
  delete( $main_conf->{"VAL_Homol_HomolHomol_HomolData_dmlist"} );

  # Add modified versions
  $main_conf->{"TBL_${dataset}__physical_position__dm"} = {
    OBJECTS => sub{[ &cached_at( $_[0],'Homol.Homol_homol' ) ]},
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
  };

  $main_conf->{"TBL_${dataset}__homol__dm"} = {
    OBJECTS => sub{ 
      my @homols = &cached_at( $_[0],'Homol.Homol_homol' );
      my %rnais = ( map{$_->{name} => $_} 
                    map{&cached_data_at($_,'Homol.RNAi_homol')} @homols );
      return[ values %rnais ];
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
  };
  
  $main_conf->{"VAL_homol_dmlist"} = sub{
    my @homols = &cached_at( $_[0],'Homol.Homol_homol' );
    my %rnais = ( map{$_->{name} => $_} 
                  map{&cached_data_at($_,'Homol.RNAi_homol')} @homols );
    return join( " | ", keys %rnais );
  };

  $main_conf->{"VAL_homol_name_dmlist"} = sub{
    my @homols = &cached_at( $_[0],'Homol.Homol_homol' );
    my %rnais = ( map{$_->{history_name} ? ($_->{history_name}=>$_) : () }
                  map{&cached_data_at($_,'Homol.RNAi_homol')} @homols );
    return join( " | ", keys %rnais );
  };

  $main_conf->{"VAL_homol_dminfo"} = sub{
    my @homols = &cached_at( $_[0],'Homol.Homol_homol' );
    my %rnais = ( map{$_->{info} => $_}
                  map{&cached_data_at($_,'Homol.RNAi_homol')} @homols );
    return join( " | ", keys %rnais );
  };

  #----------------------------------------------------------------------
  # TODO: Primary vs. secondary gene targets


  #----------------------------------------------------------------------
  # TODO: Gets Experiment.Delivered_by wrong!

  #==========
  # Add custom indices
  $main_conf->{IDX} ||= [];
  push @{$main_conf->{IDX}}, qw( rnai HistoryName_Text );
#  push @{$main_conf->{IDX}}, map{/^VAL_(\w+_count)/} keys %$main_conf );
#  push @{$main_conf->{IDX}}, ( map{$_.'_count'} 
#                               map{/^TBL_\w+__(\w+)__dm/} keys %$main_conf );
  $main_conf->{"TBL_${dataset}__Phenotype__dm"}->{IDX} ||= [];
  push ( @{ $main_conf->{"TBL_${dataset}__Phenotype__dm"}->{IDX} }, 
         'primary_name' );

  return $CONFIG;
}  

#----------------------------------------------------------------------
1;
