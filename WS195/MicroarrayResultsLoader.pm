package MicroarrayResultsLoader;

use strict;
use warnings;
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


my $acedb_class = 'microarray_results';
my $dataset     = 'wormbase_' . lc( $acedb_class );
my $ACE_QUERY   = '*'; # All 

# Examples
#$ACE_QUERY    = '171720_x_at';

$CONFIG = undef; # Use &get_config instead

sub get_config{
  my $acedb  = shift || die "Need an ACE DB handle";
  my $CONFIG = &autogen_ace2mart_config( $acedb, $acedb_class, 
                                         $dataset, $ACE_QUERY );
  
  my $main = "TBL_${dataset}__${acedb_class}__main";
  my $main_conf = $CONFIG->{$main};

  #====================
  # Custom alterations to config

  #--------------------
  # Create a 'paper' dimension based on 'Microarray_experiment' dimension
  my $paper_dm = {
    OBJECTS => sub{
      my @papers = map{@{$_->{paper}}} &cached_data_at( $_[0],'Results' );
      my %papers = map{$_->{name}=>$_} @papers;
      return[ values( %papers ) ];
    },
    VAL_paper          => &cached_val_sub('name'),
    VAL_cgc_name       => &cached_val_sub('cgc_name'),
    VAL_pmid           => &cached_val_sub('pmid'),
    VAL_brief_citation => &cached_val_sub('brief_citation'),
    VAL_info           => &cached_val_sub('info'),
    IDX => ['paper','cgc_name','pmid'],
  };
  # Add the dimension
  $main_conf->{"TBL_${dataset}__paper__dm"} = $paper_dm; 
  $main_conf->{"VAL_paper_count"} = sub{
    my @papers = map{@{$_->{paper}}} &cached_data_at( $_[0],'Results' );
    my %papers = map{$_->{name}=>$_} @papers;
    return scalar( values( %papers ) );
  };
  $main_conf->{"VAL_paper_dmlist"} = sub{
    my @papers = map{@{$_->{paper}}} &cached_data_at( $_[0],'Results' );
    my %papers = map{$_->{name}=>$_} @papers;
    return join( $WormMartTools::LIST_SEPARATOR, 
                 map{ $_->{name} } values %papers );
  };
  $main_conf->{"VAL_paper_dminfo"} = sub{
    my @papers = map{@{$_->{paper}}} &cached_data_at( $_[0],'Results' );
    my %papers = map{$_->{name}=>$_} @papers;
    return join( $WormMartTools::LIST_SEPARATOR, 
                 map{ $_->{info} } values %papers );
  };

  return $CONFIG;
}


#----------------------------------------------------------------------
1;
