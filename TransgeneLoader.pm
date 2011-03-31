package TransgeneLoader;

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


my $acedb_class = 'transgene';
my $dataset     = 'wormbase_' . lc( $acedb_class );
my $ACE_QUERY   = '*'; # All 
# Examples?
#$ACE_QUERY    = '';

$CONFIG = undef; # Use &get_config instead

sub get_config{
  my $acedb  = shift || die "Need an ACE DB handle";
  my $CONFIG = &autogen_ace2mart_config( $acedb, $acedb_class, 
                                         $dataset, $ACE_QUERY );
  

  #====================
  # Custom alterations to config
  # NONE - as yet :)

  # TODO: Fix ?Transgene.Reporter_product.GFP and LacZ as these are currently
  # ignored. Bug in WormMartTools?
  # TODO: Fix Map/Mapping attribs to consolidate in a singme dimension

  my $main = "TBL_${dataset}__${acedb_class}__main";
  my $main_conf = $CONFIG->{$main};

  return $CONFIG;
}


#----------------------------------------------------------------------
1;
