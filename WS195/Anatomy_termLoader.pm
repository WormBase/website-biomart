package Anatomy_termLoader;

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


my $acedb_class = 'anatomy_term';
my $dataset     = 'wormbase_' . lc( $acedb_class );
my $ACE_QUERY   = '*'; # All 
# Examples?
#$ACE_QUERY    = 'WBbt:0000101'; # First one
#$ACE_QUERY    = 'WBbt:0001002'; # First one with a Remark
#$ACE_QUERY = 'WBbt:0003903'; # Three remarks, all with evidence
#$ACE_QUERY = 'WBbt:0006870'; # Definition with two pieces of exidence
#$ACE_QUERY    = 'WBbt:0003810'; # Remark with evidence also GO_term
#$ACE_QUERY    = 'WBbt:0000???'; # First three
#$ACE_QUERY    = 'WBbt:0001???'; # Second 1000

$CONFIG = undef; # Use &get_config instead

#TODO:
# - Fix assignment of #Evidence to Remarks
# - Strip 'lineage name: ' prefix from synonym
# - Combine the parent/child classifications into a single dimension!

sub get_config{
  my $acedb  = shift || die "Need an ACE DB handle";
  my $CONFIG = &autogen_ace2mart_config( $acedb, $acedb_class, 
                                         $dataset, $ACE_QUERY );
  

  #====================
  # Custom alterations to config
  my $main = "TBL_${dataset}__${acedb_class}__main";
  my $main_conf = $CONFIG->{$main};

  # Replace synonym config with one that strips 'lineage name: ' prefix
  my $col = "Synonym_AnatomyName";
  my $pos = "Synonym";
  $main_conf->{"TBL_${dataset}__${col}__dm"} || 
      die( "$col not found in CONF" );
  $main_conf->{"TBL_${dataset}__${col}__dm"}->{OBJECTS} = sub{
    [ map{ s/lineage name: //;$_ } &names_at($_[0],$pos) ];
  };
  $main_conf->{"VAL_${col}_dmlist"} = sub{
    join( $LIST_SEPARATOR, map{ s/lineage name: //;$_ } 
          &names_at($_[0],$pos) );
  };

  # Create combined dimensions for parent/child terms
  my @types = qw( DESCENDENT_OF_%s
                  DESC_IN_HERM_%s
                  DESC_IN_MALE_$s
                  DEVELOPS_FROM_%s
                  IS_A_%s
                  PART_OF_%s );
  foreach my $relation( 'Child', 'Parent' ){
    my @pos = ( map{ join(".", $relation, 
                          sprintf( $_, lc( substr($relation,0,1) ) ) ) }
                @types );
    my $col  = "${relation}_AnatomyTerm";
    warn( "TBL_${dataset}__${col}__dm" );
    $main_conf->{"TBL_${dataset}__${col}__dm"} = {
      OBJECTS => sub{ 
        my @objs;
        foreach my $pos( @pos ){
          my $rtype = $pos;
          $rtype =~ s/.+\.//o; # Strip Child./Parent.
          foreach my $anat( &cached_data_at($_[0],$pos) ){
            my %row = %$anat; # Dereference as adding term-specific info
            $row{info} .= ' ($rtype)';
            $row{relationship_type} = $rtype;
            push @objs, {%row};
          }
        }
        return [ @objs ];
      },
      VAL_anatomy_term      => &cached_val_sub('name'),
      VAL_term              => &cached_val_sub('term'),
      VAL_relationship_type => &cached_val_sub('relationship_type'),
      IDX                   => ['anatomy_term','term','relationship_type'],
    };
    $main_conf->{"VAL_${col}_dmlist"} = sub{
      &dmlist_at($_[0],@pos);
    };
    $main_conf->{"VAL_${col}_dminfo"} = sub{ 
      &dminfo_at( $_[0],@pos );
    };
  };

  return $CONFIG;
}


#----------------------------------------------------------------------
1;
