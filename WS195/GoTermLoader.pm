package GoTermLoader;

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
$CONNECTION->{MART_DBNAME} = 'wormmart_147'; # No default


my $dataset    = 'wormbase_go_term';
my $focus_object = 'go_term';
my $ace_query    = '*'; # All 
#my $ace_query = 'WBRNAi0000000?'; # First 9
#my $ace_query = 'WBRNAi000000??'; # First 99
#my $ace_query = 'WBRNAi00000???'; # First 999

$CONFIG = {
  "TBL_${dataset}__${focus_object}__main" => {
    
    OBJECTS => sub{
      my $ace_handle = shift;
      $ace_handle->fetch_many($focus_object=>$ace_query);
    },

    VAL_go_term => sub{ &name($_[0]) },

    VAL_name => sub{ &names_at($_[0],'Name') },

    VAL_definition => sub{ &names_at($_[0],'Definition') },

    VAL_term => sub{ &names_at($_[0],'Term') },

    VAL_type => sub{ &names_at($_[0],'Type') },

    #==================================================
    # Child
    #&insert_dimension('GO_term','Child.Instance',
    #                  $dataset,'child_instance'  ),
    #&insert_dimension('GO_term','Child.Component',
    #                  $dataset,'child_component'),

    # Cannot use insert_dimension as need to combine two model 
    # locations together into a single dimension.
    "TBL_${dataset}__child__dm" => {
      OBJECTS => sub{
        my @objs = ();
        push @objs, ( map{$_->{relation} = 'Instance'; $_} 
                      &cached_data_at($_[0],'Child.Instance') );
        push @objs, ( map{$_->{relation} = 'Component'; $_}
                      &cached_data_at($_[0],'Child.Component' ) );
        return[ @objs ];
      },
      VAL_go_term  => &cached_val_sub('name'),
      VAL_term     => &cached_val_sub('term'),
      VAL_type     => &cached_val_sub('type'),
      VAL_relation => &cached_val_sub('relation'),
      IDX => ['go_term','type'],
    },
    "VAL_child_dmlist" => sub{
      join( $LIST_SEPARATOR,
            map{ $_->{name} }
            ( &cached_data_at($_[0],'Child.Instance'), 
              &cached_data_at($_[0],'Child.Component') ) );
    },
    "VAL_child_dminfo" => sub{
      join( $LIST_SEPARATOR,
            map{ $_->{info} }
            ( &cached_data_at($_[0],'Child.Instance'), 
              &cached_data_at($_[0],'Child.Component') ) );
    },

    #----------
    # Don't need - using ?GO_term.Attributes_of.Index.Ancessor.
    # Need a dimension that has all children of the term to the 
    # bottom of the tree - not just the next level.
    #"TBL_${dataset}__child_tree__dm" => {
    #  OBJECTS     => sub{[ &goterm_child_tree($_[0]) ]},
    #  VAL_go_term => sub{ ref($_[0]) ? '' : $_[0] },
    #  IDX => ['go_term'],
    #},


    #==================================================
    # Parent
    #&insert_dimension('GO_term','Parent.Instance_of',
    #                $dataset,'parent_instance'),
    #&insert_dimension('GO_term','Parent.Component_of',
    #                  $dataset,'parent_component'),

    # Cannot use insert_dimension as need to combine two model 
    # locations together into a single dimension.
    "TBL_${dataset}__parent__dm" => {
      OBJECTS => sub{
        my @objs = ();
        push @objs, ( map{$_->{relation} = 'Instance'; $_} 
                      &cached_data_at($_[0],'Parent.Instance_of') );
        push @objs, ( map{$_->{relation} = 'Component'; $_}
                      &cached_data_at($_[0],'Parent.Component_of' ) );
        return[ @objs ];
      },
      VAL_go_term  => &cached_val_sub('name'),
      VAL_term     => &cached_val_sub('term'),
      VAL_type     => &cached_val_sub('type'),
      VAL_relation => &cached_val_sub('relation'),
      IDX => ['go_term','type'],
    },
    "VAL_parent_dmlist" => sub{
      join( $LIST_SEPARATOR,
            map{ $_->{name} }
            ( &cached_data_at($_[0],'Parent.Instance_of'),
              &cached_data_at($_[0],'Parent.Component_of') ) );
    },
    "VAL_parent_dminfo" => sub{
      join( $LIST_SEPARATOR,
            map{ $_->{info} }
            ( &cached_data_at($_[0],'Parent.Instance_of'), 
              &cached_data_at($_[0],'Parent.Component_of') ) );
    },

    #==================================================
    # Attributes_of 
    &insert_dimension       ('Cell','Attributes_of.Cell',
                             $dataset,'cell'),
    &insert_dimension       ('Paper','Attributes_of.Reference',
                             $dataset,'paper'),
    &insert_dimension       ('Motif', 'Attributes_of.Motif',
                             $dataset,'motif'),
    &insert_dimension       ('Gene','Attributes_of.Gene',
                             $dataset,'gene'),
    &insert_simple_dimension('Attributes_of.CDS',
                             $dataset,'cds'),
    &insert_simple_dimension('Attributes_of.Sequence',
                             $dataset,'sequence'),
    &insert_simple_dimension('Attributes_of.Transcript',
                             $dataset,'transcript'),
    &insert_simple_dimension('Attributes_of.Pseudogene',
                             $dataset,'pseudogene'),
    &insert_dimension       ('Phenotype','Attributes_of.Phenotype',
                             $dataset,'phenotype'),
    &insert_dimension       ('Anatomy_term','Attributes_of.Anatomy_term',
                             $dataset,'anatomy_term'),
    &insert_dimension       ('Homology_group','Attributes_of.Homology_group',
                             $dataset,'homology_group'),
    &insert_dimension       ('Expr_pattern','Attributes_of.Expr_pattern',
                             $dataset,'expr_pattern' ),

    &insert_dimension('GO_term','Attributes_of.Index.Ancestor',
                      $dataset,'ancestor'),
    &insert_dimension('GO_term','Attributes_of.Index.Descendent',
                      $dataset,'descendent'),

    #======================================================================
    # Version
    val_version => sub{ warn $_[0]->at('Version') },
    
    IDX => ['go_term','type'],

    #==================================================
    # DONE
    #==================================================
  }, 
};


#---
# Recursive function that builds a list of all child GO terms under a 
# node of the DAG, and adds this to the mart dimension pseudo-object 
#sub goterm_child_tree{
#  my $parent = shift;
#  
#  my $data_from_cache = &object_data_cache($parent);
#
#  unless( $data_from_cache->{'child_tree'} ){
#    # Build from scratch
#    my @go_ids = ( "$parent" );
#    foreach my $child( &cached_at( $parent, 'Child.Instance' ),
#                       &cached_at( $parent, 'Child.Component' ) ){
#      push @go_ids, &goterm_child_tree($child); #Recurse
#    }
#    
#    # Uniquify list. Using reference so cache should auto-update
#    my %seen;
#    $data_from_cache->{'child_tree'} = [ grep{ ! $seen{$_} ++ } @go_ids ];
#  }
#  return @{$data_from_cache->{'child_tree'}};
#}

1;
