package GoTermPointerLoader;

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

$CONNECTION->{MART_HOST}   = ''; # Def localhost
$CONNECTION->{MART_PORT}   = 3307;    # Def 3306
$CONNECTION->{MART_USER}   = '';  # Def euid user
$CONNECTION->{MART_PASS}   = undef(); # Def empty
$CONNECTION->{MART_DBNAME} = ''; # No default


my $dataset    = 'wormbase_go_term_pointer';
my $focus_object = 'go_term';
my $ace_query    = '*'; # All 

$CONFIG = {
  "TBL_${dataset}__${focus_object}__main" => {
    
    OBJECTS => sub{
      my $ace_handle = shift;
      $ace_handle->fetch_many($focus_object=>$ace_query);
    },

    VAL_go_term => sub{ &name($_[0]) },

    VAL_name => sub{ &names_at($_[0],'Name') },

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

    #======================================================================
    # Version
    val_version => sub{ warn $_[0]->at('Version') },
    
    IDX => ['go_term','type'],

    #==================================================
    # DONE
    #==================================================
  }, 
};

1;
