# A grab-bag of Ace convenience accessors and caches used to help with
# object loader modules 
package WormMartTools;

use strict;
use warnings;
use Data::Dumper qw( Dumper );
use Date::Calc;
use Carp;
use vars qw( @ISA @EXPORT );
use vars qw( $LIST_SEPARATOR ); # Sepatator to use for joining dimension lists 
$LIST_SEPARATOR = ' | ';

@ISA    = qw( Exporter );
@EXPORT = qw( $LIST_SEPARATOR
              at 
              names_at 
              unique_names_at
              dmlist_at 
              dminfo_at
              cached_at
              cached_data_at 
              evidence_at
              recursive_at
              recursive_cached_data_at 
              cached_val_sub
              has_val_sub
              object_cache
              object_cache_delete
              object_cache_query
              object_data_cache
              name
              insert_simple_dimension
              insert_coord_dimension
              insert_dimension
              insert_val
              physical_position
              autogen_ace2mart_config
              clear_large_caches
              );

#----------------------------------------------------------------------
# Convenience AceDB accessor.
# Arg[0]: The AceDB object to interrogate.
# Arg[1..n]: The AceDB schema location(s) to retrieve;
#   can be simple, e.g. Index.Ancestor (returns all 'at' that tag),
#   or complex; Attribute_of.GO_term.?GO_term.XREF.Anatomy_term.?AO_code
#   (returns all evidence codes for all GO_term attributes of the obj.
# List context; Retrieves a list of whatever the object has at the location,
# Scalar context; the first element
#
sub at{
  my $obj = shift; #$_[0];
  
  unless( UNIVERSAL::isa( $obj, "Ace::Object") ){
    return wantarray ? () : ''; # Handle empty or non-Ace objects
  }
  my @pos = @_; # Can handle multiple schema locations (e.g. for transgenes)
  @pos || die( "Need an schema location to retrieve for $obj. Called from: ".
               join( ", ", (caller(0))[1..2]));
  my @all_vals;
  foreach my $pos( @pos ){
####test by RF_011211
      ##print "WormMartTools_inside_sub_at_L64_pos=$pos\n";
####################################
    my @calls = ('');
    # build up the set of 'at' and 'col' calls needed to get vals for pos 
    my $skip_next = 0;
    foreach my $tag( split('\.', $pos ) ){
####test by RF_011211

##	print "WormmartTool_L69_tag=$tag\n";
###################################
      if( $skip_next ){ $skip_next = ''; next } # Flow control
      if( $tag eq 'UNIQUE' ){ next } # Skip
      if( $tag eq 'XREF' ){ $skip_next ++; next } # Skip + what XREF points to

      my %acedb_types = map{$_=>1} qw(Text Int Float DateTime DateType);
      if( $acedb_types{$tag} or $tag =~ m/^(\?)|(\#)/ ){
        # Traverse this tag with a 'col' call;
####test by RF_011211
	  ##print "WormMartTools_L79_if_tag=$tag==obj=$obj\n";
#################################
        push( @calls, '' );
      }
      else{
        # Traverse this tag with an 'at' call
	 ####test by RF_011211
	 ## print "WormMartTools_inside_sub_at_L86_else_tag=$tag==obj=$obj\n";  ##got the ortholog of WBGene00000001 listed in ace file such as WBGene00069407 etc but did not get paralog of WBGene0000001 i.e. WBGene00000004 listed in the ace file
################################# 
        $calls[-1] = join( ".", ($calls[-1]||()), "$tag" );
      }
    }
    if( ! $calls[-1] ){ pop @calls } # Remove the last if it is empty.
    
    push @all_vals, _at_catch_large(@calls, $obj);
  }

  return wantarray ? @all_vals : $all_vals[0]; 
}


###test by RF_011411
sub at_test{
  my $obj = shift; #$_[0];
  
  unless( UNIVERSAL::isa( $obj, "Ace::Object") ){
    return wantarray ? () : ''; # Handle empty or non-Ace objects
  }
  my @pos = @_; # Can handle multiple schema locations (e.g. for transgenes)
  @pos || die( "Need an schema location to retrieve for $obj. Called from: ".
               join( ", ", (caller(0))[1..2]));
  my @all_vals;
  foreach my $pos( @pos ){
####test by RF_011211
      ##print "WormMartTools_inside_sub_at_test_L116_pos=$pos\n";
####################################
    my @calls = ('');
    # build up the set of 'at' and 'col' calls needed to get vals for pos 
    my $skip_next = 0;
    foreach my $tag( split('\.', $pos ) ){
####test by RF_011211

##	print "WormmartTool_L69_tag=$tag\n";
###################################
      if( $skip_next ){ $skip_next = ''; next } # Flow control
      if( $tag eq 'UNIQUE' ){ next } # Skip
      if( $tag eq 'XREF' ){ $skip_next ++; next } # Skip + what XREF points to

      my %acedb_types = map{$_=>1} qw(Text Int Float DateTime DateType);
      if( $acedb_types{$tag} or $tag =~ m/^(\?)|(\#)/ ){
        # Traverse this tag with a 'col' call;
####test by RF_011211
	  ##print "WormMartTools_sub_at_test_L134_if_tag=$tag==obj=$obj\n";
#################################
        push( @calls, '' );
      }
      else{
        # Traverse this tag with an 'at' call
	 ####test by RF_011211
	  ##print "WormMartTools_inside_sub_at_test_L141_else_tag=$tag==obj=$obj\n";  ##got the ortholog of WBGene00000001 listed in ace file such as WBGene00069407 etc but did not get paralog of WBGene0000001 i.e. WBGene00000004 listed in the ace file
################################# 
        $calls[-1] = join( ".", ($calls[-1]||()), "$tag" );
      }
    }
    if( ! $calls[-1] ){ pop @calls } # Remove the last if it is empty.
    
    push @all_vals, _at_catch_large(@calls, $obj);
  }

  return wantarray ? @all_vals : $all_vals[0]; 
}


##############################################


#----------------------------------------------------------------------
# catches the cases that cause a segfault for larger arrays of data.  uses an iterator in those cases.
# Arg[0]   : The AceDB calls
# Arg[1]   : The AceDB schema location(s) to retrieve;
#
sub _at_catch_large(){
    my @calls = shift;
    my $obj = shift;
    my @vals  = $obj;
    my $first = 1;
    foreach my $tag( @calls ){
      unless( $first ){ @vals = map{ $_->col } @vals }
      if( $first ){ $first = 0 }
      if( $tag ){ 
         my @tot_items;
         foreach my $val(@vals){
           unless($val){ next } 
           my @items;
           #use an iterator instead of at to prevent segfault when getting large amount of data
           if(_get_count($val, $tag) > 20000){ 
	     warn( "[WARN] Too many objects in $tag, using iterator to retrieve");
             my @ts = split('\.', $tag);
             my $query = "find " . $ts[-1] . " " . $val->class . "=" . $val->name;
             my $i  = $obj->db->fetch_many(-query=>$query);  # fetch a cursor
             while (my $o = $i->next) { push @items, $o }
           } else { @items = $val->at($tag) }
           push @tot_items, @items;
         }
         @vals = @tot_items;
      }
    }
  return @vals; 
}


####test by RF_011411
sub _at_catch_large_test(){
    my @calls = shift;
    my $obj = shift;
    my @vals  = $obj;
    my $first = 1;
####test by RF_011411
      ##print "WormMartTools_inside__at_catch_large_test_L199_obj=$obj==calls=@calls\n";
###########################################
    foreach my $tag( @calls ){
      unless( $first ){ @vals = map{ $_->col } @vals }
####test by RF_011411
     ## print "WormMartTools_inside__at_catch_large_test_L204_tag=$tag==_vals=@vals\n";
###########################################
      if( $first ){ $first = 0 }
      if( $tag ){ 
         my @tot_items;
         foreach my $val(@vals){
           unless($val){ next } 
           my @items;
           #use an iterator instead of at to prevent segfault when getting large amount of data
           if(_get_count($val, $tag) > 20000){ 
####test by RF_011411
	       ##print "WormMartTools_inside__at_catch_large_test_L214_aftercalling__get_count\n";
################################################
	     warn( "[WARN] Too many objects in $tag, using iterator to retrieve");
             my @ts = split('\.', $tag);
             my $query = "find " . $ts[-1] . " " . $val->class . "=" . $val->name;
             my $i  = $obj->db->fetch_many(-query=>$query);  # fetch a cursor
             while (my $o = $i->next) { push @items, $o }
           } else { 
####test by RF_011411
	      
	      ## print "WormMartTools_inside__at_catch_large_test_L224_tag=$tag==val=$val\n";
#######################################
	       @items = $val->at($tag);

	       ##print "WormMartTools_inside__at_catch_large_test_L228_items=@items\n";
	       }
           push @tot_items, @items;
         }
         @vals = @tot_items;
      }
    }
####test by RF_011411
    ##print "WormMartTools_inside__at_catch_large_test_L236_vals=@vals\n";
########################################
  return @vals; 
}

##########################################

#----------------------------------------------------------------------
# Returns count of objects to be returned with the given tag.
# If no tag is given, it counts the amount of objects in the next column.
# undef objects return a count of -1
# Arg[0]   : The AceDB object to interrogate.
# Arg[1]   : The AceDB schema location to count the amount of retrievable objects;
#
sub _get_count{
  my $obj = shift;
  my $tag = shift;
  my $first_item = $tag ? $obj->at($tag . ".[1]") : $obj->right;
  return ($first_item->{'.end_row'} || return -1) - ($first_item->{'.start_row'} || return -1) + 1;
}

#----------------------------------------------------------------------
# Convenience AceDB accessor.
# Similar to &at except that it returns the object names rather than
# the objects themselves. Used to prevent DB fetch.
# Arg[0]   : The AceDB object to interrogate.
# Arg[1..n]: The attribute(s) to retrieve.    
#
sub names_at{
####test by RF_011411
    ##print "test_at_WormmartTool_L160_before_calling_at=parameter=@_\n";
    my @test_at = &at(@_);
    ##print "test_at_WormmartTool_L162_after_calling_at\n";
    my @values = map{$_->name} @test_at;
  ##my @values = map{$_->name} &at(@_);
#############################################
  return wantarray ? @values : $values[0];
}

#----------------------------------------------------------------------
# Convenience AceDB accessor.                                          
# Similar to &names_at except that it uniquifies the list of names
# Arg[0]   : The AceDB object to interrogate.          
# Arg[1..n]: The attribute(s) to retrieve.                                 
#  
sub unique_names_at{
  my %names = map{$_=>1} &names_at(@_);
  my @names = keys( %names );
  return wantarray ? @names : $names[0];
}

#----------------------------------------------------------------------
# Convenience AceDB accessor.                                          
# Similar to &unique_names_at except that it concats the list of names
# using a seperator.
# Arg[0]   : The AceDB object to interrogate.          
# Arg[1..n]: The attribute(s) to retrieve.                                 
#  
sub dmlist_at{
  my $aceobj     = $_[0];
  my $schema_pos = $_[1];
#  my %unique;
#  return join( $LIST_SEPARATOR,
#               map{ $unique{$_->{name}} ++ ? () : $_->{name} }
#               &cached_data_at($aceobj,$schema_pos) );
  return join( $LIST_SEPARATOR, &unique_names_at(@_) );
}

#----------------------------------------------------------------------
# Convenience AceDB accessor.
# Similar to dmlist_at, except that objects have more detail than their
# basic name.
sub dminfo_at{
  my $aceobj     = shift;
  my @schema_pos = @_;
  return join( $LIST_SEPARATOR,
               map{ $_->{info} || '' }
               &cached_data_at($aceobj,@schema_pos) );
}

#----------------------------------------------------------------------
# For ascending/descending recursive object heirarchies. 
# Use with care! 
#
sub recursive_at{
  my $obj = shift;
  my $pos = shift;
  my %seen = @_;
  return() if( $seen{$obj} ); #Protect against deep recursion
  my @objs = ($obj);
  foreach my $robj( &cached_at( $obj, $pos ) ){
    push @objs, &recursive_at( $robj, $pos, %seen, map{$_=>1} @objs );
  }
  return @objs;
}

#----------------------------------------------------------------------
# Queries/updates a local cache of AceDB objects.
# Args: The AceDB object(s) to cache/retrieve.
#
use vars qw( $ObjectCache $ObjectCache_count );
BEGIN{
  $ObjectCache = {};
  $ObjectCache_count = 0;
}
sub object_cache{
  my @objects = @_;
  my @cached_objects;
  foreach my $obj( @objects ){
    my $class = $obj->class;
    $ObjectCache->{$class} ||= {};
    unless(  $ObjectCache->{$class}->{$obj} ){
      $ObjectCache->{$class}->{$obj} = $obj->fetch;
      $ObjectCache_count ++;
      #warn( "--> $ObjectCache_count objects in ObjectCache" );
    }
    push  @cached_objects, $ObjectCache->{$class}->{$obj};
  }
  return wantarray ? @cached_objects : $cached_objects[0];
}
sub object_cache_delete{
  my @objects = @_;
  foreach my $obj( @objects ){
    my $class = $obj->class;
    if( $ObjectCache->{$class} ){
      if( exists( $ObjectCache->{$class}->{$obj} ) ){
        delete( $ObjectCache->{$class}->{$obj} );
        $ObjectCache_count --;
      }
    }
  }
}
sub object_cache_query{
  my $class = shift;
  my $name  = shift;
  return $ObjectCache->{$class} ? $ObjectCache->{$class}->{$name} : undef;
}

# Convenience AceDB accessor.
# Similar to &at except that the objects are fetched and then cached. 
# Useful for dimension tables based on certain objects e.g. Paper, Phenotype
# Arg[0]: The AceDB object to interrogate.
# Arg[1]: The attribute to retrieve.
#
sub cached_at{
  
  return &object_cache( &at(@_) );
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
# Takes an AcePerl object, processes the bits it needs, and stores result
# in cache. Means that we store the bits we want, and not the whole object, 
# in the cache. Returns a hashref containing the processed data.
# Arg[1] the object to process
# Arg[2] the object processing to do, if other than the default for the class.
#
use vars qw( $ObjectDataCache $ObjectDataCache_count );
BEGIN{
  $ObjectDataCache = {};
  $ObjectDataCache_count = 0;
}
sub object_data_cache{
  my $obj        = shift || die( "Need an objct" );
  my $obj_class  = $obj->class;
  my $data_class = shift || $obj_class;

  my $name  = $obj->name;
####test by RF_011411
  ##print "WormMartTools_start_object_data_cache_L315==name=$name\n";  ##ortholog got here but paralog did not get here
##############################################
  unless( $ObjectDataCache->{$data_class}->{$name} ){
    # Generate data
    my %data = ('name'=>$name);

    # Fetch object
    if( $ObjectCache->{$obj_class} and 
        my $o = $ObjectCache->{$obj_class}->{$name} ){
      # Take fetched object from object cache;
      $obj = $o;
    } else {
      # Fetch object explicitly
      $obj = $obj->fetch;
    }

    if(0){}

    elsif( $data_class eq 'AO_code' ){
      # ?AO_code # GO term evidence code
      $data{info} = "[$data{name}]";
    } # End ?AO_code

    elsif( $data_class eq 'Anatomy_term' ){
      # ?Anatomy_term
      $data{term} = &names_at($obj,'Term');
      $data{info} = "[$data{name}] $data{term}";      
    } # End ?Anatomy_term
    
    elsif( $data_class eq 'Antibody' ){
      # ?Antibody
      $data{clonality} = &names_at($obj,'Clonality');
      $data{antigen}   = &names_at($obj,'Antigen');
      $data{animal}    = &names_at($obj,'Animal');
      $data{summary}   = [ &names_at($obj,'Summary') ],
      $data{info} = join( ' ', "[$data{name}]", ($data{summary}->[0]||()) );
    } # End ?Antibody

    elsif( $data_class eq 'CDS' ){
      # ?CDS
      $data{gene} = &names_at($obj,'Visible.Gene');
      $data{info} = "[$data{name}] product of gene $data{gene}";
    }

    elsif( $data_class eq 'Cell' ){
      # ?Cell
      $data{brief_id} = &names_at($obj,'Brief_id');
      $data{cell_group} = &names_at($obj,'Cell_group');
      $data{anatomy_term} = &names_at($obj,'anatomy_term');
      $data{info} = join( ' ', "[$data{name}]", ( $data{brief_id} || () ) );
    } # End ?Cell
    
    elsif( $data_class eq 'Expr_pattern' ){
      # ?Expr_pattern
      $data{patterns} = [ &names_at($obj,'Pattern') ];
      $data{subcellular_localizations} = 
          [ &names_at($obj,'Subcellular_localization') ];
      $data{info} = join( $LIST_SEPARATOR,
                          map{"[$data{name}] $_"} 
                          @{$data{patterns}} );
      $data{info} ||= "[$data{name}]";
    } # End ?Expr_pattern

    elsif( $data_class eq 'Gene' ){
      # ?Gene
	##print "WormMartTools_inside_object_data_cache_before_calling_names_at_L378\n";
      $data{sequence_name} = 
          &names_at($obj,'Identity.Name.Sequence_name');
####test by RF_011211
      my $t_data_sequence_name = $data{sequence_name};
      ##print "WormMartTools_inside_object_data_cache_after_calling_names_at_L383_t_data_sequence_name=$t_data_sequence_name\n";
#################################################
      $data{public_name} = 
          &names_at($obj,'Identity.Name.Public_name');
      $data{cgc_name} = 
          &names_at($obj,'Identity.Name.CGC_name');
      $data{concise_description} = 
      [&names_at($obj,'Gene_info.Structured_description.Concise_description')];

      my %seen = ();
      $data{molecular_names} = 
          [ grep{ ! $seen{$_} ++ } 
            ( ($data{sequence_name}||()),
              &names_at($obj,'Identity.Name.Molecular_name') ) ];
      %seen = ();
      $data{other_names} =
          [ grep{ ! $seen{$_} ++ }
            ( ($data{public_name}||()),($data{cgc_name}||()),
              &names_at($obj,'Identity.Name.Other_name') ) ];
      %seen = ();
      $data{all_names} = 
          [ grep{ ! $seen{$_} ++ }
            ( $data{name},@{$data{molecular_names}},@{$data{other_names}} ) ];

      $data{gene_class} = 
         &names_at($obj,'Gene_info.Gene_class');
      if( my $gene_class = cached_at($obj,'Gene_info.Gene_class') ){
        $data{class_description} = [&names_at($gene_class,'Description')];
      }
      $data{species} =
          &names_at($obj,'Identity.Species');
####test by RF_010711
      my $t_data_species = $data{species};
     ## print "WormMartTool_inside_object_data_cache_L408_species=$t_data_species\n";
###########################################################

      $data{info} = "[$data{name}] $data{public_name}";
      if( my $d = $data{class_description}->[0] ){ $data{info} .= " ($d)" }
    } # End Gene

    elsif( $data_class eq 'Gene_regulation' ){
      # ?Gene_regulation
      $data{summary} = [ &names_at($obj,'Summary') ];
      $data{method}               = [];
      $data{method_info}          = [];
      $data{trans_regulator_gene} = [];
      $data{trans_regulator_seq}  = [];
      $data{cis_regulator_seq}    = [];
      $data{other_regulator}      = [];
      $data{target_info}          = [];
      $data{trans_regulated_gene} = [];
      $data{trans_regulated_seq}  = [];
      $data{cis_regulated_seq}    = [];
      $data{other_regulated}      = [];
      $data{result}               = [];
      $data{result_info}          = [];

      foreach my $method( &names_at($obj,'Method') ){
        push( @{$data{method}}, $method );
        push( @{$data{method_info}}, 
              map{ "[$method] $_" } &names_at($obj,"Method.$method")||('') )
      }

      foreach my $loc( 'Regulator.Trans_regulator_seq',
                       'Regulator.Cis_regulator_seq',
                       'Regulator.Other_regulator',
                       'Target.Target_info.Expr_pattern',
                       'Target.Trans_regulated_seq',
                       'Target.Cis_regulated_seq',
                       'Target.Other_regulated' ){

        foreach my $val( &names_at($obj,$loc) ){
          my $key = lc($loc);
          $key =~ s/\w+\.(\w+)/$1/;
          push( @{$data{$key}}, $val );
        }
      }

      foreach my $loc( 'Regulator.Trans_regulator_gene',
                       'Target.Trans_regulated_gene' ){
        foreach my $gene( &cached_data_at($obj,$loc) ){
          my $key = lc($loc);
          $key =~ s/\w+\.(\w+)/$1/;
          push( @{$data{$key}}, $gene->{name} );
          push( @{$data{$key."_name"}}, $gene->{public_name} );
        }
      }
                       
      foreach my $result( &names_at($obj,'Result') ){
        push( @{$data{result}}, $result );
        my $gr_condition = &at($obj,"Result.$result") || next;
        my $val = $gr_condition->fetch;
        push( @{$data{result_info}},
              "[$result] $gr_condition - $val" );
      }
      $data{info} = join( ", ", "[$data{name}]", ($data{summary} || ()) ); 
    } # End ?Gene_regulation

    elsif( $data_class eq 'GO_term' ){
      # ?GO_term
      $data{term} = &names_at($obj,'Term');
      $data{type} = names_at($obj,'Type');
      $data{info} = join( ' ', "[$data{name}]", 
                          uc($data{type}||''),
                          ($data{term}||()) );
    } 

    elsif( $data_class eq 'Homology_group' ){
      # ?Homology_group (e.g. KOGs, Imparanoid)
      $data{group_type} = &names_at($obj,'Group_type');

      $data{title}      = &dmlist_at($obj,'Title');
###test by RF_010711
      my $t_data_group = $data{group_type};
      my $t_data_name = $data{name};
      ##print "WormMartTools_L467_t_data_group=$t_data_group==t_data_name=$t_data_name\n";
      if($t_data_name eq 'InP_Cae_000004'){
	POSIX:exit(0);
      }
##############################
      $data{cog_type}   = &names_at($obj,'Group_type.COG.COG_type');
      $data{cog_code}   = &names_at($obj,'Group_type.COG.COG_code');
####added by RF_011211
      ##$data{hops_group} = &names_at($obj, 'Group_type.HOPS_group');
      ##$data{rio_group} = &names_at($obj, 'Group_type.RIO_group');
      ##$data{inparanoid_group} = &names_at($obj, 'Group_type.InParanoid_group');
      ##$data{orthomcl_group} = $names_at($obj, 'Group_type.OrthoMCL_group');
#########################################################################
      $data{info} = join( ' ', "[$data{name}]", ( $data{title} || () ) );
    } # End ?Homology_group

    elsif( $data_class eq 'Laboratory' ){
      # ?Laboratory
      $data{mails} = [ &names_at($obj,'Address.Mail') ],
      $data{info} = "[$data{name}] $data{mails}->[0]";
    } # End ?Laboratory

    elsif( $data_class eq 'Microarray' ){
      # ?Microarray
      $data{paper_dmlist}  = &dmlist_at($obj,'Reference');
    } # End ?Microarray

    elsif( $data_class eq 'Microarray_experiment' ){
      # ?Microarray_experiment
      $data{remark_dmlist} = &dmlist_at($obj,'Remark');
      $data{paper_dmlist}  = &dmlist_at($obj,'Reference');
      $data{paper} = [ &cached_data_at($obj,'Reference') ];
    } # End ?Microarray_experiment

    elsif( $data_class eq 'Motif' ){
      # ?Motif
      $data{title}      = &names_at($obj,'Title');
      my $database      = &at($obj,'DB_info.Database');
      $data{database}   = $database ? $database->name : undef;
      $data{accession}  = $database ? $database->right(2)->name : undef;
      $data{info} = "[$data{name}] ".($data{title}||'');
    } # End ?Motif

    elsif( $data_class eq 'Oligo_set' ){
      # ?Oligo_set
      $data{remark}     = &dmlist_at($obj,'Remark');
    } # End ?Oligo_set

    elsif( $data_class eq 'Operon' ){
      # ?Operon
      my( $seq, $start, $end ) = &physical_position($obj);
      $seq=~s/^CHROMOSOME_//; # Strip any leading 'CHROMOSOME_'
      $data{smap_sequence} = $seq;
      $data{smap_start}    = $start;
      $data{smap_end}      = $end;
      $data{method}        = &names_at($obj,'Method');
    } # End ?Operon
    
    elsif( $data_class eq 'Paper' ){
      # ?Paper
      $data{cgc_name}       = &names_at($obj,'Name.CGC_name');
      $data{pmid}           = &names_at($obj,'Name.PMID');
      $data{brief_citation} = &names_at($obj,'Brief_citation');
      $data{info} = join( " ", "[$data{name}]", ($data{brief_citation}||()) );
      ($data{inline_name})  = ( $data{info} =~ m/(.+\))?/ );
    } # End ?Paper

    elsif( $data_class eq 'Person' ){
      # ?Person
      $data{first_name}    = &names_at($obj,'Name.First_name');
      $data{last_name}     = &names_at($obj,'Name.Last_name');
      $data{standard_name} = &names_at($obj,'Name.Standard_name'); 
      $data{full_name}     = &names_at($obj,'Name.Full_name');
      $data{middle_names}  = [&names_at($obj,'Name.Middle_name')];
      $data{info} = "[$data{name}] ". $data{standard_name} || '';
    } # End ?Person

    elsif( $data_class eq 'Phenotype' ){
      # ?Phenotype
      if( $data{name} !~ /WBPhenotype/ ){ return() } # Ignore old-style
      $data{description}  = &names_at($obj,'Description');
      $data{primary_name} = &names_at($obj,'Name.Primary_name');
      $data{short_name}   = [grep{defined($_)} 
                             &names_at($obj,'Name.Short_name')];
      $data{label} = join( ' ',
                           ( scalar( @{$data{short_name}} )
                             ? ( sprintf('%s',
                                         join(',',
                                              map{uc($_)} 
                                              @{$data{short_name}})))
                             : () ),
                           ( $data{primary_name} || () ),
                           );
      $data{info}  = join( ' ',
                           "[$data{name}]",
                           $data{label}, ); 
    } # End Phenotype

    elsif( $data_class eq 'Protein' ){
      # ?Protein
      #$data{database} = undef;
      #$data{database_id} = undef;
      #if( $data{name} =~ m/^(\w+)\:/ ){
      #  $data{database} = $1;
      #  $data{database_id} =&names_at($obj,"DB_info.Database.$data{database}");
      #}

      ( $data{database}, $data{accession} ) = split( ':', $data{name} );
      $data{swissprot_ac} = 
          &names_at($obj,'DB_info.Database.SwissProt.SwissProt_AC');
      $data{swissprot_id} = 
          &names_at($obj,'DB_info.Database.SwissProt.SwissProt_ID');
      $data{trembl_ac} = 
          &names_at($obj,'DB_info.Database.TREMBL.TrEMBL_AC');
      $data{gene_name}    = &names_at($obj,'DB_info.Gene_name');
      $data{description}  = &names_at($obj,'DB_info.Description');
      $data{species}      = &names_at($obj,'Origin.Species');

      unless( $data{description} ){ # TODO Look for description in parent CDS
        if( my $cds = &cached_at( $obj,'Visible.Corresponding_CDS' ) ){
          $data{description} = &names_at($cds, 'DB_info.DB_remark' );
        }
      }
    } # End ?Protien

    elsif( $data_class eq 'Protein_for_gene' ){
      # An attempt to get round the protein-centered memory problems
      # encountered when building the Gene dataset
      $data{homology_group} = [&cached_data_at($obj,'Visible.Homology_group')];
      $data{motif}          = [&cached_data_at($obj,'Homol.Motif_homol')];
    }

    elsif( $data_class eq 'Pseudogene' ){
      # ?Pseudogene
      $data{gene} = &names_at($obj,'Visible.Gene');
      $data{info} = "[$data{name}] product of gene $data{gene}";
    } # End ?Pseudogene

    elsif( $data_class eq 'RNAi' ){
      # ?RNAi
      # This is quite complex, as it builds an array of phenotypes data hashes
      # (including #Phenotype_info) under the 'phenotype' key.
      $data{history_name} = 
          &names_at($obj,'History_name');

      # Process experiment...
      $data{experiment_author} = 
          [ &names_at($obj,'Experiment.Author')];
      $data{experiment_laboratory} = 
          [ &names_at($obj,'Experiment.Laboratory')];
      $data{experiment_date} = 
          &names_at($obj,'Experiment.Date');
      $data{experiment_strain} = 
          &names_at($obj,'Experiment.Strain');
      my $author = ( @{$data{experiment_author}} > 1 ? 
                     $data{experiment_author}->[0] . " et.al." :
                     $data{experiment_author}->[0] );
      my( $year ) = ( $data{experiment_date} || '' ) =~ /(\d{4})/;
      my $published = join( ' ',
                            ( $author||() ),
                            ( $year  ||() ) );
      $published &&= "($published)";


      # Process Genes (should we turn this into a hash?)
      $data{inhibits_gene}   = [];
      $data{inhibits_gene_primary}   = [];
      $data{inhibits_gene_secondary} = [];
      foreach my $gene( &cached_data_at( $obj, 'Inhibits.Gene' ) ){
        # Take a copy of gene as adding rnai-specific info
        my $gene = {%{$gene}};
        # Get the rnai-primary/secondary
        my $gene_id   = $gene->{name}; 
        my $root    = &at( $obj, "Inhibits.Gene.$gene_id" );
        if( $root and $root->right =~ /primary/i ){
          $gene->{rnai_xref}    = 'primary target';
          $gene->{rnai_primary} = 1;
          push @{$data{inhibits_gene_primary}}, $gene;
        } else {
          $gene->{rnai_xref}      = 'secondary target';
          $gene->{rnai_secondary} = 1;
          push @{$data{inhibits_gene_secondary}}, $gene;
        }
        push @{$data{inhibits_gene}}, $gene;        
      }

      # Process Phenotypes...
      $data{phenotype} = _phenotype_data_with_info( $obj );

      # Process the info
      my $target = join( ', ', 
                         map{$_->{public_name} }
                         @{$data{inhibits_gene_primary}} );
      $target &&= "targets $target";
      my $phenotype = join( ', ',
                            map{$_->{primary_name}}
                            grep{$_->{PhenotypeInfo_Observed}}
                            @{$data{phenotype}} );
      $phenotype ||= 'no observed phenotype';
      #$phenotype = "causing $phenotype";
      
      $data{info} = join( ' ',
                          "[$data{name}]", 
                          ( $data{history_name} || () ),
                          ( $target || () ),
                          ( $phenotype || () ),
                          ( $published || () ),
                          );
    } # End RNAi

    elsif( $data_class eq 'Strain' ){
      # ?Strain
      $data{genotypes} = [ &names_at($obj,'Genotype') ],
      $data{info} = join( $LIST_SEPARATOR,
                          map{"[$data{name}] $_"}
                          @{$data{genotypes}} );
      $data{info} ||= "[$data{name}]";
    } # End ?Strain

    elsif( $data_class eq 'Transcript' ){
      # ?Transcript
      $data{gene} = &names_at($obj,'Visible.Gene');
      $data{cds}  = &names_at($obj,'Corresponding_CDS');
      $data{info} = "[$data{name}] product of " . 
          ($data{gene}||$data{cds}||'unknown');
    }

    elsif( $data_class eq 'Transgene' ){
      # ?Transgene
      $data{summary} = [ &names_at($obj,'Summary') ],
      $data{info}    = join( ' ', "[$data{name}]", ($data{summary}->[0]||()) );
    } # End ?Transgene

    elsif( $data_class eq 'Variation' ){
      # ?Variation
      # Similar to the RNAi objctet;
      # Gene.Molecular_change and Phenotype.Phenotype_info are built into hash

      # Process Gene, CDS, Transcript, Pseudogene
      $data{gene} 
        = _data_with_molecular_change( $obj, 'Affects.Gene' );
      $data{cds}  
        = _data_with_molecular_change( $obj, 'Affects.Predicted_CDS' );
      $data{transcript} 
        = _data_with_molecular_change( $obj, 'Affects.Transcript' );
      $data{Pseudogene}
        = _data_with_molecular_change( $obj, 'Affects.Pseudogene' );

      # Process Phenotypes...
      $data{phenotype} = _phenotype_data_with_info( $obj );
      
    }
    else{
      # Unknown object
      die("Class $data_class has no configuration for data parsing. ".
          "From: ", join( ", ", (caller(0))[1..2]) );
    }
    $ObjectDataCache->{$data_class}->{$name} = {%data};
    $ObjectDataCache_count ++;
  }
  return $ObjectDataCache->{$data_class}->{$name};
}

# Helper function to add the data within the RNAi/Variation object's
# Phenotype_info hash into the 'standard' phenotype data hash
sub _phenotype_data_with_info{
  my $obj = shift || die("Need an AceDB object!");
  my $class = $obj->class;
  my $loc; # Where in the object the phenotypes are found
  if( $class eq 'RNAi' ){ $loc='Phenotype' }
  elsif( $class eq 'Variation' ){ $loc='Description.Phenotype' }
  else{ die( "Cannot parse Phenotype_info for class $class" ) }
  
  # Get list of phenotypes, ignoring WT.
  my @phens = ( grep{ $_->{name} =~ /^WBPhenotype/
                          and $_->{name} ne 'WBPhenotype0001179' }
                &cached_data_at( $obj, $loc ) );
  my @phenotypes_data; # The array that will be returned

  # Process each Phenotype
  foreach my $phen( @phens ){
    my %phen_data = %$phen; # Dereference as adding per-RNAi/Variation info
    my $phen_name = $phen_data{name};

    # Assume Phenotype is observed
    $phen_data{"PhenotypeInfo_Observed"} = 1;

    # Process the #Phenotype_info tags...
    foreach my $tag( &at( $obj, "$loc.$phen_name" ) ){
      
      # For the 'Not' tag, unset the 'Observed' value
      if( $tag eq 'Not' ){
        $phen_data{"PhenotypeInfo_Observed"} = undef;
      }
      
      # Quantity is a special case, two integer values...
      if( $tag eq 'Quantity' ){ # Only allow unique quantity
        my $root = $obj->at("$loc.$phen_name.$tag");
        my $int_a = $root->right;
        my $int_b = $root->right(2);
        if( defined( $int_a ) ){
          $phen_data{"PhenotypeInfo_${tag}_a"} = "$int_a";
        }
        if( defined( $int_b ) ){
          $phen_data{"PhenotypeInfo_${tag}_b"} = "$int_b";
        }
      }
      
      # Penetrance has multiple levels, inc one with two int values...
      if( $tag eq 'Penetrance' ){ 
        foreach my $subtag ( $obj->at("$loc.$phen_name.$tag" ) ){
          if( $subtag eq 'Range' ){ 
            my $root = $obj->at("$loc.$phen_name.$tag.$subtag");
            my $int_a = $root->right;
            my $int_b = $root->right(2) || $int_a;
            if( defined( $int_a ) ){
              $phen_data{"PhenotypeInfo_${tag}_${subtag}_a"} = "$int_a";
            }
            if( defined( $int_b ) ){
              $phen_data{"PhenotypeInfo_${tag}_${subtag}_b"} = "$int_b";
            }
          }
          $phen_data{"PhenotypeInfo_${tag}_${subtag}"} 
          = &dmlist_at($obj,"$loc.$phen_name.$tag.$subtag");
          $phen_data{"PhenotypeInfo_${tag}_${subtag}"} ||= 1;
        }
      }
      
      # Temperature_sensitive has multiple levels...
      if( $tag eq 'Temperature_sensitive' ){
        foreach my $subtag( $obj->at("$loc.$phen_name.$tag" ) ) {
          $phen_data{"PhenotypeInfo_${tag}_${subtag}"} 
          = &dmlist_at($obj,"$loc.$phen_name.$tag.$subtag");
          $phen_data{"PhenotypeInfo_${tag}_${subtag}"} ||= 1;
        }
      }
      
      # Default case, single level...
      $phen_data{"PhenotypeInfo_$tag"} 
      = &dmlist_at( $obj, "$loc.$phen_name.$tag" );
      $phen_data{"PhenotypeInfo_$tag"} ||= 1;
    }
    
    # Process the 'Penetrance' tag
    if( $phen_data{"PhenotypeInfo_Penetrance_Complete"} ){
      $phen_data{"PhenotypeInfo_Penetrance"} = 'Complete';
    } elsif( $phen_data{"PhenotypeInfo_Penetrance_High"} ){
      $phen_data{"PhenotypeInfo_Penetrance"} = 'High';
    } elsif( $phen_data{"PhenotypeInfo_Penetrance_Low"} ){
      $phen_data{"PhenotypeInfo_Penetrance"} = 'Low';
    } elsif( $phen_data{"PhenotypeInfo_Penetrance_Incomplete"} ){
      $phen_data{"PhenotypeInfo_Penetrance"} = 'Incomplete';
    } elsif( $phen_data{"PhenotypeInfo_Not"} ){
      $phen_data{"PhenotypeInfo_Penetrance"} = 'None';
    } else {
      $phen_data{"PhenotypeInfo_Penetrance"} = 'Unknown';
    }
    
    # Process the 'Dominance' tag
    if( $phen_data{"PhenotypeInfo_Dominant"} ){
      $phen_data{"PhenotypeInfo_Dominance"} = 'Dominant';
    } elsif( $phen_data{"PhenotypeInfo_Semi_dominant"} ){
      $phen_data{"PhenotypeInfo_Dominance"} = 'Semi_dominant';
    } elsif( $phen_data{"PhenotypeInfo_Recessive"} ){
      $phen_data{"PhenotypeInfo_Dominance"} = 'Recessive';
    } elsif( $phen_data{"PhenotypeInfo_Haplo_insufficient"} ){
      $phen_data{"PhenotypeInfo_Dominance"} = 'Haplo_insifficient';
    } elsif( $phen_data{"PhenotypeInfo_Not"} ){
      $phen_data{"PhenotypeInfo_Dominance"} = 'None';
    } else {
      $phen_data{"PhenotypeInfo_Dominance"} = 'Unknown';
    } 
    
    # Process the 'Allele' tag
    if( $phen_data{"PhenotypeInfo_Loss_of_function"} ){
      $phen_data{"PhenotypeInfo_Allele"} = 'Loss_of_function';
    } elsif( $phen_data{"PhenotypeInfo_Gain_of_function"} ){
      $phen_data{"PhenotypeInfo_Allele"} = 'Gain_of_function';
    } elsif( $phen_data{"PhenotypeInfo_Other_allele_type"} ){
      $phen_data{"PhenotypeInfo_Allele"} = 'Other';
    } elsif( $phen_data{"PhenotypeInfo_Not"} ){
      $phen_data{"PhenotypeInfo_Allele"} = 'None';
    } else {
      $phen_data{"PhenotypeInfo_Allele"} = 'Unknown';
    }
    
    # Process the 'Inheretance' tag
    if( $phen_data{"PhenotypeInfo_Maternal"} ){
      $phen_data{"PhenotypeInfo_Inheretance"} = 'Maternal';
    } elsif( $phen_data{"PhenotypeInfo_Paternal"} ){
      $phen_data{"PhenotypeInfo_Inheretance"} = 'Paternal';
    } elsif( $phen_data{"PhenotypeInfo_Not"} ){
      $phen_data{"PhenotypeInfo_Inheretance"} = 'None';
    } else {
      $phen_data{"PhenotypeInfo_Inheretance"} = 'Unknown';
    }
    
    # Process the 'Temperature_sensitive' tag
    if( $phen_data{"PhenotypeInfo_Temperature_sensitive_Heat_sensitive"} ){
      $phen_data{"PhenotypeInfo_Temperature_sensitive"} = 'Heat_sensitive';
    } elsif( $phen_data{"PhenotypeInfo_Temperature_sensitive_Cold_sensitive"}){
      $phen_data{"PhenotypeInfo_Temperature_sensitive"} = 'Cold_sensitive';
    } elsif( $phen_data{"PhenotypeInfo_Not"} ){
      $phen_data{"PhenotypeInfo_Temperature_sensitive"} = 'None';
    } else {
      $phen_data{"PhenotypeInfo_Temperature_sensitive"} = 'Unknown';
    }
  
    # Tweak the info value
    $phen_data{"info"} = 
        ( $phen_data{"PhenotypeInfo_Not"} ? 'Not Observed: ' : 'Observed: ' )
        . $phen_data{"info"};
      
    push @phenotypes_data, {%phen_data};
  }
  
  return [@phenotypes_data];
}

# Helper function to add the data within the RNAi/Variation object's
# Molecular_change hash into the 'standard' gene/cds/transcript data hash
sub _data_with_molecular_change{
  my $obj = shift;
  my $loc = shift; # Object location where Gene/CDS/Transcript is found
  my @molecules = &cached_data_at( $obj, $loc ); # Could be Gene/CDS/Transcr
  my @molecules_data = ();
  foreach my $molecule( @molecules ){
    my %mol_data = %$molecule; # Dereference as adding per-variation info
    my $mol_name = $mol_data{name};
    $mol_name =~ s/\./\\\./g;
    # Process the #Molecular_change tags...
    foreach my $tag( &at( $obj, "$loc.$mol_name" ) ){
      if( $tag eq 'Missense' ){
        $mol_data{"MolecularChange_$tag"}
        = sprintf( '%s (%s)',  
                   ( $tag->right || '?' ), ( $tag->right(2) || '? to ?' ) );
      }
      else{
        # Default case, single level...
        $mol_data{"MolecularChange_$tag"} 
        = &dmlist_at( $obj, "$loc.$mol_name.$tag" );
      }
      $mol_data{"MolecularChange_$tag"} ||= 1;      
    }
    push @molecules_data, {%mol_data};
  }
  #warn Dumper( [@molecules_data] );
  return [@molecules_data];
}

#----------------------------------------------------------------------
# Cache accessor. Returns the hashref(s) found in the cache for the
# object's schema location.
# Arg[0]: The AceDB object to interrogate. 
# Arg[1]: The location of the 'child' objects in the 'parent' AceDB schema
#
sub cached_data_at{
  my $obj = shift || return wantarray ? () : ''; # Handle empty objects
  my @pos = @_;
  @pos || die( "Need an schema location to retrieve for $obj" );
####test by RF_011211
  ##print "WormMartTool_inside_cached_data_at_before_calling_object_data_cache_L959\n";
  ##print "WormMartTool_inside_cached_data_at_L962_obj=$obj==pos=@pos\n";
  ##my @test_map_arr_1 = map{};
 ## print "WormMartTool_inside_cached_data_at_L964_map_arr_1=@test_map_arr_1\n";
  ##my @test_map_arr_2 = map{&at_test($obj,$_)} @pos;
  ##print "WormMartTool_inside_cached_data_at_L966_map_arr_2=@test_map_arr_2\n";
###############################################
  ##my @values = map{&object_data_cache($_)} map{&at($obj,$_)} @pos;
####test by RF_011211
  ##print "WormMartTool_inside_cached_data_at_after_calling_object_data_cache_L967\n";
###############################################
  my @values = map{&object_data_cache($_)} map{&at($obj,$_)} @pos;
  #scalar( @values ) or @values = ({});
  return wantarray ? @values : $values[0];
}

sub recursive_cached_data_at{
  my $obj = shift || return wantarray ? () : ''; # Handle empty objects
  my @pos = @_;
  my @values = map{ &object_data_cache($_) } map{&recursive_at($obj,$_)} @pos;
  return wantarray ? @values : $values[0];
}

#----------------------------------------------------------------------
# Inserts a VAL entry into a table for a given schema location
# [arg0]: The location of the value in the AceDB schema
# [arg1]: The mart schema field name
sub insert_val{
  my $class = shift || 'Text';
  my $pos   = shift || die( "Need a schema location" );
  my $field = shift || die( "Need a MartTable column name" );
  my $useparent = shift || 0; # Use the main table object, not the dimension's
  
  $field || die( "Need a field name" );
  
  my %vals;
  
  $vals{DEFAULT} = 
  { "VAL_${field}" => sub{ 
    my $obj = $useparent ? $_[1] : $_[0];
    &dmlist_at($obj,$pos) } 
  };
  
  $vals{Int} =
  { "VAL_${field}" => sub{ &names_at($_[0],$pos) },
    "COL_${field}" => 'INT(10) DEFAULT NULL', };

  $vals{DateType} = 
  { "VAL_${field}" => sub{ &ace2isodate(&at($_[0],$pos)) },
    "COL_${field}" => 'DATE DEFAULT NULL', };

  $vals{'#Evidence'} = 
  {
    "VAL_${field}" => sub{ 
      my $obj = $useparent ? $_[1] : $_[0];
      unless( UNIVERSAL::isa( $obj, "Ace::Object") ){
        return wantarray ? () : '';
      }
      
      if( $useparent ){
        # Use the parent object. The dimension object will be used to
        # get the 'correct' evidence. Maybe another generalised 'at' call?
        my $obj = $_[1] || return wantarray ? () : '';
        my $dmobj = $_[0];
        my $dmid;
        if( UNIVERSAL::isa( $dmobj, "Ace::Object") ){ $dmid = "$dmobj" }
        elsif( ref( $dmobj ) eq 'HASH' ){ $dmid = $dmobj->{name} }
        else{ $dmid = $dmobj }

        my @calls = ('');
        # build up the set of 'at' and 'col' calls needed to get vals for pos 
        my $skip_next = 0;
        foreach my $tag( split('\.', $pos ) ){
          
          if( $skip_next ){ $skip_next = ''; next } # Flow control
          if( $tag eq 'UNIQUE' ){ next } # Skip
          if( $tag eq 'XREF' ){ $skip_next ++; next } # Skip + XREF points to

          if( $tag =~ m/^(\?)|(\#)|(Text)|(Int)|(Float)|(DateTime)/ ){
            # Traverse this tag with a 'col' call;
            push( @calls, '' );
          }
          else{
            # Traverse this tag with an 'at' call
            $calls[-1] = join( ".", ($calls[-1]||()), "$tag" );
          }
        }
        if( ! $calls[-1] ){ pop @calls } # Remove the last if it is empty.
        #warn( "==> $dmid" );
        my @vals = $obj;
        my $first ++;
        foreach my $tag( @calls ){
          unless( $first ){ 
#             foreach my $val( @vals ){
              #warn join( ", ", $val->col );
#             }
            @vals = map{ $_->col } @vals;
          }
          if( $first ){ $first = 0 }
          if( $tag ){ 
#             foreach my $val( @vals ){
              #warn join( ", ", $val->at($tag) );
#             }
            @vals = map{ $_->at($tag) } @vals }
        }


#        my( $first, $second ) = split( /\.(\?\w+\.)/, $pos );
#        warn( "**> $first $second \n" );
#        foreach my $tag( &at($obj, $first) ){
#          if( $tag eq $dmid ){
#            #$tag = $tag->right;
#            foreach my $tag2( &at($tag, $second) ){
#              warn( "  > ", $tag2->col );
#            }
#          }
#        }
      }
      

      my @vals;
      foreach my $tag( &at($obj, $pos) ){
        foreach my $next_tag( $tag->col ){
          if( $next_tag->class eq 'txt' ){
            push @vals, uc("$tag").": $next_tag";
          } else {
            my $data = &object_data_cache( $next_tag );
            push @vals, uc("$tag").": $data->{info}";
          }
        }
      }
      return join( $LIST_SEPARATOR, @vals );
    }
  };

  return( %{ $vals{$class} || $vals{DEFAULT} } );
}

# Takes AceDB DateTime value, and converts into MySQL compat ISO date.
sub ace2isodate{
  my $acedate = shift || return;
  my ($y,$m,$d) = Date::Calc::Decode_Date_EU($acedate);
  $y||=0; $m||=0; $d||=0;
  return sprintf( "%4.4d-%2.2d-%2.2d", $y,$m,$d );
}
#----------------------------------------------------------------------
# Inserts a 'simple' dimension for a given schema location.
# Used where a fetch for the child object is not required.
# Includes a dmlist field in the main table.
# [arg0]: The location of the 'child' objects in the 'parent' AceDB schema
# [arg1]: The mart schema table name prefix ('dataset')
# [arg2]: [optional] The mart column name prefix (defaults to arg0)
#
sub insert_simple_dimension{
  my $pos = shift;
  my $dataset = shift;
  my $col = shift;
  $col ||= lc($pos);
  $col =~ s/\./_/g; # Replace '.' in col name with '_'
  return
  (
   "TBL_${dataset}__${col}__dm" => {
     OBJECTS      => sub{ [&names_at($_[0],$pos)] },
     "VAL_${col}" => sub{ ref($_[0]) ? '' : $_[0] },
     IDX => [$col],
   },
   "VAL_${col}_dmlist" => sub{ &dmlist_at($_[0],$pos) },
  );

}

# Similar to insert simple dimension, but also stores alignment coords
# in dimension table. Used for child objs of Sequence dataset
sub insert_coord_dimension{
  my $pos = shift;
  my $dataset = shift;
  my $col = shift;
  $col ||= lc($pos);
  $col =~ s/\./_/g; # Replace '.' in col name with '_'
  return
  (
   "TBL_${dataset}__${col}__dm" => {
     OBJECTS      => sub{ 
       my @hashrefs;
       foreach my $t( $_[0]->at($pos) ){
         push @hashrefs, {
           $col => "$t",
           start => $t->right(1),
           end   => $t->right(2),
         };
       }
       return [@hashrefs];
     },
     "VAL_${col}" => &cached_val_sub($col),
     VAL_start => &cached_val_sub('start'),
     VAL_end   => &cached_val_sub('end'),
     IDX => [$col],
   },
   "VAL_${col}_dmlist" => sub{ &dmlist_at($_[0],$pos) },
  );
}

#----------------------------------------------------------------------
# Inserts a pre-configured dimension for objects of a given class.
# This helps to get dimensions consistent across object loader modules
# [arg0]: The class of AceDB object, e.g. Gene, RNAi
# [arg1]: The location of the 'child' objects in the 'parent' AceDB schema
# [arg2]: The mart schema table name prefix ('dataset')
# [arg3]: [optional] The mart column name prefix (defaults to arg0) 
#
sub insert_dimension{
  my $class = shift;
  my $pos = shift;
  my $dataset = shift;
  my $col = shift;

  $class =~ s/^\?//; # Remove leading '?'
  $col ||= lc($class);
  $col =~ s/^\?//;

  if(0){}
  elsif( $class eq 'Anatomy_term' ){
    # ?Anatomy_term
    return
    (
     "TBL_${dataset}__${col}__dm" => {
       OBJECTS           => sub{ return [&cached_data_at($_[0],$pos)] },
       VAL_anatomy_term  => &cached_val_sub('name'),
       VAL_term          => &cached_val_sub('term'),
       VAL_info          => &cached_val_sub('info'),
       IDX               => ['anatomy_term','term'],
     },
     "VAL_${col}_dmlist" => sub{ 
       &dmlist_at($_[0],$pos);
     },
     "VAL_${col}_dminfo" => sub{ &dminfo_at( $_[0],$pos ) },
    );       
  } # End ?Anatomy_term
  elsif( $class eq 'Antibody' ){
    # ?Antibody
    return
    (
     "TBL_${dataset}__${col}__dm" => {
       OBJECTS => sub{ [&cached_data_at($_[0],$pos)] },
       VAL_antibody  => &cached_val_sub('name'),
       VAL_clonality => &cached_val_sub('clonality'),
       VAL_antigen   => &cached_val_sub('antigen'),
       VAL_animal    => &cached_val_sub('animal'),
       VAL_summary   => &cached_val_sub('summary'),
       VAL_info      => &cached_val_sub('info'),
       IDX => ['antibody'],
     },
     "VAL_${col}_dmlist" => sub{ 
       &dmlist_at($_[0],$pos);
     },
     "VAL_${col}_dminfo" => sub{ &dminfo_at( $_[0],$pos ) },
    );
  } # End ?Antibody

  elsif( $class eq 'Cell' ){
    # ?Cell
    return
    (
     "TBL_${dataset}__${col}__dm" => {
       OBJECTS           => sub{ [&cached_data_at($_[0],$pos)] },
       VAL_cell          => &cached_val_sub('name'),
       VAL_brief_id      => &cached_val_sub('brief_id'),
       VAL_cell_group    => &cached_val_sub('cell_group'),
       VAL_anatomy_term  => &cached_val_sub('anatomy_term'),
       VAL_info          => &cached_val_sub('info'),
       IDX               => ['cell'],
     },
     "VAL_${col}_dmlist" => sub{ 
       &dmlist_at($_[0],$pos);
     },
     "VAL_${col}_dminfo" => sub{ &dminfo_at( $_[0],$pos ) },
    );
  } # End ?Cell

  elsif( $class eq 'CDS' ){
    # ?CDS
    return
    (
     "TBL_${dataset}__${col}__dm" => {
       OBJECTS  => sub{ [&cached_data_at($_[0],$pos)] },
       VAL_cds  => &cached_val_sub('name'),
       VAL_gene => &cached_val_sub('gene'),
       VAL_info => &cached_val_sub('info'),
       IDX      => ['cds']
     },
     "VAL_${col}_dmlist" => sub{ 
       &dmlist_at($_[0],$pos);
     },
     "VAL_${col}_dminfo" => sub{ &dminfo_at( $_[0],$pos ) },
    );
  } # End ?CDS

  elsif( $class eq 'Expr_pattern' ){
    # ?Expr_pattern
    return
    (
     "TBL_${dataset}__${col}__dm" => {
       OBJECTS => sub{ [&cached_data_at($_[0],$pos)] },
       VAL_expr_pattern => &cached_val_sub('name'),
       VAL_pattern      => &cached_val_sub('patterns'),
       VAL_subcellular_localization 
                        => &cached_val_sub('subcellular_localizations'),
       VAL_info         => &cached_val_sub('info'),
       IDX => ['expr_pattern'],
     },
     "VAL_${col}_dmlist" => sub{ 
       &dmlist_at($_[0],$pos);
     },
     "VAL_${col}_dminfo" => sub{ &dminfo_at( $_[0],$pos ) },
    );
  } # End ?Expr_pattern

  elsif( $class eq 'Gene' or
      $class eq '?Gene' ){
    # ?Gene
    return
    (
     "TBL_${dataset}__${col}__dm" => {
       OBJECTS => sub{ [&cached_data_at($_[0],$pos)] },
       VAL_gene                => &cached_val_sub('name'),
       VAL_sequence_name       => &cached_val_sub('sequence_name'),
       VAL_public_name         => &cached_val_sub('public_name'),
       VAL_cgc_name            => &cached_val_sub('cgc_name'),
       VAL_gene_class          => &cached_val_sub('gene_class'),
       VAL_class_description   => &cached_val_sub('class_description'),
       VAL_concise_description => &cached_val_sub('concise_description'),
       VAL_species             => &cached_val_sub('species'),
       VAL_evidence            => sub{&evidence_at( $_[0], $_[1], $pos )},
       VAL_info                => &cached_val_sub('info'),
       IDX => ['gene','sequence_name','public_name','cgc_name','gene_class'],
     },
     "VAL_${col}_dmlist" => sub{ 
       &dmlist_at($_[0],$pos);
     },
     "VAL_${col}_dminfo" => sub{ &dminfo_at( $_[0],$pos ) },

    );
  } # End ?Gene

  elsif( $class eq 'Gene_regulation' or
         $class eq '?Gene_regulation' ){
    # ?Gene_regulation 
    return
    (
     "TBL_${dataset}__${col}__dm" => {
       OBJECTS => sub{ [&cached_data_at($_[0],$pos)] },
       VAL_gene_regulation      => &cached_val_sub('name'),
       VAL_info                 => &cached_val_sub('info'),
       VAL_summary              => &cached_val_sub('summary'),
       VAL_method               => &cached_val_sub('method'),
       VAL_method_info          => &cached_val_sub('method_info'),
       VAL_trans_regulator_gene => &cached_val_sub('trans_regulator_gene'),
       VAL_trans_regulator_seq  => &cached_val_sub('trans_regulator_seq'),
       VAL_cis_regulator_seq    => &cached_val_sub('cis_regulator_seq'),
       VAL_other_regulator      => &cached_val_sub('other_regulator'),
       VAL_target_info          => &cached_val_sub('target_info'),
       VAL_trans_regulated_gene => &cached_val_sub('trans_regulated_gene'),
       VAL_trans_regulated_seq  => &cached_val_sub('trans_regulated_seq'),
       VAL_cis_regulated_seq    => &cached_val_sub('cis_regulated_seq'),
       VAL_other_regulated      => &cached_val_sub('other_regulated'),
       VAL_result               => &cached_val_sub('result'),
       VAL_result_info          => &cached_val_sub('result_info'),
       VAL_trans_regulator_gene_name => 
           &cached_val_sub('trans_regulator_gene_name'),
       VAL_trans_regulated_gene_name =>
           &cached_val_sub('trans_regulated_gene_name'),
       IDX => ['gene_regulation'],
     },
     "VAL_${col}_dmlist" => sub{ 
       &dmlist_at($_[0],$pos);
     },
     "VAL_${col}_dminfo" => sub{ &dminfo_at( $_[0],$pos ) },
     );
  } # End ?Gene_regulation

  elsif( $class eq 'GO_term' ){
    # ?GO_Term
    return
    (
     "TBL_${dataset}__${col}__dm" => {
       OBJECTS => sub{ [&cached_data_at($_[0],$pos)] },
       VAL_go_term => &cached_val_sub('name'),
       VAL_term    => &cached_val_sub('term'),
       VAL_type    => &cached_val_sub('type'),
       VAL_info    => &cached_val_sub('info'),
       IDX => ['go_term','type'],
     },
     "VAL_${col}_dmlist" => sub{ 
       &dmlist_at($_[0],$pos);
     },
     "VAL_${col}_dminfo" => sub{ &dminfo_at( $_[0],$pos ) },
    );
  } # End ?GO_Term

  elsif( $class eq 'Homology_group'  ){
    # ?Homology_group
    return
    (
     "TBL_${dataset}__${col}__dm" => {
       OBJECTS            => sub{ [&cached_data_at($_[0],$pos)] },
       VAL_homology_group => &cached_val_sub('name'),
       VAL_group_type     => &cached_val_sub('group_type'),
       VAL_title          => &cached_val_sub('title'),
       VAL_info           => &cached_val_sub('info'),
       IDX => ['homology_group'],
     },
     "VAL_${col}_dmlist" => sub{ 
       &dmlist_at($_[0],$pos);
     },
     "VAL_${col}_dminfo" => sub{ &dminfo_at( $_[0],$pos ) },

     # Handle each group separatly
     # COG
     "TBL_${dataset}__${col}_cog_dm" => { 
       OBJECTS => sub{
         [ grep{ $_->{group_type} eq 'COG' } &cached_data_at($_[0],$pos) ];
       },
       VAL_homology_group => &cached_val_sub('name'),
       VAL_title          => &cached_val_sub('title'),
       VAL_cog_type       => &cached_val_sub('cog_type'),
       VAL_cog_code       => &cached_val_sub('cog_code'),
       ##VAL_hops_group     => &cached_val_sub('hops_group'),  ##added by RF_011211
       ##VAL_rio_group      => &cached_val_sub('rio_group'),  ##added by RF_011211
       ##VAL_inparanoid_group => &cached_val_sub('inparanoid_group'),  ##added by RF_011211
       ##VAL_orthomcl_group => &cached_val_sub ('orthomcl_group'),  ##added by RF_011211
     },
     "VAL_${col}_cog_dmlist" => sub{ 
       return join( $LIST_SEPARATOR,
                    map{ $_->{name} || '' }
                    grep{ $_->{group_type} eq 'COG'}
                    &cached_data_at($_[0],$pos) );
     },
     "VAL_${col}_cog_dminfo" => sub{ 
       return join( $LIST_SEPARATOR,
                    map{ $_->{info} || '' }
                    grep{ $_->{group_type} eq 'COG'}
                    &cached_data_at($_[0],$pos) );
     },

     # HOPS
     "TBL_${dataset}__${col}_hops_dm" => {
       OBJECTS => sub{
         [ grep{ $_->{group_type} eq 'HOPS_group' } 
           &cached_data_at($_[0],$pos) ];
       },
       VAL_homology_group => &cached_val_sub('name'),
       VAL_title          => &cached_val_sub('title'),       
     },
     "VAL_${col}_hops_dmlist" => sub{ 
       return join( $LIST_SEPARATOR,
                    map{ $_->{name} || '' }
                    grep{ $_->{group_type} eq 'HOPS_group'}
                    &cached_data_at($_[0],$pos) );
     },
     "VAL_${col}_hops_dminfo" => sub{ 
       return join( $LIST_SEPARATOR,
                    map{ $_->{info} || '' }
                    grep{ $_->{group_type} eq 'HOPS_group'}
                    &cached_data_at($_[0],$pos) );
     },

     # RIO
     "TBL_${dataset}__${col}_rio_dm" => {
       OBJECTS => sub{
         [ grep{ $_->{group_type} eq 'RIO_group' } 
           &cached_data_at($_[0],$pos) ];
       },
       VAL_homology_group => &cached_val_sub('name'),
       VAL_title          => &cached_val_sub('title'),       
     },
     "VAL_${col}_rio_dmlist" => sub{ 
       return join( $LIST_SEPARATOR,
                    map{ $_->{name} || '' }
                    grep{ $_->{group_type} eq 'RIO_group'}
                    &cached_data_at($_[0],$pos) );
     },
     "VAL_${col}_rio_dminfo" => sub{ 
       return join( $LIST_SEPARATOR,
                    map{ $_->{info} || '' }
                    grep{ $_->{group_type} eq 'RIO_group'}
                    &cached_data_at($_[0],$pos) );
     },

     # InParanoid
     "TBL_${dataset}__${col}_inparanoid_dm" => {
       OBJECTS => sub{
         [ grep{ $_->{group_type} eq 'InParanoid_group' } 
           &cached_data_at($_[0],$pos) ];
       },
       VAL_homology_group => &cached_val_sub('name'),
       VAL_title          => &cached_val_sub('title'),       
     },
     "VAL_${col}_inparanoid_dmlist" => sub{ 
       return join( $LIST_SEPARATOR,
                    map{ $_->{name} || '' }
                    grep{ $_->{group_type} eq 'InParanoid_group'}
                    &cached_data_at($_[0],$pos) );
     },
     "VAL_${col}_inparanoid_dminfo" => sub{ 
       return join( $LIST_SEPARATOR,
                    map{ $_->{info} || '' }
                    grep{ $_->{group_type} eq 'InParanoid_group'}
                    &cached_data_at($_[0],$pos) );
     },
    );           
  } # End ?Homology_group

  elsif( $class eq 'Laboratory' ){
    # ?Laboratory
    return
    (
     "TBL_${dataset}__${col}__dm" => {
       OBJECTS => sub{ [ &cached_data_at($_[0],$pos) ] },
       VAL_laboratory => &cached_val_sub('name'),
       VAL_mail       => &cached_val_sub('mails'),
       VAL_info       => &cached_val_sub('info'),
       IDX => ['laboratory','info'],
     },
     "VAL_${col}_dmlist" => sub{ 
       &dmlist_at($_[0],$pos);
     },
     "VAL_${col}_dminfo" => sub{ &dminfo_at( $_[0],$pos ) },
    );
  } # End ?Laboratory

  elsif( $class eq 'Motif' ){
    # ?Motif
    return
    (
     "TBL_${dataset}__${col}__dm" => {
       OBJECTS => sub{ [ &cached_data_at($_[0],$pos) ] },
       VAL_motif          => &cached_val_sub('name'),
       VAL_db             => &cached_val_sub('database'),
       VAL_title          => &cached_val_sub('title'),
       VAL_accession      => &cached_val_sub('accession'),
       VAL_info           => &cached_val_sub('info'),
       IDX => ['motif','db','accession'],
     },
      "TBL_${dataset}__${col}_namelist__dm" => { 
        # Used for searching domain - by WB ID and native id
        OBJECTS => sub{
          return [ map{ $_->{name},$_->{accession} } 
                   &cached_data_at($_[0],$pos ) ];
        },
        VAL_name  => sub{ ref($_[0]) ? '' : $_[0] },
        IDX => ['name'],
      },
     "VAL_${col}_dmlist" => sub{ 
       &dmlist_at($_[0],$pos);
     },
     "VAL_${col}_dminfo" => sub{ &dminfo_at( $_[0],$pos ) },
    );
  } # End ?Motif

  elsif( $class eq 'Oligo_set' ){
    # ?Oligo_set
    return
    (
     "TBL_${dataset}__${col}__dm" => {
       OBJECTS           => sub{ [ &cached_data_at($_[0],$pos) ] },
       VAL_oligo_set     => &cached_val_sub('name'),
       VAL_remark        => &cached_val_sub('remark'),
       IDX               => ['oligo_set'],
     },
     "VAL_${col}_dmlist" => sub{ 
       &dmlist_at($_[0],$pos);
     },
    );
  } # End ?Oligo_set

  elsif( $class eq 'Operon' ){
    # ?Operon
    return
    (
     "TBL_${dataset}__${col}__dm" => {
       OBJECTS           => sub{ [ &cached_data_at($_[0],$pos) ] },
       VAL_operon        => &cached_val_sub('name'),
       VAL_smap_sequence => &cached_val_sub('smap_sequence'),
       VAL_smap_start    => &cached_val_sub('smap_start'),
       VAL_smap_end      => &cached_val_sub('smap_end'),
       VAL_method        => &cached_val_sub('method'),
       IDX               => ['operon'],
     },
     "VAL_${col}_dmlist" => sub{ 
       &dmlist_at($_[0],$pos);
     },
    );
  } # End ?Operon

  elsif( $class eq 'Paper' ){
    # ?Paper
    return
    (
     "TBL_${dataset}__${col}__dm" => {
       OBJECTS => sub{ [ &cached_data_at($_[0],$pos) ] },
       VAL_paper          => &cached_val_sub('name'),
       VAL_cgc_name       => &cached_val_sub('cgc_name'),
       VAL_pmid           => &cached_val_sub('pmid'),
       VAL_brief_citation => &cached_val_sub('brief_citation'),
       VAL_info           => &cached_val_sub('info'),
       IDX => ['paper','cgc_name','pmid'],
     },
     "VAL_${col}_dmlist" => sub{ 
       &dmlist_at($_[0],$pos);
     },
     "VAL_${col}_dminfo" => sub{ &dminfo_at( $_[0],$pos ) },
    );
  } # End ?Paper

  elsif( $class eq 'Person' ){
    # ?Person
    return
    (
     "TBL_${dataset}__${col}__dm" => {
       OBJECTS           => sub{ [ &cached_data_at($_[0],$pos) ] },
       VAL_person        => &cached_val_sub('name'),
       VAL_standard_name => &cached_val_sub('standard_name'),
       VAL_first_name    => &cached_val_sub('first_name'),
       VAL_last_name     => &cached_val_sub('last_name'),
       VAL_info          => &cached_val_sub('info'),
       IDX               => ['person'],
     },
     "VAL_${col}_dmlist" => sub{ 
       &dmlist_at($_[0],$pos);
     },
     "VAL_${col}_dminfo" => sub{
       &dminfo_at($_[0],$pos);
     },
     
    );
  } # End ?Person

  elsif( $class eq 'Phenotype' ){
    # ?Phenotype
    return
    (
     "TBL_${dataset}__${col}__dm" => {
       OBJECTS => sub{ [ &cached_data_at($_[0],$pos) ] },
       VAL_phenotype    => &cached_val_sub('name'),
       VAL_primary_name => &cached_val_sub('primary_name'),
       VAL_short_name   => &cached_val_sub('short_name'),
       VAL_label        => &cached_val_sub('label'),
       VAL_description  => &cached_val_sub('description'),
       VAL_info         => &cached_val_sub('info'),
       IXD => ['phenotype','primary_name','short_name'],
     },
     "VAL_${col}_dmlist" => sub{ 
       &dmlist_at($_[0],$pos);
     },
     "VAL_${col}_dminfo" => sub{ &dminfo_at( $_[0],$pos ) },
    );
  } # End ?Phenotype

  if( $class eq 'Phenotype_info' ){
    # Pseudo-class to deal with extended phenotype (i.e. Phenotype_info)
    # Used by RNAi and Variation classes
    
    my %dm_values;
    foreach my $tag qw( Paper_evidence
                        Person_evidence
                        Curator_confirmed
                        Remark
                        Quantity_description
                        Quantity
                        Quantity_a
                        Quantity_b
                        Observed
                        Not
                        Penetrance
                        Penetrance_Incomplete
                        Penetrance_Low
                        Penetrance_High
                        Penetrance_Complete
                        Penetrance_Range
                        Penetrance_Range_a
                        Penetrance_Range_b
                        Recessive
                        Semi_dominant
                        Dominant
                        Haplo_insufficient
                        Loss_of_function
                        Gain_of_function
                        Other_allele_type
                        Temperature_sensitive
                        Temperature_sensitive_Heat_sensitive
                        Temperature_sensitive_Cold_sensitive
                        Maternal
                        Paternal
                        Phenotype_assay ){
      my $key = 'PhenotypeInfo_'.$tag;
      $dm_values{"VAL_$key"} = &cached_val_sub($key);
    }
    
    return
        (
         "TBL_${dataset}__${col}__dm" => {
           OBJECTS => sub{ 
             my $data = &object_data_cache( $_[0] );
             return [ @{$data->{phenotype}} ];
           },
           VAL_phenotype    => &cached_val_sub('name'),
           VAL_primary_name => &cached_val_sub('primary_name'),
           VAL_short_name   => &cached_val_sub('short_name'),
           VAL_label        => &cached_val_sub('label'),
           VAL_description  => &cached_val_sub('description'),
           VAL_info         => &cached_val_sub('info'),
           %dm_values,
           IXD => ['phenotype','primary_name','short_name'],
         },
         "VAL_${col}_dmlist" => sub{
           my $data = &object_data_cache( $_[0] );
           return join( $LIST_SEPARATOR, map{$_->{name}} @{$data->{phenotype}} );
         },
         "VAL_${col}_dminfo" => sub{ 
           my $data = &object_data_cache( $_[0] );
           return join( $LIST_SEPARATOR, map{$_->{info}} @{$data->{phenotype}} );
         },
         "VAL_${col}_observed_count" => sub{
           my $data = &object_data_cache( $_[0] );
           return scalar( grep{ ! $_->{PhenotypeInfo_Not} }
                          @{$data->{phenotype}} ) || undef;
         },
         "VAL_${col}_unobserved_count" => sub{
           my $data = &object_data_cache( $_[0] );
           return scalar( grep{ $_->{PhenotypeInfo_Not} }
                          @{$data->{phenotype}} ) || undef;
         },
         "VAL_${col}_observed_dmlist" => sub{
           my $data = &object_data_cache( $_[0] );
           return join( $LIST_SEPARATOR,
                        map{ $_->{name} } grep{ ! $_->{PhenotypeInfo_Not} }
                        @{$data->{phenotype}} );
         },
         "VAL_${col}_unobserved_dmlist" => sub{
           my $data = &object_data_cache( $_[0] );
           return join( $LIST_SEPARATOR,
                        map{ $_->{name} } grep{ $_->{PhenotypeInfo_Not} }
                        @{$data->{phenotype}} );
         },
         "VAL_${col}_observed_dminfo" => sub{
           my $data = &object_data_cache( $_[0] );
           return join( $LIST_SEPARATOR,
                        map{ $_->{info} } grep{ ! $_->{PhenotypeInfo_Not} }
                        @{$data->{phenotype}} );
         },
         "VAL_${col}_unobserved_dminfo" => sub{
           my $data = &object_data_cache( $_[0] );
           return join( $LIST_SEPARATOR,
                        map{ $_->{info} } grep{ $_->{PhenotypeInfo_Not} }
                        @{$data->{phenotype}} );
         },
         );
  }

  if( $class eq 'RNAi' ){
    # ?RNAi
    return
    (
     "VAL_${col}_dmlist" => sub{ 
       &dmlist_at($_[0],$pos);
     },
     "VAL_${col}_name_dmlist" => sub{
       my $dataset_object = $_[0];
       if( $dataset_object->class eq 'Phenotype' and 
           ( $dataset_object->name eq 'WT' or 
             $dataset_object->name eq 'WBPhenotype0001179' ) ){ 
         return "Too many to list"; # Hack for parent WT Phenotype - broken!
       }
       join( $LIST_SEPARATOR,
             map{ $_->{history_name} || () } 
             &cached_data_at($dataset_object,$pos) );
     },
     "VAL_${col}_dminfo" => sub{
       my $dataset_object = $_[0];
       if( $dataset_object->class eq 'Phenotype' and
           ( $dataset_object->name eq 'WT' or 
             $dataset_object->name eq 'WBPhenotype0001179' ) ){ 
         return "Too many to list"; # Hack for parent WT Phenotype - broken!
       }
       join( $LIST_SEPARATOR,
             map{ $_->{info} }
             &cached_data_at($dataset_object,$pos) );
     },

     "TBL_${dataset}__${col}__dm" => {
       OBJECTS => sub{ [ &cached_data_at($_[0],$pos) ] },
       VAL_rnai                  => &cached_val_sub('name'),
       VAL_history_name          => &cached_val_sub('history_name'),
       VAL_experiment_date       => &cached_val_sub('experiment_date'),
       VAL_experiment_strain     => &cached_val_sub('experiment_strain'),
       VAL_experiment_author     => &cached_val_sub('experiment_author'),
       VAL_experiment_laboratory => &cached_val_sub('experiment_laboratory'),
       VAL_info                  => &cached_val_sub('info'),
       VAL_phenotype             => &cached_val_sub('phenotype'),
       #VAL_inhibits_gene         => &cached_val_sub('inhibits_gene'),
       #VAL_inhibits_gene_primary => &cached_val_sub('inhibits_gene_primary'),
       #VAL_inhibits_gene_secondary=>&cached_val_sub('inhibits_gene_secondary'),
       VAL_inhibits_gene => sub{
         join( $LIST_SEPARATOR, 
               map{$_->{public_name}} @{$_->{inhibits_gene}} );
       },
       VAL_inhibits_gene_primary => sub{
         join( $LIST_SEPARATOR, 
               map{$_->{public_name}} @{$_->{inhibits_gene_primary}} );
       },
       VAL_inhibits_gene_secondary => sub{
         join( $LIST_SEPARATOR, 
               map{$_->{public_name}} @{$_->{inhibits_gene_secondary}} );
       },
       VAL_phenotype_name => sub{
         join( $LIST_SEPARATOR, map{$_->{name}} @{$_->{phenotype}} );
       },
       VAL_phenotype_info => sub{
         join( $LIST_SEPARATOR, map{$_->{info}} @{$_->{phenotype}} );
       },
       VAL_phenotype_label => sub{ # Do we need this one?
         join( $LIST_SEPARATOR, map{$_->{info}} @{$_->{label}} );
       },
       VAL_phenotype_name_observed => sub{ # Get rid of NOT phenotypes
         join( $LIST_SEPARATOR, 
               ( map{$_->{name}} grep{ ! $_->{Not} } @{$_->{phenotype}} ) );
       },
       VAL_phenotype_info_observed => sub{ # Get rid of NOT phenotypes
         join( $LIST_SEPARATOR, 
               ( map{$_->{info}} grep{ ! $_->{Not} } @{$_->{phenotype}} ) );
       },
       VAL_phenotype_name_unobserved => sub{ # Get rid of NOT phenotypes
         join( $LIST_SEPARATOR, 
               ( map{$_->{name}} grep{ $_->{Not} } @{$_->{phenotype}} ) );
       },
       VAL_phenotype_info_unobserved => sub{ # Get rid of NOT phenotypes
         join( $LIST_SEPARATOR, 
               ( map{$_->{info}} grep{ $_->{Not} } @{$_->{phenotype}} ) );
       },
       IDX => ['rnai','history_name'],
     },

     "TBL_${dataset}__${col}_phenotype__dm" => {
       # Used for phenotype pick-lists
       OBJECTS => sub{ 
         my %phens = ();
         foreach my $rnai_data( &cached_data_at($_[0],$pos) ){
           foreach my $phen_data( @{$_->{phenotype} || []} ){
             my $nm = $phen_data->{name};
             unless( $phens{$nm} ){
               $phens{$nm} = { phenotype       => $phen_data->{name},
                               primary_name    => $phen_data->{primary_name},
                               label           => $phen_data->{label},
                               info            => $phen_data->{info},
                               rnai_scored          => [],
                               rnai_info_scored     => [],
                               rnai_observed        => [],
                               rnai_info_observed   => [],
                               rnai_unobserved      => [],
                               rnai_info_unobserved => [], };
             }
             push( @{$phens{$nm}->{rnai_scored}}, $rnai_data->{name} );
             push( @{$phens{$nm}->{rnai_info_scored}}, $rnai_data->{info} );
             if( $phen_data->{Not} ){
               push( @{$phens{$nm}->{rnai_unobserved}}, 
                     $rnai_data->{name} );
               push( @{$phens{$nm}->{rnai_info_unobserved}}, 
                     $rnai_data->{info} );
             }
             else{
               push( @{$phens{$nm}->{rnai_observed}}, 
                     $rnai_data->{name} );
               push( @{$phens{$nm}->{rnai_info_observed}}, 
                     $rnai_data->{info} );
             }
           }
         }
         return [ values %phens ];
       },
       VAL_phenotype     => &cached_val_sub('phenotype'),
       VAL_primary_name  => &cached_val_sub('primary_name'),
       VAL_label         => &cached_val_sub('label'),
       VAL_info          => &cached_val_sub('info'),
       VAL_rnai_scored_count     => sub{ scalar(@{$_->{rnai_scored}||[]})},
       VAL_rnai_scored           => &cached_val_sub('rnai_scored'),
       VAL_rnai_info_scored      => &cached_val_sub('rnai_info_scored'),
       VAL_rnai_observed_count   => sub{ scalar(@{$_->{rnai_observed}||[]})},
       VAL_rnai_observed         => &cached_val_sub('rnai_observed'),
       VAL_rnai_observed_info    => &cached_val_sub('rnai_info_observed'),
       VAL_rnai_unobserved_count => sub{ scalar(@{$_->{rnai_unobserved}||[]})},
       VAL_rnai_unobserved       => &cached_val_sub('rnai_unobserved'),
       VAL_rnai_unobserved_info  => &cached_val_sub('rnai_info_unobserved'),
       IDX => ['phenotype','primary_name','rnai_scored_count',
               'rnai_observed_count','rnai_unobserved_count']
     },

    );
  } # End ?RNAi

  elsif( $class eq 'Strain' ){
    # ?Strain
    return
    (
     "TBL_${dataset}__${col}__dm" => {
       OBJECTS => sub{ [ &cached_data_at($_[0],$pos) ] },
       VAL_strain   => &cached_val_sub('name'),
       VAL_genotype => &cached_val_sub('genotypes'),
       VAL_info     => &cached_val_sub('info'),
       IDX => ['strain'],
     },
     "VAL_${col}_dmlist" => sub{ 
       &dmlist_at($_[0],$pos);
     },
     "VAL_${col}_dminfo" => sub{ &dminfo_at( $_[0],$pos ) },
    );
  } # End ?Strain

  elsif( $class eq 'Microarray' ){
    # ?Microarray_experiment
    # Referenced by Microarray_result
    return
    (
     "TBL_${dataset}__${col}__dm" => {
       OBJECTS => sub{ [ &cached_data_at($_[0],$pos) ] },
       VAL_microarray => &cached_val_sub('name'),
     },
     "VAL_${col}_dmlist" => sub{
       &dmlist_at($_[0],$pos);
     },
    );
  } # End ?Microarray_experiment

  elsif( $class eq 'Microarray_experiment' ){
    # ?Microarray_experiment
    # Referenced by Microarray_result
    return
    (
     "TBL_${dataset}__${col}__dm" => {
       OBJECTS => sub{ [ &cached_data_at($_[0],$pos) ] },
       VAL_microarray_experiment => &cached_val_sub('name'),
       VAL_remark_dmlist         => &cached_val_sub('remark_dmlist'),
       #VAL_paper_dmlist          => &cached_val_sub('paper_dmlist'),
       #VAL_paper_dminfo          => &cached_val_sub('paper_dminfo'),
       #VAL_sample_a              => &cached_val_sub('sample_a'),
       #VAL_sample_a              => &cached_val_sub('sample_b'),
       #VAL_microarray_sample     => &cached_val_sub('microarray_sample'),
       IDX => ['microarray_experiment'],
     },
     "VAL_${col}_dmlist" => sub{ 
       &dmlist_at($_[0],$pos);
     },
     #"VAL_${col}_dminfo" => sub{ &dminfo_at( $_[0],$pos ) },
    );
  } # End ?Microarray_experiment

  elsif( $class eq 'Pseudogene' ){
    # ?Pseudogene
    return
    (
     "TBL_${dataset}__${col}__dm" => {
       OBJECTS  => sub{ [ &cached_data_at($_[0],$pos) ] },
       VAL_pseudogene  => &cached_val_sub('name'),
       VAL_gene        => &cached_val_sub('gene'),
       VAL_info        => &cached_val_sub('info'),
       IDX             => ['pseudogene']
     },
     "VAL_${col}_dmlist" => sub{ 
       &dmlist_at($_[0],$pos);     },
     "VAL_${col}_dminfo" => sub{ &dminfo_at( $_[0],$pos ) },
    );
  } # End ?Pseudogene

  elsif( $class eq 'Transcript' ){
    # ?Transcript
    return
    (
     "TBL_${dataset}__${col}__dm" => {
       OBJECTS  => sub{ [ &cached_data_at($_[0],$pos) ] },
       VAL_transcript  => &cached_val_sub('name'),
       VAL_gene        => &cached_val_sub('gene'),
       VAL_info        => &cached_val_sub('info'),
       IDX             => ['transcript']
     },
     "VAL_${col}_dmlist" => sub{ 
       &dmlist_at($_[0],$pos);
     },
     "VAL_${col}_dminfo" => sub{ &dminfo_at( $_[0],$pos ) },
    );
  } # End ?Transcript

  elsif( $class eq 'Transgene' ){
    # ?Transgene
    return
    (
     "TBL_${dataset}__${col}__dm" => {
       OBJECTS => sub{ [ &cached_data_at($_[0],$pos) ] },
       VAL_transgene => &cached_val_sub('name'),
       VAL_summary   => &cached_val_sub('summary'),
       VAL_info      => &cached_val_sub('info'),
       IDX => ['transgene'],
     },
     "VAL_${col}_dmlist" => sub{ 
       &dmlist_at($_[0],$pos);
     },
     "VAL_${col}_dminfo" => sub{ &dminfo_at( $_[0],$pos ) },
    );
  } # End ?Transgene

  elsif( $class eq 'Experiment' ){
    # Experiment - not an object, but a tree location

    return
    (
     "TBL_${dataset}__${col}_author__dm" => {
       OBJECTS => sub{ [ &names_at( $_[0], "$pos.Author" ) ] },
       VAL_author => sub{ ref($_[0]) ? '' : $_[0] },
       IDX => ['author'], 
     },
     "VAL_${col}_author_dmlist" =>sub{&dmlist_at($_[0],"$pos.Author") },
     
     &insert_dimension('Laboratory',"g$pos.Laboratory",$dataset,
                       join( "_", $col, 'laboratory') ),
     "VAL_${col}_date"         => sub{ &names_at($_[0],"$col.Date") },
     "VAL_${col}_strain"       => sub{ &names_at($_[0],"$col.Strain") },
     "VAL_${col}_genotype"     => sub{ &names_at($_[0],"$col.Genotype") },
     "VAL_${col}_treatment"    => sub{ &names_at($_[0],"$col.Treatment") },
     "VAL_${col}_life_stage"   => sub{ &names_at($_[0],"$col.Life_stage") },
     "VAL_${col}_temperature"  => sub{ &names_at($_[0],"$col.Temperature") },
     "VAL_${col}_delivered_by" => sub{ &names_at($_[0],"$col.Delivered_by") },
    ),
  } # End Experiment

  elsif( $class eq 'Map_position' ){
    # Map_position - not an object, but a tree location
    # Referenced by Gene and Variation datasets
    return
    (
     "TBL_${dataset}__${col}__dm" => {
       OBJECTS => sub{ 
         my @data;
         foreach my $map( &at($_[0],"$pos.Map") ){
           my $map_name = "$map";
           foreach my $position( &at($_[0],"$pos.Map.$map_name.Position") ){
             my %this 
                 = ( name => $map_name,
                     position => $position,
                     method => 'linkage' );
             push @data, {%this};
           }
         }
         if( my $map = &at($_[0],"$pos.Interpolated_map_position") ){
           my $map_name = "$map";
           foreach my $position
               ( &at($_[0],"$pos.Interpolated_map_position.$map_name") ){
             my %this 
                 = ( name => $map_name,
                     position => $position, 
                     method => 'interpolation' );
             push @data, {%this};
           }
         }
         return [ @data ];
       },
       VAL_map      => &cached_val_sub('name'),
       VAL_position => &cached_val_sub('position'),
       VAL_method   => &cached_val_sub('method'),
       COL_position => qq| float default NULL |,
       IDX => ['map','position'],
     }
     
    );

  } # End Map_position

  elsif( $class eq 'DB_info.Database' ){
    # DB_info - not an object, but a tree location
    # TODO: UniProtKB dimension
    return
    (
     "TBL_${dataset}__${col}__dm" => {
       OBJECTS      => sub{
         my $obj = $_[0] || return [];
         my @hashrefs = ();
         my @dbs = $obj->at("$pos");
         @dbs = (undef) unless @dbs;

         foreach my $db( @dbs ){
           $db ||= '';
           my @fields = $obj->at(join('.', $pos, ($db||'-')));
           @fields = (undef) unless @fields;
           foreach my $field( @fields ){
             my @accessions = 
                 $obj->at(join('.', $pos, ($db||'-'),($field||'-')));
             @accessions = (undef) unless @accessions;
             foreach my $acc( @accessions ){ # bit of UniProt hackery
               my $db_label = $db;
               #warn( "==> $db, $field, $acc" );
               if( $db eq 'SwissProt' or $db eq 'TREMBL' ){
                 if( $db eq 'TREMBL' and ! $acc ){
                   $acc = $field;
                   $field = 'TrEMBL_ID';
                 }
                 $db_label = 'UniProtKB';
               }

               push @hashrefs, {
                 db        => ( $db_label ? "$db_label"    : undef ),
                 field     => ( $field    ? "$field" : undef ),
                 accession => ( $acc      ? "$acc"   : undef ),
                 db_field  => join( ":", $db_label||(), $field||() ),
                 info      => join( ":", $db_label||(), $field||(), $acc||() ),
               }
             }
           }
         }
         return [ @hashrefs ];
       },
       VAL_db        => &cached_val_sub('db'),
       VAL_field     => &cached_val_sub('field'),
       VAL_db_field  => &cached_val_sub('db_field'),
       VAL_accession => &cached_val_sub('accession'),
       VAL_info      => &cached_val_sub('info'),
     },
     "VAL_${col}_dmlist" => sub{
       my $obj  = $_[0] || return;
       my @vals = ();
       foreach my $db( $obj->at("$pos") ){
         foreach my $field( $obj->at("$pos.$db") ){
           foreach my $acc( $obj->at("$pos.$db.$field") ){
             push @vals, join( ":", $db||(), $field||(), $acc||() );
           }
         }
       }
       return join( $LIST_SEPARATOR, @vals );
     }, 
     );
  } # End DB_info

  elsif( $class eq 'Author' or        # RNAi
         $class eq 'Database'         # RNAi
         ){
    # Other named classes - handle in a generic way, but use cache.
    return
    (
     "TBL_${dataset}__${col}__dm" => {
       OBJECTS      => sub{ [ &cached_at($_[0],$pos) ] },
       "VAL_${col}" => sub{ defined($_[0]) ? $_[0] : undef },
       IDX => [$col],
     },
     "VAL_${col}_dmlist" => sub{ &dmlist_at($_[0],$pos) },
    );
  }

  elsif( $class eq '2_point_data' or        # Transgene
         $class eq 'Anatomy_name' or
         $class eq 'Anatomy_function' or    # Anatomy_term
         # SPECIFIED $class eq 'Author'         
         $class eq 'Cell_group' or
         $class eq 'Clone' or               # Transgene
         $class eq 'Condition' or           # Phenotype_info
         $class eq 'Expression_cluster' or  # Anatomy_term
         $class eq 'Expr_profile' or        # RNAi
         $class eq 'Gene_class' or          # Variation
         $class eq 'Homol_data' or          # RNAi
         $class eq 'Interaction' or         # Phenotype
         $class eq 'Life_stage' or
         $class eq 'Map' or                 # Transgene
         $class eq 'Mass_spec_peptonide' or  # Typo Protein
         $class eq 'Mass_spec_peptide' or    # Protein
         $class eq 'Microarray' or          # Microarray_results
         $class eq 'Movie' or               # RNAi
         $class eq 'Multi_pt_data' or       # Transgene
         $class eq 'PCR_product'or          # RNAi
         $class eq 'Person_name' or
         $class eq 'Phenotype_name' or
         $class eq 'Picture' or             # RNAi
         $class eq 'Reconstruction' or
         $class eq 'Rearrangement' or       # Phenotype
         $class eq 'Sequence' or            # RNAi
         $class eq 'Text' or
         # SPECIFIED $class eq 'Transgene'
         $class eq 'TreeNode' or
         $class eq 'Variation' or
         0
         ){
    # Other named classes - handle in a generic way.
    return
    (
     "TBL_${dataset}__${col}__dm" => {
       OBJECTS      => sub{ [ &at($_[0],$pos) ] },
       "VAL_${col}" => sub{ defined($_[0]) ? $_[0] : undef },
       IDX => [$col],
     },
     "VAL_${col}_dmlist" => sub{ &dmlist_at($_[0],$pos) },
    );

    #return &insert_simple_dimension( $pos, $dataset, $col );
  }

  else {
    # Cannot handle this class
    my $caller = join( ', ', (caller(0))[1..2] );
    die("Class $class has no default dimension. Called by $caller.");    
  }
}

#----------------------------------------------------------------------
# Convenience accessor to query the cached_data_at hashes. 
# Provided the hash key, returns a subroutine CODE ref that can query 
# the hash at reun time
sub cached_val_sub{
  my $hashkey = shift || die( "Need a hash key!" );
  return sub{ 
    ref($_[0]) eq 'HASH' || return undef;
    my $val = $_[0]->{$hashkey};

    $val || return $val;
    $val = ( ref($val) eq 'ARRAY' ) ? $val : [$val];
    return join( $LIST_SEPARATOR, grep{defined($_)} @{$val} );
  };
}

#----------------------------------------------------------------------
# Returns a serialised version of the evidence hash found at the location
sub evidence_at{
  my $xref_obj = $_[0] || return;
  my $main_obj = $_[1] || return;
  my $pos      = $_[2];
  if( ref( $xref_obj ) eq 'HASH' ){ 
    # Probably a cached_data stub
    $xref_obj = $xref_obj->{name};
  }
  $pos =~ s/\.\?\w+$//; # Strip trailing object, e.g. '.?Gene'
  my @evidence;
  foreach my $ev_type( &names_at($main_obj, "${pos}.${xref_obj}" ) ){
    my $ev_value = join( ", ", &names_at($main_obj, 
                                         "${pos}.${xref_obj}.${ev_type}" ) );
    push @evidence, "$ev_type: $ev_value";
  }
  return join( $LIST_SEPARATOR, grep{defined($_)} @evidence );
}


#----------------------------------------------------------------------
# Provided the schema location, returns a subroutine CODE ref that, when 
# passed an ace object, returns whether there is anything at that location.
sub has_val_sub{
  my $location = shift;
  return sub{ $_[0]->at($location) ? 1 : undef }
}

#----------------------------------------------------------------------
# Takes a sequence or gene object, or a tag referencing one, and returns
# the name, start (bp), end (bp), and strand of the top-level sequence. 
# Postions are cached to optimise multiple hits.
# Function is recursive
#
my $ASSEMBLY = {};
my $ASSEMBLY_count = 0;
sub physical_position{
  my $obj = shift || return;
  my $class = $obj->class;
  my $name  = $obj->name;

  # Look in cache first, and return if hit
  if( $ASSEMBLY->{$class.$name} ){ 
      ##print "L2165=in cache\n";
      return @{$ASSEMBLY->{$class.$name}} 
  }

 # Object's physical pos'n not in cache. Calculate from location in parent.
  # Note: $obj can be either Sequence, Gene, CDS etc; Allow for model diffs.
  if( my $parent_obj = 
      (
       &cached_at($obj,'Structure.From.Source') ||
       &cached_at($obj,'SMap.S_parent.Sequence' ) ||
       &cached_at($obj,'SMap.S_parent.Canonical_parent' ) ||
       &cached_at($obj,'Sequence_details.SMap.S_parent.Sequence' )
       ) ){
####test by RF_010410
      ##print "L2179=not in cache\n";
#############################################
    my( $pname, $pstart, $pend, $pstrand ) =  &physical_position($parent_obj);
    my $pos_t;
    if( $class eq 'Sequence'){
      ($pos_t) = grep{$_ eq $name} $parent_obj->at("Structure.Subsequence");
  } elsif( $class eq 'Gene'){

      ($pos_t) = grep{$_ eq $name} $parent_obj->at("SMap.S_child.Gene_child");
####test by RF_010410
      ##print "L2187_class=gene==pos_t=$pos_t\n"; ##$pos_t=WBGene00119000
########################
    } elsif( $class eq 'CDS'){
      ($pos_t) = grep{$_ eq $name} $parent_obj->at("SMap.S_child.CDS_child");
    } elsif( $class eq 'Transcript'){
      ($pos_t) = grep{$_ eq $name} $parent_obj->at("SMap.S_child.Transcript");
    } elsif( $class eq 'Pseudogene'){
      ($pos_t) = grep{$_ eq $name} $parent_obj->at("SMap.S_child.Pseudogene");
    } elsif( $class eq 'Homol_data'){
      ($pos_t) = grep{$_ eq $name} $parent_obj->at("SMap.S_child.Homol_data");
    } elsif( $class eq 'Operon' ){
      ($pos_t) = grep{$_ eq $name} $parent_obj->at("SMap.S_child.Operon" );
    } elsif( $class eq 'Variation' ){
      ($pos_t) = grep{$_ eq $name} $parent_obj->at("SMap.S_child.Allele" );
    } else { 
      warn( "Cannot get physical_position for $class objects" );
      return();
    }
    if( ! $pos_t ){
      warn( "Failed to get physical_position for $class $name from seq $parent_obj" );
      return();
    }
    my $start  = ($pos_t->right    ||0) + $pstart - 1;
    my $end    = ($pos_t->right(2) ||0) + $pstart - 1;
    my $strand = $pstrand;
####test by RF_010410
      ##print "L2215_start=$start==end=$end==strand=$strand\n";
##########################################
    if( $start > $end ){
      my $s = $start;
      $start = $end;
      $end   = $s;
      $strand = $strand * -1;
####test by RF_010410
      ##print "L2223_start=$start==end=$end==strand=$strand\n";
##########################################
    }
    $ASSEMBLY->{$class.$name} = [$pname, $start,$end,$strand]; # Update cache
  }
  else{
    if( $class eq "Sequence" ){ 
      # top-level sequence (e.g. chromosome)
      # Start is 1, end is got direct from SMap
      #warn( "==> $class:$name" );
      my $smap = $obj->db->raw_query("smap -from $class:$name");
      my ($length) = 
          $smap =~ /^SMAP\s+\S+\s+\d+\s+(\d+)/;
      $ASSEMBLY->{$class.$name} = [$name, 1,$length,1]; # Update cache
    } elsif( $class eq "Gene" and 
             $obj->at("Molecular_info.Corresponding_CDS") ){
      # Non-SMap gene (e.g. briggsae) - rely on CDS
      my $cds = &at($obj,"Molecular_info.Corresponding_CDS");
      $cds = $cds->fetch;
      $ASSEMBLY->{$class.$name} = [&physical_position($cds)];
    } else {
      # Unmapped e.g. gene object
      $ASSEMBLY->{$class.$name} = [];
    }
  }
  #warn ("$class.$name:\n".Dumper( $ASSEMBLY->{$class.$name} ) );
  $ASSEMBLY_count ++;
####test by RF_010410
  ##my @test_arr = @{$ASSEMBLY->{$class.$name}};
  ##foreach(@test_arr){
      ##print "L2253_ASSEMBLY_e=$_\n";
  ##}
  ##print "L2242_test_end_physical_position\n";
###################################
  #warn( "--> $ASSEMBLY_count entries in ASSEMBLY cache" );
  return @{$ASSEMBLY->{$class.$name}};
}

#----------------------------------------------------------------------
# Interrogates the AceDB schema for the given acedb class and has a stab 
# at auto-generating an ace2mart config data structure. This structure
# can, of course, be overwritten where the autogen made a bad call ;)
sub autogen_ace2mart_config{
  my $acedb       = shift || die "Need an ACE DB handle";
  my $acedb_class = shift || die "Need an ACE DB object class";
  my $dataset     = shift || lc( $acedb_class );
  my $ace_query   = shift || '*';

  my $config = {}; # It's all going to end up in here
  
  my( $model ) = $acedb->fetch(Model=>"?$acedb_class");
  $model || die ("Cannot find schema for $acedb_class in ACE DB");

  &_descend_col( $dataset, $model, $config, '', [], $ace_query );
  return $config;
}

sub _descend_col{
  my $dataset   = shift;
  my $obj       = shift;
  my $config    = shift || die( "Need a config hashref! From: ".
                               join( ", ", (caller(0))[1..2])."\n");
  my $table     = shift;
  if( length($table) > 68 ){ confess( "Table $table is too long (max 68)!" ) }

  my @heirarchy = @{shift(@_)||[]};
  my $ace_query = shift || '';

  #----------
  # What de we do with this tag heirarchy in terms of the config?
  # We need to work out the string that specifies the point 
  # in the schema where this data lives, and a field prefix 
  # to use in the mart schema
  my @loc;
  my @field;
  my $skip_next = 0;
  foreach my $string( @heirarchy ){

    if( $string eq 'UNIQUE' ){ next }
    if( $skip_next )         { $skip_next = 0; next }
    if( $string eq 'XREF' )  { $skip_next = 1; next } 

    push @field, $string;
    $field[-1] =~ s/^\?//;
    $field[-1] =~ s/^\#//;
    if( $field[-2] and 
        $field[-2] ne 'Int' and
        $field[-2] ne 'Text'and 
        $field[-1] eq $field[-2] ){ 
      # Avoid duplicates where leaf object eq the parent tag name
      pop @field;
    }
  }
  my $location = join( ".", @heirarchy );

  # Turn field names for MySQL into camel-case;
  my @camel_field;
  foreach my $f( @field ){
    push @camel_field, join('', map{ucfirst($_)} split('_',$f) );
  }
  my $field; # Create a field from position, but ensure that it's length is OK 
  while( @camel_field ){
    $field = join( "_", @camel_field );
    if( length( "${dataset}__${field}__dm" ) > 64 ){
      warn( "[WARN] FIELD TOO LONG: Removing $camel_field[0] from ". 
            join("_",@camel_field) );
      shift @camel_field; # Field is too long, reject root of tree
      next;
    }
    last;
  }

  #----------
  # The main object itself
  if( @heirarchy == 0 ){

    my $class = "$obj";
    $class =~ s/^\?//;
    $field = lc( $class );

    # Create the main table in the config;
    $table = "TBL_${dataset}__${field}__main";
    $config->{$table} = {};
    $config = $config->{$table}; # Descend into main table config 
    
    # Add the code ref that retrieves from ACE all objects of the given class
    $config->{OBJECTS} = sub{ [ $_[0]->fetch($class=>$ace_query) ] };

    # Add a field corresponding to the object's name 
    $config->{"VAL_$field"} = sub{ &name($_[0]) };
  }

  #----------
  # 'meta-object' tag (e.g. #Evidence)
  elsif( "$obj" =~ /^\#/ ){
    # TODO: work out how to handle #Evidence as a pseudo-dminfo col
    warn( "# $location => $field \n" );
    %{$config} = ( %{$config}, &insert_val( "$obj", $location, $field,
                                            ( $table =~ /__dm$/ ? 1 : () ) ));
  }

  #----------
  # ?Object, 'Text', 'Int' and 'Date' tags
  elsif( "$obj" =~ /^\?/ or
      "$obj" eq 'Text' or
      "$obj" eq 'Int' ){
    
    if( $heirarchy[-2] and $heirarchy[-2] eq 'UNIQUE' ){ 
      # Unique; put in this table
      warn( "+ $location => $field \n" );
      %{$config} = ( %{$config}, 
                     &insert_val( "$obj", $location, $field,
                                  ( $table =~ /__dm$/ ? 1 : () ) ));
    } 
    else { # Multi-value
      warn( "* $location => $field \n" );
      if( $table =~ /__main$/ ){ # In main table - create a dimension
        my %dmconfig = &insert_dimension( "$obj",$location,$dataset,$field );
        foreach my $key( keys %dmconfig ){
          $config->{$key} = $dmconfig{$key};
          if( $key =~ /^TBL/ ){
            $table = $key; # Set table that corresponds to this dimension
          }
        }
        $config = $config->{$table}; # Descend into dimension table
      } 
      else { # Already in dimension - create dminfo
        my %new_conf = &insert_val( "$obj", $location, "${field}_dmlist", 1 );
        map {$config->{$_} = $new_conf{$_} } keys %new_conf;
      }
    }
  } # End 'Text', 'Int' and 'Date' variables

  #----------
  elsif( scalar( $obj->col ) > 1 ){
    if( "$obj" eq 'UNIQUE' ){
      # Muiti-value unique tag; put in this table
      warn( "+ $location => $field \n" );
      %{$config} = ( %{$config}, 
                     &insert_val( "$obj", $location, $field,
                                  ( $table =~ /__dm$/ ? 1 : () ) ));
    }
    else{
      # Multi-value intermediate tag
      warn( "- $location => $field \n" );
      $config->{"VAL_${field}_count"} = sub{ 
        my @tags = &at($_[0], $location);
        return @tags ? scalar( @tags ) : undef;
      };
      $config->{"COL_${field}_count"} = 'int(10) unsigned default NULL';
    }
  }

  #----------
  # 'UNIQUE' tag (Uninteresting)
  elsif( "$obj" eq 'UNIQUE' ){
  }

  #----------
  # 'XREF' tag (Uninteresting)
  elsif( "$obj" =~ 'XREF' ){
  }

  #----------
  # Probably single-value intermediate tag (Uninteresting) 
  else{
  }

  #====================
  # Recurse through the child tags
  foreach my $tag( $obj->col ){
    &_descend_col( $dataset, $tag, $config, $table, [ @heirarchy, "$tag" ] );
  }
  return;
}

#----------------------------------------------------------------------
# If the caches take up too much memory they need to be cleared
# periodically. 
sub clear_large_caches{
  my $ACEDB = shift || die( "Need an AceDB object" );
  my $threshold = shift || 50000; # Size cache must reach before clearing
  my @cleared;
  if( $ObjectCache_count > $threshold ){
    $ObjectCache = {};
    $ObjectCache_count = 0;
    push @cleared,"ObjectCache";
  }
  if( $ObjectDataCache_count > $threshold ){
    $ObjectDataCache = {};
    $ObjectDataCache_count = 0;
    push @cleared,"ObjectDataCache";
    $ACEDB->memory_cache_clear();
    push @cleared,"AceMemoryCache";
  }
  if( $ASSEMBLY_count > $threshold ){
    $ASSEMBLY = {};
    $ASSEMBLY_count = 0;
    push @cleared,"ASSEMBLY";
  }
  #warn( "     > Obj: $ObjectCache_count, ".
  #      "Dat: $ObjectDataCache_count, Asm: $ASSEMBLY_count\n" );
  return @cleared;
}

#---
1;
