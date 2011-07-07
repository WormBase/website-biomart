package DnaLoader;

use strict;
use warnings;
use Data::Dumper qw( Dumper );

use WormMartTools;

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
$CONNECTION->{MART_DBNAME} = undef();


my $dataset     = 'wormbase_dna';
my %seq_queries = (
                   "elegans"  => 'CHROMOSOME_*',
                   "briggsae" => 'chr*',
                   "remanei"  => 'Crem_Contig*',
		   "japonica" => 'Cjap_Contig*', ##added by RF_122810, need test
		   "pacificus" => 'Ppa_Contig*',  ##added by RF_122810, need test
		   "brenneri" => 'Cbre_Contig*',  ##added by RF_122810, need test
                   #"briggsae" => 'cb25.*', 
                   ); 
# DEBUG
#%seq_queries = ( "briggsae" => "cb25.fpc0156");
#%seq_queries = ( "briggsae" => "chrI");
#%seq_queries = ( "elegans"  => 'CHROMOSOME_III');
#%seq_queries = ( "remanei" => 'Crem_Contig1' );
#%seq_queries = ( "japonica" => 'Cjap_Contig209' );  ##added by RF_010510 WBGene00182385
##%seq_queries = ( "pacificus" => 'Ppa_Contig2687' );  ##added by RF_010510 WBGene00103474
##%seq_queries = ( "brenneri" => 'Cbre_Contig66' );  ##added by RF_010510 WBGene00148355

my $chunk     = 100000;

$CONFIG = {
  "TBL_${dataset}__dna__main" => {

    'OBJECTS' => sub{
      # Retrieves chr sequence and splits into chunks of size $chunk.
      # A 'pseudo' object is created from each chunk
      my $ace_handle = shift;
      my @objs;
      foreach my $seq_query( values %seq_queries ){
        foreach my $seq($ace_handle->fetch('Sequence'=>$seq_query)){
          #my $dna;
          my $dna = $seq->asDNA;
          if( $dna =~ m/(>.+\n)/ ){
            if( length( $1 ) < 59 ){
              # Normal fasta header
              $dna =~ s/(>.+\n)//; #Strip first line
            }
            else{
              # Header probably merged with dna
              die( "Don't like the look of this FASTA file, no linebreak: \n".
                   substr($dna, 1, 60) );
            }
          }
          else{
              # Header probably missing
              warn( "Don't like the look of this FASTA file for ".
                    $seq->name . 
                    " no header: \n".
                    substr($dna, 1, 60) );
          }
          $dna =~ s/\s//g; #Strip whitespace
          my $seq_length = length( $dna );
          for( my $start=1; $start<$seq_length; $start+=$chunk ){
            my( $species ) = &names_at($seq,'Origin.Species');
            push @objs, {'chr_name' =>$seq->name,
                         'species'  => $species,
                         'chr_start'=>$start,
                         'sequence' =>substr( $dna, 
                                              $start-1, 
                                              $chunk ) };
          }
        }
      }
      return [ @objs ];
    },

    'COL_chr_name'  => qq| varchar(64) |,
    'COL_chr_start' => qq| int(10)     |,
    'COL_sequence'  => qq| mediumtext  |,
    'VAL_chr_name'  => sub{ 
      my $name; # Strip any leading 'CHROMOSOME_' prefix
      ($name = $_[0]->{'chr_name'}) =~ s/^CHROMOSOME_//;
      return $name },
    'VAL_chr_start' => sub{ $_[0]->{'chr_start'} },
    'VAL_sequence'  => sub{ $_[0]->{'sequence'} },
    'VAL_species'   => sub{ $_[0]->{'species'} },
    'IDX'           => [['chr_name','chr_start']],
  },
};

#---
1;
