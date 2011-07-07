
CREATE TABLE wb130_gene__gene__main (
  gene_key            int(10) unsigned NOT NULL AUTO_INCREMENT,
  wb_gene_id          varchar(128)     NOT NULL,

  cds_count           int(5) unsigned  NOT NULL,
  transcript_count    int(5) unsigned  NOT NULL,

  # Xref
  swissprot_count     int(5) unsigned  default NULL,
  ndb_count           int(5) unsigned  default NULL, 

#  # Protein
#  pfam_count          int(5) unsigned  default NULL,

  # General
  species             varchar(64)      NOT NULL,
  concise_description text             default NULL,
  brief_ident_count   int(5) unsigned  default NULL,
  db_remark_count     int(5) unsigned  default NULL,
  status              varchar(32)      default NULL,
  go_term_count       int(5) unsigned  default NULL,

#  # Gene
#  allele_reference    varchar(64)      default NULL,
#  allele_count        int(5) unsigned  default NULL,
#  public_name         varchar(64)      default NULL,
#  locus_cannonical    varchar(64)      default NULL,
#  locus_other_count   int(5) unsigned  default NULL,

  # Location
  chromosome_name     varchar(64)      default NULL,
  chromosome_start    int(10) unsigned default NULL,
  chromosome_end      int(10) unsigned default NULL,
  chromosome_strand   tinyint(2)       default NULL,
  chromosome_genetic  float(12)        default NULL,

#  # Phenotype
#  phenotype_count     int(5) unsigned  default NULL,
#  rnai_count          int(5) unsigned  default NULL,

  strain_count        int(5) unsigned  default NULL,
  antibody_count      int(5) unsigned  default NULL,

  PRIMARY KEY (gene_key)
);


CREATE TABLE wb130_gene__cds__main (
  cds_key             int(10) unsigned NOT NULL AUTO_INCREMENT,
  gene_key            int(10) unsigned NOT NULL,
  wb_gene_id          varchar(128)     NOT NULL,
  wb_wormpep_id       varchar(128)     default NULL,
  cds_name            varchar(128)     default NULL,
  cds_count           int(5) unsigned  default NULL,
  transcript_count    int(5) unsigned  default NULL,

  # Xref
  swissprot_id        varchar(128)     default NULL,
  swissprot_ac        varchar(128)     default NULL,
  ndb_gi              varchar(128)     default NULL, 

  # Protein
  pfam_count          int(5) unsigned  default NULL,

  # General
  species             varchar(64)      NOT NULL,
  concise_description text             default NULL,
  brief_ident         text             default NULL,
  db_remark           text             default NULL,
  status              varchar(32)      default NULL,
  go_term_count       int(5) unsigned  default NULL,

  # Gene
#  allele_reference    varchar(64)      default NULL,
#  allele_count        int(5) unsigned  default NULL,
#  public_name         varchar(64)      default NULL,
#  locus_cannonical    varchar(64)      default NULL, 
#  locus_count         int(5)           default NULL,

  # Location
  chromosome_name     varchar(64)      default NULL,
  chromosome_start    int(10) unsigned default NULL,
  chromosome_end      int(10) unsigned default NULL,
  chromosome_strand   tinyint(2)       default NULL,
  chromosome_genetic  float(12)        default NULL,

#  # Phenotype
#  phenotype_count     int(5) unsigned  default NULL,
#  rnai_count          int(5) unsigned  default NULL,

#  # Reagents
  strain_count        int(5) unsigned  default NULL,
  antibody_count      int(5) unsigned  default NULL,

  PRIMARY KEY (cds_key)
) TYPE=MyISAM;

## Protein
#CREATE TABLE wb130_gene__pfam__dm (
#  gene_key int(10) unsigned NOT NULL,
#  cds_key  int(10) unsigned NOT NULL,
#  pfam_id     varchar(32)      default NULL
#) TYPE=MyISAM;

# General
CREATE TABLE wb130_gene__go_term__dm (
  gene_key   int(10) unsigned NOT NULL,
  cds_key    int(10) unsigned NOT NULL,
  go_term    varchar(32)      default NULL,
  evidence_code enum('IC','IDA','IEA','IEP','IGI','IMP',
                     'IPI','ISS','NAS','ND','TAS',
                     'NR')       default NULL
) TYPE=MyISAM;

## Gene
#CREATE TABLE wb130_gene__allele_gene__dm (
#  gene_key   int(10) unsigned NOT NULL,
#  allele        varchar(64)      default NULL
#) TYPE=MyISAM;


## Gene
#CREATE TABLE wb130_gene__allele__dm (
#  gene_key   int(10) unsigned NOT NULL,
#  cds_key    int(10) unsigned NOT NULL,
#  allele        varchar(64)      default NULL
#) TYPE=MyISAM;

## Gene
#CREATE TABLE wb130_gene__locus__dm (
#  gene_key   int(10) unsigned NOT NULL,
#  locus         varchar(64)      default NULL
#) TYPE=MyISAM;

## Gene
CREATE TABLE wb130_gene__transcript__dm (
  gene_key   int(10) unsigned NOT NULL,
  cds_key    int(10) unsigned NOT NULL,
  transcript varchar(64)      default NULL
) TYPE=MyISAM;


## Phenotype
#CREATE TABLE wb130_gene__phenotype__dm (
#  gene_key   int(10) unsigned NOT NULL,
#  phenotype     text             default NULL
#) TYPE=MyISAM;

#CREATE TABLE wb130_gene__rnai__dm (
#  gene_key   int(10) unsigned NOT NULL,
#  cds_key    int(10) unsigned NOT NULL,
#  rnai          varchar(64)      default NULL
#) TYPE=MyISAM;


## Reagents
CREATE TABLE wb130_gene__strain__dm (
  gene_key   int(10) unsigned NOT NULL,
  strain        varchar(128)     default NULL
);

## Reagents
CREATE TABLE wb130_gene__antibody__dm (
  gene_key   int(10) unsigned NOT NULL,
  antibody      varchar(128)     default NULL
);