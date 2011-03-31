-- MySQL dump 9.10
--
-- Host: localhost    Database: wormmart_195
-- ------------------------------------------------------
-- Server version	5.0.37

--
-- Table structure for table `meta_conf__dataset__main`
--

CREATE TABLE meta_conf__dataset__main (
  dataset_id_key int(11) NOT NULL,
  dataset varchar(100) default NULL,
  display_name varchar(200) default NULL,
  description varchar(200) default NULL,
  `type` varchar(20) default NULL,
  visible int(1) unsigned default NULL,
  version varchar(25) default NULL,
  modified timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
  UNIQUE KEY dataset_id_key (dataset_id_key)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

--
-- Dumping data for table `meta_conf__dataset__main`
--

INSERT INTO meta_conf__dataset__main VALUES (18,'wormbase_anatomy_term','Anatomy Term',NULL,'TableSet',1,NULL,'2008-11-05 11:48:07');
INSERT INTO meta_conf__dataset__main VALUES (14,'wormbase_dna','DNA',NULL,'GenomicSequence',0,NULL,'2008-11-05 11:48:18');
INSERT INTO meta_conf__dataset__main VALUES (19,'wormbase_expr_pattern','Expression Pattern',NULL,'TableSet',1,NULL,'2008-11-05 11:48:24');
INSERT INTO meta_conf__dataset__main VALUES (21,'wormbase_gene','Gene',NULL,'TableSet',1,NULL,'2008-11-14 13:55:48');
INSERT INTO meta_conf__dataset__main VALUES (24,'wormbase_go_term','GO Term',NULL,'TableSet',1,NULL,'2008-11-05 11:48:49');
INSERT INTO meta_conf__dataset__main VALUES (20,'wormbase_paper','Paper',NULL,'TableSet',1,NULL,'2008-11-05 11:48:57');
INSERT INTO meta_conf__dataset__main VALUES (22,'wormbase_phenotype','Phenotype',NULL,'TableSet',1,NULL,'2008-11-10 13:37:48');
INSERT INTO meta_conf__dataset__main VALUES (23,'wormbase_rnai','RNAi',NULL,'TableSet',1,NULL,'2008-11-05 11:49:17');
INSERT INTO meta_conf__dataset__main VALUES (17,'wormbase_variation','Variation',NULL,'TableSet',1,NULL,'2008-11-12 15:13:43');
INSERT INTO meta_conf__dataset__main VALUES (25,'wormbase_phenotype_pointer','wormbase_phenotype_pointer','','TableSet',0,'','2008-11-10 13:07:26');

