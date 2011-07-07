-- MySQL dump 9.10
--
-- Host: localhost    Database: wormmart_195
-- ------------------------------------------------------
-- Server version	5.0.37

--
-- Table structure for table `meta_template__template__main`
--

CREATE TABLE meta_template__template__main (
  dataset_id_key int(11) NOT NULL,
  template varchar(100) NOT NULL
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

--
-- Dumping data for table `meta_template__template__main`
--

INSERT INTO meta_template__template__main VALUES (18,'wormbase_anatomy_term');
INSERT INTO meta_template__template__main VALUES (14,'wormbase_dna');
INSERT INTO meta_template__template__main VALUES (19,'wormbase_expr_pattern');
INSERT INTO meta_template__template__main VALUES (21,'wormbase_gene');
INSERT INTO meta_template__template__main VALUES (24,'wormbase_go_term');
INSERT INTO meta_template__template__main VALUES (20,'wormbase_paper');
INSERT INTO meta_template__template__main VALUES (22,'wormbase_phenotype');
INSERT INTO meta_template__template__main VALUES (23,'wormbase_rnai');
INSERT INTO meta_template__template__main VALUES (17,'wormbase_variation');
INSERT INTO meta_template__template__main VALUES (25,'wormbase_phenotype_pointer');

