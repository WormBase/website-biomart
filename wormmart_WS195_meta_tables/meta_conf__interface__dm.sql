-- MySQL dump 9.10
--
-- Host: localhost    Database: wormmart_195
-- ------------------------------------------------------
-- Server version	5.0.37

--
-- Table structure for table `meta_conf__interface__dm`
--

CREATE TABLE meta_conf__interface__dm (
  dataset_id_key int(11) default NULL,
  interface varchar(100) default NULL,
  UNIQUE KEY dataset_id_key (dataset_id_key,interface)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

--
-- Dumping data for table `meta_conf__interface__dm`
--

INSERT INTO meta_conf__interface__dm VALUES (14,'default');
INSERT INTO meta_conf__interface__dm VALUES (17,'default');
INSERT INTO meta_conf__interface__dm VALUES (18,'default');
INSERT INTO meta_conf__interface__dm VALUES (19,'default');
INSERT INTO meta_conf__interface__dm VALUES (20,'default');
INSERT INTO meta_conf__interface__dm VALUES (21,'default');
INSERT INTO meta_conf__interface__dm VALUES (22,'default');
INSERT INTO meta_conf__interface__dm VALUES (23,'default');
INSERT INTO meta_conf__interface__dm VALUES (24,'default');
INSERT INTO meta_conf__interface__dm VALUES (25,'default');

