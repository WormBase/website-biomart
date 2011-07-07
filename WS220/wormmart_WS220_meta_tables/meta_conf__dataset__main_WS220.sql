-- MySQL dump 10.11
--
-- Host: localhost    Database: wormmart_220
-- ------------------------------------------------------
-- Server version	5.0.77

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `meta_conf__dataset__main`
--

DROP TABLE IF EXISTS `meta_conf__dataset__main`;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
CREATE TABLE `meta_conf__dataset__main` (
  `dataset_id_key` int(11) NOT NULL,
  `dataset` varchar(100) default NULL,
  `display_name` varchar(200) default NULL,
  `description` varchar(200) default NULL,
  `type` varchar(20) default NULL,
  `visible` int(1) unsigned default NULL,
  `version` varchar(25) default NULL,
  `modified` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
  UNIQUE KEY `dataset_id_key` (`dataset_id_key`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
SET character_set_client = @saved_cs_client;

--
-- Dumping data for table `meta_conf__dataset__main`
--

LOCK TABLES `meta_conf__dataset__main` WRITE;
/*!40000 ALTER TABLE `meta_conf__dataset__main` DISABLE KEYS */;
INSERT INTO `meta_conf__dataset__main` VALUES (18,'wormbase_anatomy_term','Anatomy Term',NULL,'TableSet',1,NULL,'2011-04-15 21:31:07'),(14,'wormbase_dna','DNA',NULL,'GenomicSequence',0,NULL,'2011-04-15 21:31:07'),(19,'wormbase_expr_pattern','Expression Pattern',NULL,'TableSet',1,NULL,'2011-04-15 21:31:07'),(21,'wormbase_gene','Gene',NULL,'TableSet',1,NULL,'2011-04-15 21:31:08'),(24,'wormbase_go_term','GO Term',NULL,'TableSet',1,NULL,'2011-04-15 21:31:08'),(20,'wormbase_paper','Paper',NULL,'TableSet',1,NULL,'2011-04-15 21:31:08'),(22,'wormbase_phenotype','Phenotype',NULL,'TableSet',1,NULL,'2011-04-15 21:31:08'),(25,'wormbase_phenotype_pointer','wormbase_phenotype_pointer',NULL,'TableSet',0,NULL,'2011-04-15 21:31:08'),(23,'wormbase_rnai','RNAi',NULL,'TableSet',1,NULL,'2011-04-15 21:31:08'),(17,'wormbase_variation','Variation',NULL,'TableSet',1,NULL,'2011-04-15 21:31:09');
/*!40000 ALTER TABLE `meta_conf__dataset__main` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2011-07-05 23:23:42
