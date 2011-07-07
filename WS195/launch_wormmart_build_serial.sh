#!/bin/bash

# Check these before running
WORMMART_HOST=localhost
WORMMART_PORT=3306
WORMMART_USER=acabunoc
WORMMART_PASS=
ACE_HOST=localhost
WORMMART_DBNAME=wormmart_215
RESUME_GENE=WBGene00123682

if [ ! $WORMMART_DBNAME ]; then
  echo "[*DIE] Set WORMMART_DBNAME environment variable to the name of the mart 
DB"
  exit 1;
fi
if [ ! $WORMMART_PORT ]; then WORMMART_PORT=3306; fi
echo "[INFO] Writing to database ${WORMMART_HOST}:${WORMMART_DBNAME}"

run()
{
  COMMAND="$PWD/ace2mart.pl --mart_host=$WORMMART_HOST --mart_port=$WORMMART_PORT --mart_user=$WORMMART_USER --mart_pass=$WORMMART_PASS --mart_dbname=$WORMMART_DBNAME --ace_host=$ACE_HOST --resume_gene=$RESUME_GENE ${MODULE}.pm > logs/${MODULE}.pm"
  echo [INFO] running $COMMAND 
  $COMMAND
}

for MODULE in \
   GeneLoader 
do 
  run
done

#   Anatomy_termLoader \
#   DnaLoader \
#   Expr_patternLoader \
#   GoTermLoader \
#   GoTermPointerLoader \
#   PaperLoader \
#   PhenotypeLoader \
#   PhenotypePointerLoader \
#   RnaiLoader \
#   VariationLoader \
#   GeneLoader 
#do
#  run
#done


