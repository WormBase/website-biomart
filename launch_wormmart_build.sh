#!/bin/bash

# Check these before runnimng
MART_HOST=${MART_DBHOST}
MART_PORT=${MART_DBPORT}
MART_USER=${MART_DBUSER}
MART_PASS=${MART_DBPASS}
#ACE_HOST=${ACE_HOST}
ACE_HOST=cbi4a

#echo Enter MART_DBNAME: 
#read MART_DBNAME

if [ ! $MART_DBNAME ]; then
  echo "[*DIE] Set MART_DBNAME environment variable to the name of the mart DB"
  exit 1;
fi
if [ ! $MART_PORT ]; then MART_PORT=3306; fi
echo "[INFO] Writing to database ${MART_HOST}:${MART_DBNAME}"

BSUB_QUE=long

# This is the subroutine that submits the job.
dispatch()
{
  COMMAND="$PWD/ace2mart.pl --mart_host=$MART_HOST --mart_port=$MART_PORT --mart_user=$MART_USER --mart_pass=$MART_PASS --mart_dbname=$MART_DBNAME --ace_host=$ACE_HOST --force_recreate ${MODULE}.pm"
  echo [INFO] submitting $COMMAND
  bsub -q $BSUB_QUE -M $BSUB_MEM -o "logs/${MODULE}_%J.out" -e "logs/${MODULE}_%J.err" "$COMMAND"
}

#for MODULE in \
#  PhenotypePointerLoader
#do
#  BSUB_MEM=2000000
#  dispatch
#done

for MODULE in \
  Anatomy_termLoader \
  DnaLoader \
  Expr_patternLoader \
  GoTermLoader \
  GoTermPointerLoader \
  PaperLoader \
  PhenotypeLoader \
  PhenotypePointerLoader
do
  BSUB_MEM=2000000
  #dispatch
done

for MODULE in \
  RnaiLoader \
  VariationLoader 
do
  BSUB_MEM=7900000
#  dispatch
done

for MODULE in \
  GeneLoader 
do
  BSUB_MEM=7900000
#  dispatch
done

