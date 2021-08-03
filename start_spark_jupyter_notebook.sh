#!/bin.bash

# jupyter-notebook.sh
#
# Usage: bash jupyter-notebook.sh <hostname> <local|cluster> [queue]
#
# Start an jupyter notebook in either local (edge node only) or cluster mode
#
# The memory and executor settings in this file are optimized for a 5 node cluster
# composed of medium sized sparkplug nodes.
#
# NOTE: this should be run on your laptop since it will start an SSH tunnel

NOTEBOOK_PORT=1623

REPO_PATH="/home/cphillip/observation"
HDP_PATH="/usr/hdp/2.5.3.0-37"
REPO="http://uk.maven.org/maven2"
JARS="$REPO_PATH/jars/commons-csv-1.1.jar,$REPO_PATH/jars/univocity-parsers-1.5.1.jar"

EM="20G"   # Memory per Executor
MO="4096"  # Executor Memory Overhead

# Some of the configuration settings above are from:
# http://stackoverflow.com/questions/31728688/how-to-prevent-spark-executors-from-getting-lost-when-using-yarn-client-mode
# http://stackoverflow.com/questions/33074288/getting-error-in-spark-executor-lost
# http://stackoverflow.com/questions/37871194/how-to-tune-spark-executor-number-cores-and-executor-memory


HOSTNAME=$1
TARGET=$2
QUEUE=$3
if [ "x$QUEUE" == "x" ]; then
  QUEUE="default"
fi

if [ "x$HOSTNAME" == "x" ]; then
  echo "Usage: jupyter-notebook.sh <hostname> <local|cluster> [queue]"
  exit
fi

if [ "x$TARGET" == "xlocal" ];then
  NE=2       # Number of Executors
  EC=5       # Cores per Executor
  pyspark_command="pyspark --conf 'spark.yarn.executor.memoryOverhead=$MO' --master local[10] --queue $QUEUE --repositories $REPO \
                    --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
                    --packages com.databricks:spark-csv_2.10:1.5.0,org.postgresql:postgresql:9.4.1207.jre7 --num-executors $NE \
                    --executor-cores $EC --executor-memory $EM"
else
  NE=20    # Number of Executors
  EC=5      # Cores per Executor
  pyspark_command="PYSPARK_PYTHON=./PYENV/env/bin/python nohup /usr/hdp/current/spark2-client/bin/pyspark \
                    --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
                    --conf 'spark.yarn.executor.memoryOverhead=$MO' \
                    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./PYENV/env/bin/python \
                    --conf spark.yarn.appMasterEnv.SPARK_HOME=/usr/hdp/current/spark2-client/ \
                    --conf spark.executorEnv.SPARK_HOME=/usr/hdp/current/spark2-client/ \
                    --master yarn --deploy-mode client --queue $QUEUE \
                    --repositories $REPO \
                    --num-executors $NE --executor-cores $EC --executor-memory $EM \
                    --archives /home/jperrsau/src/coda_2018_sadm_paper_experiment/harris2/env.zip#PYENV"
fi

pyspark_command="export PATH=$PATH:~/.local/bin; export PYSPARK_DRIVER_PYTHON=jupyter; export SPARK_MAJOR_VERSION=2; export PYSPARK_DRIVER_PYTHON_OPTS=\"notebook --port=$NOTEBOOK_PORT --ip=127.0.0.1 --NotebookApp.iopub_data_rate_limit=1.0e10 --no-browser\"; $pyspark_command"
echo $pyspark_command

ssh -t -t $HOSTNAME -L $NOTEBOOK_PORT:localhost:$NOTEBOOK_PORT "$pyspark_command"
