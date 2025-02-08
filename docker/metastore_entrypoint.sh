#!/bin/bash

set -x

: ${DB_DRIVER:=derby}
: ${INIT_CHECK_FILE:=/opt/hive/schema_initialized_at}

[[ $VERBOSE = "true" ]] && VERBOSE_MODE="--verbose" || VERBOSE_MODE=""

function initialize_hive {
  COMMAND="-initOrUpgradeSchema"
  if [ "$(echo "$HIVE_VER" | cut -d '.' -f1)" -lt "4" ]; then
     COMMAND="-${SCHEMA_COMMAND:-initSchema}"
  fi
  $HIVE_HOME/bin/schematool -dbType $DB_DRIVER $COMMAND $VERBOSE_MODE
  if [ $? -eq 0 ]; then
    echo "Initialized schema successfully."
    date +"%Y-%m-%d %H:%M:%S" > "$INIT_CHECK_FILE"
  else
    echo "Schema initialization failed!"
    exit 1
  fi
}

export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Xmx1G $SERVICE_OPTS"

# Check if schema has already been initialized
if [ ! -f "$INIT_CHECK_FILE" ]; then
  echo "Schema not initialized. Running initialization..."
  initialize_hive
else
  echo "Schema already initialized. Skipping initialization."
fi

export METASTORE_PORT=${METASTORE_PORT:-9083}
export HADOOP_CLASSPATH=$TEZ_HOME/*:$TEZ_HOME/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/tools/lib/*:$HIVE_HOME/lib/*
exec $HIVE_HOME/bin/hive --skiphadoopversion --skiphbasecp $VERBOSE_MODE --service metastore