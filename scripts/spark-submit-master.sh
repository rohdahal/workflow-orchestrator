#!/usr/bin/env bash
set -euo pipefail
exec docker exec -i orch-spark-master /opt/spark/bin/spark-submit \
  --master spark://orch-spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.postgresql:postgresql:42.7.4,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  "$@"
