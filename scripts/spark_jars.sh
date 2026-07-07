#!/bin/bash

set -euo pipefail

MAVEN_BASE="https://repo1.maven.org/maven2"
SPARK_VERSION="3.4.0"
SCALA_VERSION="2.12"
KAFKA_VERSION="3.3.2"
COMMONS_POOL2_VERSION="2.11.1"
CONTAINER_NAME="spark-master"
JARS_DIR="/opt/spark/jars"

JARS=(
  "org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar"
  "org/apache/kafka/kafka-clients/${KAFKA_VERSION}/kafka-clients-${KAFKA_VERSION}.jar"
  "org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar"
  "org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar"
  "org/apache/spark/spark-avro_${SCALA_VERSION}/${SPARK_VERSION}/spark-avro_${SCALA_VERSION}-${SPARK_VERSION}.jar"
)

cd ~

echo "==> Downloading Spark JARs..."
for JAR_PATH in "${JARS[@]}"; do
  JAR_FILE=$(basename "$JAR_PATH")
  if [ -f "$JAR_FILE" ]; then
    echo "  [skip] $JAR_FILE already exists"
  else
    echo "  [download] $JAR_FILE"
    wget -q --show-progress "${MAVEN_BASE}/${JAR_PATH}"
  fi
done

echo ""
echo "==> Copying JARs into container '${CONTAINER_NAME}'..."
for JAR_PATH in "${JARS[@]}"; do
  JAR_FILE=$(basename "$JAR_PATH")
  echo "  [copy] $JAR_FILE -> ${CONTAINER_NAME}:${JARS_DIR}/"
  docker cp "$JAR_FILE" "${CONTAINER_NAME}:${JARS_DIR}/"
done

echo ""
echo "Done! All JARs copied to ${CONTAINER_NAME}:${JARS_DIR}/"