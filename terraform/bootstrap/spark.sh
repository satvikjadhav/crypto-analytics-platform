#!/bin/bash
set -euo pipefail
exec > /var/log/bootstrap.log 2>&1

# ── System packages ────────────────────────────────────────────────────────────
apt-get update -y
apt-get install -y docker.io docker-compose-v2 unzip curl

systemctl enable docker
systemctl start docker
usermod -aG docker ubuntu

# ── App directory ──────────────────────────────────────────────────────────────
mkdir -p /opt/spark

# Derive worker resources dynamically so this script stays correct if the
# instance type changes. Reserve 15% of available RAM for the OS.
CORES=$(nproc)
MEM_MB=$(awk '/MemAvailable/ {print int($2 * 0.85 / 1024)}' /proc/meminfo)

# .env file – loaded explicitly via env_file in compose.
# Terraform interpolates the $${ } vars below at plan/apply time.
cat > /opt/spark/.env << EOF
KAFKA_BOOTSTRAP_SERVERS=${kafka_private_ip}:9092
S3_BUCKET=${s3_bucket}
AWS_REGION=${aws_region}
SPARK_WORKER_CORES=$CORES
SPARK_WORKER_MEMORY_MB=$MEM_MB
EOF

# ── Docker Compose stack ───────────────────────────────────────────────────────
# Unquoted heredoc: shell fills in $CORES/$MEM_MB at runtime.
# $${...} in the history server opts is Docker Compose variable substitution
# syntax (the double $ escapes one level so Terraform passes it through as $${}).
cat > /opt/spark/docker-compose.yml << EOF
services:
  spark-master:
    image: apache/spark:3.4.4
    hostname: spark-master
    container_name: spark-master
    restart: always
    env_file:
      - .env
    environment:
      SPARK_NO_DAEMONIZE: 'true'
    command: >
      /opt/spark/bin/spark-class
      org.apache.spark.deploy.master.Master
      --host spark-master
      --port 7077
      --webui-port 8080
    ports:
      - "8082:8080"
      - "7077:7077"
    volumes:
      - ~/spark-jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/hadoop-aws-3.3.4.jar
      - ~/spark-jars/aws-java-sdk-bundle-1.12.262.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar
      - /opt/spark/jobs:/opt/spark/jobs

  spark-worker:
    image: apache/spark:3.4.4
    hostname: spark-worker
    container_name: spark-worker
    restart: always
    env_file:
      - .env
    environment:
      SPARK_NO_DAEMONIZE: 'true'
    command: >
      /opt/spark/bin/spark-class
      org.apache.spark.deploy.worker.Worker
      --cores $CORES
      --memory $${MEM_MB}M
      spark://spark-master:7077
    depends_on:
      - spark-master
    volumes:
      - ~/spark-jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/hadoop-aws-3.3.4.jar
      - ~/spark-jars/aws-java-sdk-bundle-1.12.262.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

  spark-history:
    image: apache/spark:3.4.4
    hostname: spark-history
    container_name: spark-history
    restart: always
    env_file:
      - .env
    environment:
      SPARK_NO_DAEMONIZE: 'true'
      SPARK_HISTORY_OPTS: >-
        -Dspark.history.fs.logDirectory=s3a://${s3_bucket}/spark-logs
        -Dspark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
        -Dspark.hadoop.com.amazonaws.services.s3.enableV4=true
    command: >
      /opt/spark/bin/spark-class
      org.apache.spark.deploy.history.HistoryServer
    ports:
      - "18080:18080"
    depends_on:
      - spark-master
    volumes:
      - ~/spark-jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/hadoop-aws-3.3.4.jar
      - ~/spark-jars/aws-java-sdk-bundle-1.12.262.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar
EOF

# ── Systemd service (sole owner of stack lifecycle) ───────────────────────────
cat > /etc/systemd/system/crypto-spark.service << 'SVC'
[Unit]
Description=Crypto Spark Stack
Requires=docker.service
After=docker.service network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/opt/spark
ExecStart=/usr/bin/docker compose up -d
ExecStop=/usr/bin/docker compose down
TimeoutStartSec=300

[Install]
WantedBy=multi-user.target
SVC

systemctl daemon-reload
systemctl enable crypto-spark.service
# Systemd is the sole owner of the stack; start it here rather than calling
# docker compose up directly so there's only one source of truth for lifecycle.
systemctl start crypto-spark.service

echo "Spark bootstrap complete"


df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.0.1.82:9092") \
    .option("subscribe", "crypto.trades") \
    .option("startingOffsets", "earliest") \
    .load()