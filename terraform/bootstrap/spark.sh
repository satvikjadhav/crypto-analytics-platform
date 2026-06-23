#!/bin/bash
set -euo pipefail
exec > /var/log/bootstrap.log 2>&1

# ── System packages ────────────────────────────────────────────────────────────
apt-get update -y
apt-get install -y docker.io docker-compose-plugin unzip curl

# Install AWS CLI v2 (apt ships outdated v1)
curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip
unzip -q /tmp/awscliv2.zip -d /tmp
/tmp/aws/install
rm -rf /tmp/awscliv2.zip /tmp/aws

systemctl enable docker
systemctl start docker
usermod -aG docker ubuntu

# ── App directory ──────────────────────────────────────────────────────────────
mkdir -p /opt/spark

# .env file – loaded explicitly via env_file in compose
cat > /opt/spark/.env << EOF
KAFKA_BOOTSTRAP_SERVERS=${kafka_private_ip}:9092
S3_BUCKET=${s3_bucket}
AWS_REGION=${aws_region}
EOF

# ── Docker Compose stack ───────────────────────────────────────────────────────
cat > /opt/spark/docker-compose.yml << 'EOF'
version: '3.8'
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
      --cores 2
      --memory 4G
      spark://spark-master:7077
    depends_on:
      - spark-master
EOF

# ── Initial stack bring-up ─────────────────────────────────────────────────────
cd /opt/spark && docker compose up -d

# ── Systemd service for restart-on-reboot ─────────────────────────────────────
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

echo "Spark bootstrap complete"