#!/bin/bash
set -euo pipefail
exec > /var/log/bootstrap.log 2>&1

apt-get update -y
apt-get install -y docker.io docker-compose-v2 awscli
systemctl enable docker && systemctl start docker
usermod -aG docker ubuntu

mkdir -p /opt/kafka
cat > /opt/kafka/docker-compose.yml << 'EOF'
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: zookeeper
    restart: always
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    healthcheck:
      test: ["CMD","bash","-c","echo ruok | nc localhost 2181"]
      interval: 10s; timeout: 5s; retries: 5

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: kafka
    restart: always
    depends_on:
      zookeeper:
        condition: service_healthy
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:29092,EXTERNAL://${private_ip}:9092
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'false'
    healthcheck:
      test: ["CMD","kafka-broker-api-versions","--bootstrap-server","localhost:9092"]
      interval: 15s; timeout: 10s; retries: 10

  schema-registry:
    image: confluentinc/cp-schema-registry:7.5.0
    container_name: schema-registry
    restart: always
    depends_on:
      kafka:
        condition: service_healthy
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:29092
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
EOF

cd /opt/kafka && docker compose up -d --wait --wait-timeout 120

sleep 15
for TOPIC in "crypto.trades:10" "crypto.market_meta:3" "crypto.ohlcv_1m:10" "crypto.trades.dlq:3"; do
  NAME="${TOPIC%%:*}"; PARTS="${TOPIC##*:}"
  docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create \
    --topic $NAME --partitions $PARTS --replication-factor 1
done

# Systemd service for auto-start on reboot
cat > /etc/systemd/system/crypto-kafka.service << 'SVC'
[Unit]
Description=Crypto Kafka Stack
Requires=docker.service
After=docker.service network-online.target
[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/opt/kafka
ExecStart=/usr/bin/docker compose up -d
ExecStop=/usr/bin/docker compose down
TimeoutStartSec=300
[Install]
WantedBy=multi-user.target
SVC
systemctl daemon-reload && systemctl enable crypto-kafka.service
echo "Kafka bootstrap complete"