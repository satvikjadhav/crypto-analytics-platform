#!/bin/bash
set -euo pipefail
exec > /var/log/bootstrap.log 2>&1

# ---------------------------------------------------------------------------
# Fetch instance IPs via IMDSv2
# ---------------------------------------------------------------------------
TOKEN=$(curl -sf -X PUT "http://169.254.169.254/latest/api/token" \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")

private_ip=$(curl -sf -H "X-aws-ec2-metadata-token: $TOKEN" \
  http://169.254.169.254/latest/meta-data/local-ipv4)

# External clients connect via the EIP — advertise the public IP on port 9092
public_ip=$(curl -sf -H "X-aws-ec2-metadata-token: $TOKEN" \
  http://169.254.169.254/latest/meta-data/public-ipv4)

# ---------------------------------------------------------------------------
# System deps
# ---------------------------------------------------------------------------
apt-get update -y
apt-get install -y docker.io docker-compose-v2 awscli
systemctl enable docker && systemctl start docker
usermod -aG docker ubuntu

# ---------------------------------------------------------------------------
# Kafka stack (KRaft — no ZooKeeper)
# ---------------------------------------------------------------------------
mkdir -p /opt/kafka

cat > /opt/kafka/docker-compose.yml << EOF
version: '3.8'
services:

  kafka:
    image: confluentinc/cp-kafka:7.7.0
    container_name: kafka
    restart: always
    ports:
      - "9092:9092"   # external clients
      - "29092:29092" # internal (intra-VPC / schema-registry)
    environment:
      # ---- KRaft identity ------------------------------------------------
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      # Pre-generated cluster UUID — must stay stable across restarts.
      # Regenerate with: kafka-storage random-uuid
      CLUSTER_ID: "Mk3OEVBNTHqLeD_HqJR05A"

      # ---- Listeners ------------------------------------------------------
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_LISTENERS: CONTROLLER://0.0.0.0:9093,INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://${private_ip}:29092,EXTERNAL://${public_ip}:9092
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER

      # ---- KRaft quorum (single node: this broker IS the controller) ------
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@localhost:9093"

      # ---- Broker tuning --------------------------------------------------
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"

      # ---- Log retention (keep disk usage bounded) -----------------------
      KAFKA_LOG_RETENTION_HOURS: 24
      KAFKA_LOG_RETENTION_BYTES: "10737418240"   # 10 GB per partition
      KAFKA_LOG_SEGMENT_BYTES: "536870912"        # 512 MB segments

    volumes:
      - kafka-data:/var/lib/kafka/data
    healthcheck:
      test: ["CMD", "kafka-broker-api-versions", "--bootstrap-server", "localhost:9092"]
      interval: 15s
      timeout: 10s
      retries: 10
      start_period: 30s

  schema-registry:
    image: confluentinc/cp-schema-registry:7.7.0
    container_name: schema-registry
    restart: always
    depends_on:
      kafka:
        condition: service_healthy
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      # Use the internal listener — schema-registry is co-located in the VPC
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: ${private_ip}:29092
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
    healthcheck:
      test: ["CMD", "curl", "-sf", "http://localhost:8081/subjects"]
      interval: 15s
      timeout: 5s
      retries: 8
      start_period: 20s

volumes:
  kafka-data:
EOF

# ---------------------------------------------------------------------------
# Start the stack and wait for all healthchecks to pass
# ---------------------------------------------------------------------------
cd /opt/kafka && docker compose up -d --wait --wait-timeout 180

# ---------------------------------------------------------------------------
# Create topics (idempotent — safe on re-runs or reboots)
# ---------------------------------------------------------------------------
# Format: "topic-name:partitions"
TOPICS=(
  "crypto.trades:10"
  "crypto.market_meta:3"
  "crypto.ohlcv_1m:10"
  "crypto.trades.dlq:3"
)

for TOPIC in "${TOPICS[@]}"; do
  NAME="${TOPIC%%:*}"
  PARTS="${TOPIC##*:}"
  docker exec kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --create \
    --if-not-exists \
    --topic "$NAME" \
    --partitions "$PARTS" \
    --replication-factor 1
  echo "Topic ready: $NAME ($PARTS partitions)"
done

# ---------------------------------------------------------------------------
# Systemd unit — restarts the stack after reboot
# ---------------------------------------------------------------------------
cat > /etc/systemd/system/crypto-kafka.service << 'SVC'
[Unit]
Description=Crypto Kafka Stack (KRaft)
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

echo "Kafka KRaft bootstrap complete — cluster ID: Mk3OEVBNTHqLeD_HqJR05A"