#!/bin/bash
set -euo pipefail
exec > /var/log/bootstrap.log 2>&1

apt-get update -y
apt-get install -y docker.io docker-compose-v2 git awscli
systemctl enable docker && systemctl start docker
usermod -aG docker ubuntu

git clone ${git_repo_url} /opt/airflow/repo
mkdir -p /opt/airflow/logs /opt/airflow/plugins

# Fix: Airflow container runs as UID 50000; grant write access before any container starts
chown -R 50000:0 /opt/airflow/logs /opt/airflow/plugins
chmod -R 775 /opt/airflow/logs /opt/airflow/plugins

cat > /opt/airflow/docker-compose.yml << 'EOF'
version: '3.8'
x-airflow-common: &airflow-common
  image: apache/airflow:2.8.1
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    AIRFLOW__CORE__FERNET_KEY: '${fernet_key}'
    KAFKA_BOOTSTRAP_SERVERS: '${kafka_private_ip}:9092'
    SPARK_MASTER_URL: 'spark://${spark_private_ip}:7077'
  # Fix: explicitly set the user so container permissions match host chown above
  user: "50000:0"
  volumes:
    - /opt/airflow/repo/dags:/opt/airflow/dags
    - /opt/airflow/logs:/opt/airflow/logs
    - /opt/airflow/plugins:/opt/airflow/plugins
  depends_on:
    postgres:
      condition: service_healthy

services:
  postgres:
    image: postgres:15
    container_name: airflow-postgres
    restart: always
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 10s
      timeout: 5s
      retries: 5

  airflow-webserver:
    <<: *airflow-common
    container_name: airflow-webserver
    restart: always
    command: webserver
    ports:
      - "8080:8080"
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s

  airflow-scheduler:
    <<: *airflow-common
    container_name: airflow-scheduler
    restart: always
    command: scheduler

volumes:
  postgres_data:
EOF

cd /opt/airflow

# Start postgres and wait for it to be healthy
docker compose up -d postgres

echo "Waiting for postgres to be healthy..."
for i in $(seq 1 30); do
  if docker compose exec -T postgres pg_isready -U airflow > /dev/null 2>&1; then
    echo "Postgres is ready."
    break
  fi
  echo "  attempt $i/30 — sleeping 5s..."
  sleep 5
done

# Fix: use a one-shot init container instead of reusing the webserver image ad-hoc,
# and ensure logs dir exists with correct ownership before db init runs
docker compose run --rm \
  --user "50000:0" \
  airflow-webserver airflow db init

docker compose run --rm \
  --user "50000:0" \
  airflow-webserver airflow users create \
    --username admin \
    --password admin \
    --role Admin \
    --email admin@example.com \
    --firstname Admin \
    --lastname User

docker compose up -d airflow-webserver airflow-scheduler

# DAG auto-deploy cron
cat > /opt/airflow/sync_dags.sh << 'SCRIPT'
#!/bin/bash
cd /opt/airflow/repo && git pull origin main >> /var/log/dag_sync.log 2>&1
SCRIPT
chmod +x /opt/airflow/sync_dags.sh
echo "*/5 * * * * ubuntu /opt/airflow/sync_dags.sh" > /etc/cron.d/dag-sync

# Systemd service
cat > /etc/systemd/system/crypto-airflow.service << 'SVC'
[Unit]
Description=Crypto Airflow Stack
Requires=docker.service
After=docker.service network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/opt/airflow
ExecStart=/usr/bin/docker compose up -d
ExecStop=/usr/bin/docker compose down
TimeoutStartSec=300

[Install]
WantedBy=multi-user.target
SVC

systemctl daemon-reload && systemctl enable crypto-airflow.service
echo "Airflow bootstrap complete"