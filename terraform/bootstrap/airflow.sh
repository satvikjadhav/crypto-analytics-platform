#!/bin/bash
set -euo pipefail
exec > /var/log/bootstrap.log 2>&1

apt-get update -y
apt-get install -y docker.io docker-compose-v2 git awscli python3-pip
systemctl enable docker && systemctl start docker
usermod -aG docker ubuntu

# ── Generate a valid Fernet key on the instance ──────────────────────────────
# Python's cryptography library guarantees a correct 32-byte URL-safe base64 key.
FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")

# Persist it to Secrets Manager so it survives instance replacement
aws secretsmanager put-secret-value \
  --region ${aws_region} \
  --secret-id crypto-airflow/fernet-key \
  --secret-string "$FERNET_KEY" \
  || echo "WARNING: could not write Fernet key to Secrets Manager"

echo "Generated Fernet key: $FERNET_KEY"  # visible in /var/log/bootstrap.log only

# ── Clone DAG repo ────────────────────────────────────────────────────────────
git clone ${git_repo_url} /opt/airflow/repo
mkdir -p /opt/airflow/logs /opt/airflow/plugins

# Airflow container runs as UID 50000
chown -R 50000:0 /opt/airflow/logs /opt/airflow/plugins
chmod -R 775 /opt/airflow/logs /opt/airflow/plugins

# ── Write docker-compose.yml ──────────────────────────────────────────────────
# Note: $FERNET_KEY is a shell variable here — NOT a Terraform template variable.
# We close the heredoc with a quoted 'EOF' to prevent Terraform templatefile()
# from trying to interpolate $FERNET_KEY as a template variable.
cat > /opt/airflow/docker-compose.yml << COMPOSE_EOF
version: '3.8'
x-airflow-common: &airflow-common
  image: .
  user: "50000:0"
  environment:
    _PIP_ADDITIONAL_REQUIREMENTS: "dbt-snowflake==1.10.2 dbt-core==2.0.0a2"
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    AIRFLOW__CORE__FERNET_KEY: '$FERNET_KEY'
    KAFKA_BOOTSTRAP_SERVERS: '${kafka_private_ip}:9092'
    SPARK_MASTER_URL: 'spark://${spark_private_ip}:7077'
    # pulls from .env file in the repo
    SNOWFLAKE_ACCOUNT:
    SNOWFLAKE_USER:
    SNOWFLAKE_PASSWORD:
    SNOWFLAKE_DATABASE:
    SNOWFLAKE_WAREHOUSE:
    SNOWFLAKE_SCHEMA:
    SNOWFLAKE_ROLE:
  volumes:
    - /opt/airflow/repo/dags:/opt/airflow/dags
    - /opt/airflow/logs:/opt/airflow/logs
    - /opt/airflow/plugins:/opt/airflow/plugins
    - /opt/airflow/repo/producers:/opt/airflow/repo/producers
    - /opt/airflow/repo/dbt:/opt/airflow/dbt 
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
COMPOSE_EOF

# ── Start postgres and wait ───────────────────────────────────────────────────
cd /opt/airflow
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

# ── Init DB and create admin user ─────────────────────────────────────────────
docker compose run --rm --user "50000:0" airflow-webserver airflow db migrate

docker compose run --rm --user "50000:0" airflow-webserver airflow users create \
  --username admin \
  --password admin \
  --role Admin \
  --email admin@example.com \
  --firstname Admin \
  --lastname User

# ── Start all services ────────────────────────────────────────────────────────
docker compose up -d airflow-webserver airflow-scheduler

cat > /opt/airflow/sync_dags.sh << 'SCRIPT'
#!/bin/bash
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
cd /opt/airflow/repo && git pull origin main >> /opt/airflow/dag_sync.log 2>&1
SCRIPT
chmod +x /opt/airflow/sync_dags.sh

# Pre-create the log file with correct ownership
touch /opt/airflow/dag_sync.log
chown ubuntu:ubuntu /opt/airflow/dag_sync.log

printf '*/5 * * * * ubuntu /opt/airflow/sync_dags.sh\n' > /etc/cron.d/dag-sync
chmod 644 /etc/cron.d/dag-sync

# ── Systemd service ───────────────────────────────────────────────────────────
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