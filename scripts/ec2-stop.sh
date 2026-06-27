#!/bin/bash
set -euo pipefail

# ---------------------------------------------------------------------------
# ec2-start.sh  –  Start all project EC2 instances and print endpoints
# ---------------------------------------------------------------------------

REGION="${AWS_DEFAULT_REGION:-us-east-1}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ── Preflight checks ────────────────────────────────────────────────────────
for cmd in aws terraform; do
  command -v "$cmd" &>/dev/null || { echo "ERROR: '$cmd' not found in PATH."; exit 1; }
done

cd "$SCRIPT_DIR/../terraform"

# ── Resolve instance IDs ────────────────────────────────────────────────────
echo "Resolving instance IDs from Terraform state..."
KAFKA_ID=$(terraform output -raw kafka_instance_id 2>/dev/null)
AIRFLOW_ID=$(terraform output -raw airflow_instance_id 2>/dev/null)
SPARK_ID=$(terraform output -raw spark_instance_id 2>/dev/null)

for var in KAFKA_ID AIRFLOW_ID SPARK_ID; do
  [[ -z "${!var}" ]] && { echo "ERROR: Could not resolve $var from Terraform output."; exit 1; }
done

echo "  Kafka:   $KAFKA_ID"
echo "  Airflow: $AIRFLOW_ID"
echo "  Spark:   $SPARK_ID"

# ── Start ────────────────────────────────────────────────────────────────────
echo ""
echo "Starting EC2 instances..."
aws ec2 start-instances \
  --instance-ids "$KAFKA_ID" "$AIRFLOW_ID" "$SPARK_ID" \
  --region "$REGION" \
  --output table

# ── Wait for running ─────────────────────────────────────────────────────────
echo ""
echo "Waiting for all instances to reach 'running' state..."
aws ec2 wait instance-running \
  --instance-ids "$KAFKA_ID" "$AIRFLOW_ID" "$SPARK_ID" \
  --region "$REGION"
echo "Instances are running."

# ── Resolve public IPs (after instances are up) ──────────────────────────────
# Refresh outputs now that instances have started and EIPs have re-associated.
echo ""
echo "Resolving public IPs..."
AIRFLOW_IP=$(terraform output -raw airflow_public_ip 2>/dev/null)
SPARK_IP=$(terraform output -raw spark_public_ip 2>/dev/null)
KAFKA_IP=$(terraform output -raw kafka_public_ip 2>/dev/null)

for var in AIRFLOW_IP SPARK_IP KAFKA_IP; do
  [[ -z "${!var}" ]] && { echo "WARNING: Could not resolve $var – check Terraform outputs."; }
done

# ── Print endpoints ──────────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════"
echo " Service Endpoints"
echo "════════════════════════════════════════"
echo "  Airflow UI : http://${AIRFLOW_IP}:8080"
echo "  Spark UI   : http://${SPARK_IP}:8080"
echo "  Kafka      : ${KAFKA_IP}:9092"
echo "════════════════════════════════════════"