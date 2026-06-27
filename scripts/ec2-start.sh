#!/bin/bash
set -euo pipefail

# ---------------------------------------------------------------------------
# ec2-stop.sh  –  Stop all project EC2 instances (EIPs are preserved)
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

# ── Stop ────────────────────────────────────────────────────────────────────
echo ""
echo "Stopping EC2 instances..."
aws ec2 stop-instances \
  --instance-ids "$KAFKA_ID" "$AIRFLOW_ID" "$SPARK_ID" \
  --region "$REGION" \
  --output table

# ── Wait ────────────────────────────────────────────────────────────────────
echo ""
echo "Waiting for all instances to reach 'stopped' state..."
aws ec2 wait instance-stopped \
  --instance-ids "$KAFKA_ID" "$AIRFLOW_ID" "$SPARK_ID" \
  --region "$REGION"

echo ""
echo "All instances stopped. EIPs are preserved."
echo "Run ec2-start.sh to resume."