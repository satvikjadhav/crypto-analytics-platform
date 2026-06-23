#!/bin/bash
cd "$(dirname "$0")/../terraform"
KAFKA_ID=$(terraform output -raw kafka_instance_id)
AIRFLOW_ID=$(terraform output -raw airflow_instance_id)
SPARK_ID=$(terraform output -raw spark_instance_id)

echo "Stopping EC2 instances..."
aws ec2 stop-instances \
  --instance-ids $KAFKA_ID $AIRFLOW_ID $SPARK_ID \
  --region us-east-1
echo "Done. EIPs preserved. Run ec2-start.sh to resume."