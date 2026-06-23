#!/bin/bash
cd "$(dirname "$0")/../terraform"
KAFKA_ID=$(terraform output -raw kafka_instance_id)
AIRFLOW_ID=$(terraform output -raw airflow_instance_id)
SPARK_ID=$(terraform output -raw spark_instance_id)

echo "Starting EC2 instances..."
aws ec2 start-instances \
  --instance-ids $KAFKA_ID $AIRFLOW_ID $SPARK_ID \
  --region us-east-1

aws ec2 wait instance-running \
  --instance-ids $KAFKA_ID $AIRFLOW_ID $SPARK_ID \
  --region us-east-1

echo ""
echo "Airflow: http://$(terraform output -raw airflow_public_ip):8080"
echo "Spark:   http://$(terraform output -raw spark_public_ip):8080"
echo "Kafka:   $(terraform output -raw kafka_public_ip):9092"