resource "aws_instance" "airflow" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = "t3.small"
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.airflow.id]
  key_name               = aws_key_pair.crypto.key_name
  iam_instance_profile   = aws_iam_instance_profile.airflow.name

  root_block_device {
    volume_size = 20
    volume_type = "gp3"
  }

  user_data = templatefile("${path.module}/bootstrap/airflow.sh", {
    kafka_private_ip = aws_instance.kafka.private_ip
    spark_private_ip = aws_instance.spark.private_ip
    fernet_key       = var.airflow_fernet_key
    git_repo_url     = var.git_repo_url
  })

  tags = {
    Name = "crypto-airflow"
  }

  depends_on = [
    aws_instance.kafka,
    aws_instance.spark
  ]
}

resource "aws_eip" "airflow" {
  instance = aws_instance.airflow.id
  domain   = "vpc"

  tags = {
    Name = "crypto-airflow-eip"
  }
}

resource "aws_iam_role" "airflow" {
  name = "crypto-airflow-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "airflow" {
  role = aws_iam_role.airflow.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = [
          aws_secretsmanager_secret.snowflake.arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_instance_profile" "airflow" {
  name = "crypto-airflow-profile"
  role = aws_iam_role.airflow.name
}

output "airflow_public_ip" {
  value = aws_eip.airflow.public_ip
}

output "airflow_private_ip" {
  value = aws_instance.airflow.private_ip
}

output "airflow_instance_id" {
  value = aws_instance.airflow.id
}

output "airflow_url" {
  value = "http://${aws_eip.airflow.public_ip}:8080"
}