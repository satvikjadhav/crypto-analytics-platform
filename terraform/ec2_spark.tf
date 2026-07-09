resource "aws_instance" "spark" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = "m7i-flex.large"
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.spark.id]
  key_name               = aws_key_pair.crypto.key_name
  iam_instance_profile   = aws_iam_instance_profile.spark.name
  ebs_optimized          = true
  monitoring             = true

  root_block_device {
    volume_size = 40
    volume_type = "gp3"
  }

  user_data = templatefile("${path.module}/bootstrap/spark.sh", {
    kafka_private_ip = aws_instance.kafka.private_ip
    s3_bucket        = aws_s3_bucket.data_lake.bucket
    aws_region       = var.aws_region
  })

  # Ignore user_data changes to prevent instance recreation on script updates.
  # Apply bootstrap changes out-of-band or via a new AMI.
  lifecycle {
    ignore_changes = [user_data]
  }

  tags = { Name = "crypto-spark" }
}

resource "aws_eip" "spark" {
  instance   = aws_instance.spark.id
  domain     = "vpc"
  depends_on = [aws_instance.spark]
  tags       = { Name = "crypto-spark-eip" }
}

resource "aws_iam_role" "spark" {
  name = "crypto-spark-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "spark_s3" {
  role = aws_iam_role.spark.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy" "spark_secrets" {
  name = "crypto-spark-secrets-policy"
  role = aws_iam_role.spark.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = "arn:aws:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:crypto-analytics/snowflake/pipeline*"
      }
    ]
  })
}

# AmazonSSMManagedInstanceCore covers all SSM/ec2messages/ssmmessages permissions
# and stays current as AWS updates the SSM agent — preferred over an inline policy.
resource "aws_iam_role_policy_attachment" "spark_ssm" {
  role       = aws_iam_role.spark.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "spark" {
  name = "crypto-spark-profile"
  role = aws_iam_role.spark.name
}

output "spark_public_ip" { value = aws_eip.spark.public_ip }
output "spark_private_ip" { value = aws_instance.spark.private_ip }
output "spark_instance_id" { value = aws_instance.spark.id }
# spark_ui_url intentionally omitted: Spark web UI has no auth by default.
# Restrict the security group to known CIDRs or add a reverse proxy before exposing this.
