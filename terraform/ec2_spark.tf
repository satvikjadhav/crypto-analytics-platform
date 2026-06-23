resource "aws_instance" "spark" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = "t3.large"
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

  lifecycle {
    create_before_destroy = true
  }

  tags = { Name = "crypto-spark" }
  # depends_on removed: private_ip reference already creates implicit dependency
}

resource "aws_eip" "spark" {
  instance = aws_instance.spark.id
  domain   = "vpc"
  tags     = { Name = "crypto-spark-eip" }
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
      },
      {
        # Allows SSM Session Manager access without needing open SSH port
        Effect = "Allow"
        Action = [
          "ssm:UpdateInstanceInformation",
          "ssmmessages:CreateControlChannel",
          "ssmmessages:CreateDataChannel",
          "ssmmessages:OpenControlChannel",
          "ssmmessages:OpenDataChannel",
          "ec2messages:AcknowledgeMessage",
          "ec2messages:DeleteMessage",
          "ec2messages:FailMessage",
          "ec2messages:GetEndpoint",
          "ec2messages:GetMessages",
          "ec2messages:SendReply"
        ]
        Resource = ["*"]
      }
    ]
  })
}

resource "aws_iam_instance_profile" "spark" {
  name = "crypto-spark-profile"
  role = aws_iam_role.spark.name
}

output "spark_public_ip"   { value = aws_eip.spark.public_ip }
output "spark_private_ip"  { value = aws_instance.spark.private_ip }
output "spark_instance_id" { value = aws_instance.spark.id }
output "spark_ui_url"      { value = "http://${aws_eip.spark.public_ip}:8080" }