data "aws_ami" "ubuntu_22_serving" {
  most_recent = true
  owners      = ["099720109477"]  # Canonical
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }
}

resource "aws_instance" "serving" {
  ami                    = data.aws_ami.ubuntu_22_serving.id
  instance_type          = "c7i-flex.large"
  key_name               = aws_key_pair.crypto.key_name  # fixed: was aws_key_pair.main
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.serving.id]
  iam_instance_profile   = aws_iam_instance_profile.serving.name

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
  }

  user_data = <<-EOF
    #!/bin/bash
    set -e
    apt-get update -y
    apt-get install -y ca-certificates curl gnupg lsb-release git
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor \
      -o /usr/share/keyrings/docker-archive-keyring.gpg
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] \
      https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
      > /etc/apt/sources.list.d/docker.list
    apt-get update -y
    apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
    usermod -aG docker ubuntu
    mkdir -p /opt/crypto
    echo "Bootstrap complete at $(date)" >> /var/log/user-data.log
  EOF

  tags = { Name = "crypto-serving" }
}

# IAM role for the serving instance
resource "aws_iam_role" "serving" {
  name = "crypto-serving-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })

  tags = { Name = "crypto-serving-role" }
}

# Instance profile to attach the role to EC2
resource "aws_iam_instance_profile" "serving" {
  name = "crypto-serving-profile"
  role = aws_iam_role.serving.name
}

# IAM policy to allow the serving instance to access secrets in Secrets Manager
resource "aws_iam_role_policy" "serving_secrets" {
  name = "crypto-serving-secrets-policy"
  role = aws_iam_role.serving.name

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

resource "aws_eip" "serving" {
  instance = aws_instance.serving.id
  domain   = "vpc"
  tags     = { Name = "crypto-serving-eip" }
}

output "superset_url" {
  value = "http://${aws_eip.serving.public_ip}:8088"
}

output "streamlit_url" {
  value = "http://${aws_eip.serving.public_ip}:8501"
}

output "superset_streamlit_instance_id" {
  value = aws_instance.serving.id
}