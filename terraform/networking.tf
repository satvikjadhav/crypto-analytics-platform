data "http" "my_ip" {
  url = "https://checkip.amazonaws.com/"
}

locals {
  my_cidr = "${chomp(data.http.my_ip.response_body)}/32"
}

# ── Networking ────────────────────────────────────────

resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = { Name = "crypto-analytics-vpc" }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = { Name = "crypto-analytics-igw" }
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = "${var.aws_region}a"
  map_public_ip_on_launch = true

  tags = { Name = "crypto-analytics-public" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = { Name = "crypto-analytics-rt" }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

# ── Key Pair ──────────────────────────────────────────

resource "aws_key_pair" "crypto" {
  key_name   = "crypto-analytics-key"
  public_key = file(var.ssh_public_key_path)
}

# ── Kafka security group ──────────────────────────────

resource "aws_security_group" "kafka" {
  name   = "crypto-kafka-sg"
  vpc_id = aws_vpc.main.id

  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
    description = "Kafka from VPC"
  }

  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = [local.my_cidr]
    description = "Kafka from laptop"
  }

  ingress {
    from_port   = 8081
    to_port     = 8081
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
    description = "Schema Registry from VPC"
  }

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [local.my_cidr]
    description = "SSH"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "crypto-kafka-sg" }
}

# ── Airflow security group ────────────────────────────

resource "aws_security_group" "airflow" {
  name   = "crypto-airflow-sg"
  vpc_id = aws_vpc.main.id

  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = [local.my_cidr]
    description = "Airflow UI from laptop"
  }

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [local.my_cidr]
    description = "SSH"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "crypto-airflow-sg" }
}

# ── Spark security group ──────────────────────────────

resource "aws_security_group" "spark" {
  name   = "crypto-spark-sg"
  vpc_id = aws_vpc.main.id

  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = [local.my_cidr]
    description = "Spark UI from laptop"
  }

  ingress {
    from_port   = 7077
    to_port     = 7077
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
    description = "Spark cluster port from VPC"
  }

  ingress {
    from_port   = 4040
    to_port     = 4050
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
    description = "Spark driver/executor comms"
  }

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [local.my_cidr]
    description = "SSH"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "crypto-spark-sg" }
}