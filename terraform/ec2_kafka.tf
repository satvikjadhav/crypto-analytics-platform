data "aws_ami" "ubuntu" {
    most_recent = true
    owners      = ["099720109477"] # Canonical
    filter {
        name   = "name"
        values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
    }
    filter {
        name   = "virtualization-type"
        values = ["hvm"]
    }
}

resource "aws_instance" "kafka" {
    ami = data.aws_ami.ubuntu.id
    instance_type = "t3.medium"
    subnet_id = aws_subnet.public.id
    vpc_security_group_ids = [ aws_security_group.kafka.id ]
    key_name = aws_key_pair.crypto.key_name
    root_block_device {
      volume_size = 30
      volume_type = "gp3"
    }
    user_data = templatefile("${path.module}/bootstrap/kafka.sh", {
        private_ip = self.private_ip
    })
    tags = {
      Name = "crypto-kafka"
    }
}

resource "aws_eip" "kafka" {
    instance = aws_instance.kafka.id
    domain = "vpc"
    tags = {
      Name = "crypto-kafka-eip"
    }
}

output "kafka_public_ip"   { value = aws_eip.kafka.public_ip }
output "kafka_private_ip"  { value = aws_instance.kafka.private_ip }
output "kafka_instance_id" { value = aws_instance.kafka.id }