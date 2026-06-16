terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
    http = { source = "hashicorp/http", version = "~> 3.0" }
  }
  backend "s3" {
    bucket         = "tf-state-crypto-analytics-sj"
    key            = "crypto-analytics/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
  }
}

provider "aws" {
    region = var.aws_region
    default_tags {
        tags = {
            Project = "crypto-analytics-platform"
            Environment = "var.environment"
            ManagedBy = "terraform"
        }
    }
}