variable "aws_region" {
  description = "The AWS region to deploy resources in."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "The deployment environment (e.g., dev, staging, prod)."
  type        = string
  default     = "dev"
}

variable "initials" {
  description = "Appended to globally unique names."
  type        = string
}

variable "snowflake_account" {
  description = "Snowflake account identifier (e.g. xy12345.us-east-1)"
  type        = string
}

variable "snowflake_user" {
  description = "Snowflake admin user for Terraform (your login user)"
  type        = string
}

variable "snowflake_password" {
  description = "Snowflake admin password for Terraform"
  type        = string
  sensitive   = true
}

variable "snowflake_pipeline_password" {
  description = "Password for the dedicated pipeline service user"
  type        = string
  sensitive   = true
}