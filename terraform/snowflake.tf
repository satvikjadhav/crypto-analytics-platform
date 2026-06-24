# Virtual warehouse (compute)
resource "snowflake_warehouse" "pipeline" {
  name                = "CRYPTO_PIPELINE_WH"
  warehouse_size      = "XSMALL" # cheapest — 1 credit/hour when running
  auto_suspend        = 60       # suspends after 60s of inactivity
  auto_resume         = true
  initially_suspended = true
}

# Database
resource "snowflake_database" "crypto" {
  name    = "CRYPTO_ANALYTICS_DB"
  comment = "Database for crypto analytics platform"
}

# Schemas
resource "snowflake_schema" "raw" {
  name     = "RAW"
  database = snowflake_database.crypto.name
  comment  = "Schema for raw ingested data"
}

resource "snowflake_schema" "staging" {
  name     = "STAGING"
  database = snowflake_database.crypto.name
  comment  = "Schema for transformed data ready for analysis"
}

resource "snowflake_schema" "marts" {
  name     = "MARTS"
  database = snowflake_database.crypto.name
  comment  = "Schema for data marts used by analysts and BI tools"
}

# FIX 1: Add the missing random_password resource
resource "random_password" "pipeline" {
  length           = 32
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>:?"
}

# dedicated service user for the pipeline
resource "snowflake_user" "pipeline" {
  name                 = "CRYPTO_PIPELINE_USER"
  password             = random_password.pipeline.result
  default_warehouse    = snowflake_warehouse.pipeline.name
  must_change_password = false
}

# Role with least-privilege access for the pipeline
resource "snowflake_role" "pipeline" {
  name = "CRYPTO_PIPELINE_ROLE"
}

# FIX 2: snowflake_grant_privileges_to_role → snowflake_grant_privileges_to_account_role
resource "snowflake_grant_privileges_to_account_role" "warehouse_usage" {
  account_role_name = snowflake_role.pipeline.name
  privileges        = ["USAGE", "OPERATE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.pipeline.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "db_usage" {
  account_role_name = snowflake_role.pipeline.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.crypto.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "schema_all" {
  for_each          = toset([snowflake_schema.raw.name, snowflake_schema.staging.name, snowflake_schema.marts.name])
  account_role_name = snowflake_role.pipeline.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW", "CREATE STAGE"]
  on_schema {
    schema_name = "\"${snowflake_database.crypto.name}\".\"${each.value}\""
  }
}

# FIX 3: snowflake_role_grant → snowflake_grant_account_role
resource "snowflake_grant_account_role" "pipeline_user" {
  role_name = snowflake_role.pipeline.name
  user_name = snowflake_user.pipeline.name
}


# AWS store snowflake credentials in secrets manager
resource "aws_secretsmanager_secret" "snowflake" {
  name                    = "crypto-analytics/snowflake/pipeline"
  description             = "Snowflake credentials for the crypto analytics pipeline"
  recovery_window_in_days = 7
}

resource "aws_secretsmanager_secret_version" "snowflake" {
  secret_id = aws_secretsmanager_secret.snowflake.id
  secret_string = jsonencode({
    account   = var.snowflake_account,
    username  = snowflake_user.pipeline.name,
    password  = snowflake_user.pipeline.password
    database  = snowflake_database.crypto.name,
    warehouse = snowflake_warehouse.pipeline.name,
    role      = snowflake_role.pipeline.name
  })
}

# IAM role that Snowflake assumes to read your S3 bucket
# ── Step 1: IAM role with a bootstrap trust policy ─────────────────────────
resource "aws_iam_role" "snowflake_s3_role" {
  name = "crypto-snowflake-s3-role"

  # Placeholder: lets Terraform create the role without needing the
  # storage integration to exist yet. The null_resource below overwrites
  # this with the real trust policy after the integration is created.
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = "arn:aws:iam::000000000000:root" } # placeholder
      Action    = "sts:AssumeRole"
    }]
  })

  lifecycle {
    ignore_changes = [assume_role_policy] # Don't revert the real policy on re-apply
  }
}

# ── Step 2: S3 permissions (unchanged) ─────────────────────────────────────
resource "aws_iam_role_policy" "snowflake_s3" {
  role = aws_iam_role.snowflake_s3_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow" # <-- was lowercase "effect" in your original, fix this too
      Action = [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:ListBucket"
      ]
      Resource = [
        aws_s3_bucket.data_lake.arn,
        "${aws_s3_bucket.data_lake.arn}/*"
      ]
    }]
  })
}

# ── Step 3: Storage integration (no cycle — role already exists) ───────────
resource "snowflake_storage_integration" "s3" {
  name    = "CRYPTO_S3_INTEGRATION"
  type    = "EXTERNAL_STAGE"
  enabled = true

  storage_provider     = "S3"
  storage_aws_role_arn = aws_iam_role.snowflake_s3_role.arn
  storage_allowed_locations = [
    "s3://${aws_s3_bucket.data_lake.bucket}/raw/",
    "s3://${aws_s3_bucket.data_lake.bucket}/curated/",
  ]
}

# ── Step 4: Patch the real trust policy now that integration outputs exist ──
resource "null_resource" "snowflake_trust_policy_patch" {
  triggers = {
    iam_user_arn = snowflake_storage_integration.s3.storage_aws_iam_user_arn
    external_id  = snowflake_storage_integration.s3.storage_aws_external_id
  }

  provisioner "local-exec" {
    command = <<-EOT
      aws iam update-assume-role-policy \
        --role-name ${aws_iam_role.snowflake_s3_role.name} \
        --policy-document '{
          "Version": "2012-10-17",
          "Statement": [{
            "Effect": "Allow",
            "Principal": { "AWS": "${snowflake_storage_integration.s3.storage_aws_iam_user_arn}" },
            "Action": "sts:AssumeRole",
            "Condition": {
              "StringEquals": {
                "sts:ExternalId": "${snowflake_storage_integration.s3.storage_aws_external_id}"
              }
            }
          }]
        }'
    EOT
  }
}