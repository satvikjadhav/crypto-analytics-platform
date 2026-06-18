# Virtual warehouse (compute)
resource "snowflake_warehouse" "pipeline" {
    name = "CRYPTO_PIPELINE_WH"
    warehouse_size = "XSMALL"   # cheapest — 1 credit/hour when running
    auto_suspend = 60           # suspends after 60s of inactivity
    auto_resume = true
    initially_suspended = true
}

# Database
resource "snowflake_database" "crypto" {
    name = "CRYPTO_ANALYTICS_DB"
    comment = "Database for crypto analytics platform"
}

# Schemas
resource "snowflake_schema" "raw" {
    name = "RAW"
    database = snowflake_database.crypto.name
    comment = "Schema for raw ingested data"
}

resource "snowflake_schema" "staging" {
    name = "STAGING"
    database = snowflake_database.crypto.name
    comment = "Schema for transformed data ready for analysis"
}

resource "snowflake_schema" "marts" {
    name = "MARTS"
    database = snowflake_database.crypto.name
    comment = "Schema for data marts used by analysts and BI tools"
}

# dedicated service user for the pipeline
resource "snowflake_user" "pipeline" {
    name = "CRYPTO_PIPELINE_USER"
    password = random_password.pipeline.result
    default_warehouse = snowflake_warehouse.pipeline.name
    must_change_password = false
}

# Role with least-privilege access for the pipeline
resource "snowflake_role" "pipeline" {
    name = "CRYPTO_PIPELINE_ROLE"
}

resource "snowflake_grant_privileges_to_role" "warehouse_usage" {
    role_name = snowflake_role.pipeline.name
    privileges = ["USAGE", "OPERATE"]
    on_account_object = {
        object_type = "WAREHOUSE"
        object_name = snowflake_warehouse.pipeline.name
    }
}

resource "snowflake_grant_privileges_to_role" "db_usage" {
    role_name = snowflake_role.pipeline.name
    privileges = ["USAGE"]
    on_account_object = {
        object_type = "DATABASE"
        object_name = snowflake_database.crypto.name
    }
}

resource "snowflake_grant_privileges_to_role" "schema_all" {
    for_each = toset([snowflake_schema.raw.name, snowflake_schema.staging.name, snowflake_schema.marts.name])
    role_name = snowflake_role.pipeline.name
    privileges = ["USAGE", "CREATE TABLE", "CREATE VIEW", "CREATE STAGE"]
    on_schema {
        schema_name = "\"${snowflake_database.crypto.name}\".\"${each.value}\""
    }
}

resource "snowflake_role_grant" "pipeline_user" {
    role_name = snowflake_role.pipeline.name
    users = [snowflake_user.pipeline.name]
}


# AWS store snowflake credentials in secrets manager
resource "aws_secretsmanager_secret" "snowflake" {
    name = "crypto-analytics/snowflake/pipeline"
    description = "Snowflake credentials for the crypto analytics pipeline"
    recovery_window_in_days = 7
}

resource "aws_secretsmanager_secret_version" "snowflake" {
    secret_id = aws_secretsmanager_secret.snowflake.id
    secret_string = jsonencode({
        account = var.snowflake_account,
        username = snowflake_user.pipeline.name,
        password = snowflake_user.pipeline.password
        database = snowflake_database.crypto.name,
        warehouse = snowflake_warehouse.pipeline.name,
        role = snowflake_role.pipeline.name
    })
}

# IAM role that Snowflake assumes to read your S3 bucket
resource "aws_iam_role" "snowflake_s3_role" {
    name = "crypto-snowflake-s3-role"

    # Trust policy — Snowflake's AWS account assumes this role
    # The exact principal is output by the storage integration after apply
    assume_role_policy = jsonencode({
        ersion = "2012-10-17"
        Statement = [
            {
                Effect = "Allow"
                Principal = {
                    AWS = "arn:aws:iam::${snowflake_storage_integration.s3.storage_aws_iam_user_arn}:root"
                }
                Action = "sts:AssumeRole"
                Condition = {
                    StringEquals = {
                        "sts:ExternalId" = snowflake_storage_integration.s3.storage_aws_external_id
                    }
                }
            }
        ]
    })
}

resource "aws_iam_role_policy" "snowflake_s3" {
    role = aws_iam_role.snowflake_s3_role.id
    policy = jsonencode({
        Version = "2012-10-17"
        Statement = [
            {
                effect = "Allow"
                Action = [
                    "s3:GetObject",
                    "s3:GetObjectVersion",
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

# Snowflake storage integration to connect to S3
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