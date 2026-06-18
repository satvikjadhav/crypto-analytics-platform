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