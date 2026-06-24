# Update this according to your project requirements. This file is used to set the variables for Terraform.

aws_region  = "us-east-1"
environment = "dev"
initials    = "sj"

snowflake_account           = "xy12345.us-east-1" # from Snowflake console
snowflake_user              = "your_admin_login"
snowflake_password          = "YourAdminPass!"
snowflake_pipeline_password = "PipelineStr0ng!"

airflow_fernet_key = "PASTE_GENERATED_KEY_HERE"
git_repo_url       = "https://github.com/satvikjadhav/crypto-analytics-platform.git"

ssh_public_key_path = ""