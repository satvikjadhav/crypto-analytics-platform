output "pipeline_user_access_key_id" {
    value = aws_iam_access_key.pipeline.id
    description = "Access Key ID for the pipeline IAM user"
    sensitive = false
}

output "pipeline_user_secret_access_key" {
    value = aws_iam_access_key.pipeline.secret
    description = "Secret Access Key for the pipeline IAM user"
    sensitive = true
}