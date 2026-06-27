# Store it in Secrets Manager so it survives re-deploys and is never in tfstate plaintext
resource "aws_secretsmanager_secret" "fernet_key" {
  name                    = "crypto-airflow/fernet-key"
  description             = "Airflow Fernet encryption key"
  recovery_window_in_days = 0 # allow immediate deletion on destroy
}