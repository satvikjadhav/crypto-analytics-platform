resource "aws_s3_bucket" "data_lake" {
    bucket = "crypto-analytics-lake-${var.initials}"
}

resource "aws_s3_bucket_versioning" "data_lake" {
    bucket = aws_s3_bucket.data_lake.id
    versioning_configuration {
        status = "Enabled"
    }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
    bucket = aws_s3_bucket.data_lake.id
    block_public_acls       = true
    block_public_policy     = true
    ignore_public_acls      = true
    restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
    bucket = aws_s3_bucket.data_lake.id
    rule {
        apply_server_side_encryption_by_default {
            sse_algorithm = "AES256"
        }
    }
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
    bucket = aws_s3_bucket.data_lake.id

    rule {
        id     = "archive-raw-to-glacier"
        status = "Enabled"
        filter {
            prefix = "raw/"
        }
        transition {
            days          = 90
            storage_class = "GLACIER"
        }
    }

    rule {
        id = "expire-checkpoints" 
        status = "Enabled"
        filter {
            prefix = "checkpoints/"
        }
        expiration {
            days = 30
        }
    }
}

locals {
    s3_prefixes = [
        "raw/binance/trades/",
        "raw/coingecko/",
        "curated/delta/trades/",
        "curated/delta/ohlcv/",
        "checkpoints/",
        "logs/",
    ]
}

resource "aws_s3_object" "folder_placeholders" {
    for_each = toset(local.s3_prefixes)
    bucket = aws_s3_bucket.data_lake.id
    key = each.value
    content = ""
    content_type = "application/x-directory"
}

output "data_lake_bucket" { value = aws_s3_bucket.data_lake.bucket }