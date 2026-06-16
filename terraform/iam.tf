resource "aws_iam_user" "pipeline" {
    name = "crypto-pipeline-user"
    path = "/crypto-analytics/"
}

data "aws_iam_policy_document" "s3_pipeline" {
    statement {
        sid = "AllowBucketList"
        effect = "Allow"
        actions = [
            "s3:ListBucket", "s3:GetBucketLocation"
        ]
        resources = [
            aws_s3_bucket.data_lake.arn
        ]
    }
    statement {
        sid = "AllowObjectReadWrite"
        effect = "Allow"
        actions = [
            "s3:GetObject", "s3:PutObject", "s3:DeleteObject"
        ]
        resources = [
            "${aws_s3_bucket.data_lake.arn}/*"
        ]
    }
}

resource "aws_iam_policy" "s3_pipeline" {
    name = "crypto-pipeline-s3-policy"
    policy = data.aws_iam_policy_document.s3_pipeline.json
}

resource "aws_iam_user_policy_attachment" "pipeline" {
    user = aws_iam_user.pipeline.name
    policy_arn = aws_iam_policy.s3_pipeline.arn
}

resource "aws_iam_access_key" "pipeline" {
    user = aws_iam_user.pipeline.name
}