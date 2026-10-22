terraform {
  required_version = ">= 1.8"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
  }
}

# Common Crawl bucket is in us-east-1. Reading it from the same region is fast and free.
provider "aws" {
  region = "us-east-1"
}

variable "name" {
  description = "Prefix for all resources"
  type        = string
  default     = "commoncrawl"
}

variable "catalog_path" {
  description = "Frozen DuckLake catalog created by columnar.py --access-mode s3"
  type        = string
  default     = "../commoncrawl_s3.frozen.ducklake"
}

variable "lambda_package" {
  description = "Zip created from lambda/target/package"
  type        = string
  default     = "../lambda/target/lambda.zip"
}

variable "memory_size" {
  description = "Lambda memory in MB. vCPUs and network bandwidth scale with memory."
  type        = number
  default     = 3008
}

variable "warc_concurrency" {
  description = "Parallel S3 range requests per vector of warc_record() in the lambda"
  type        = number
  default     = 128
}

variable "duckdb_threads" {
  description = "DuckDB threads of the lambda. Reading S3 is network bound so more than the 2 vCPUs, but every thread can hold a vector of fetched pages in memory."
  type        = number
  default     = 16
}

variable "gb_seconds_limit" {
  description = "The lambda refuses queries when this function has used this many GB-seconds in the calendar month. 400 000 is the free tier, 0 disables the limit."
  type        = number
  default     = 400000
}

variable "origin_read_timeout" {
  description = "Seconds CloudFront waits for bytes from the lambda. Defaults to the current quota, see README.md."
  type        = number
  default     = null
}

# Increases to these quotas are requested manually, see README.md
data "aws_servicequotas_service_quota" "cloudfront_response_timeout" {
  service_code = "cloudfront"
  quota_code   = "L-AECE9FA7"
}

data "aws_servicequotas_service_quota" "lambda_network_bandwidth" {
  service_code = "lambda"
  quota_code   = "L-14320ECE"
}

data "aws_caller_identity" "current" {}

# Bucket for the lambda package and large query results
resource "aws_s3_bucket" "artifacts" {
  bucket_prefix = "${var.name}-artifacts-"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Lambda uploads results over 5MB here and CloudFront serves them
resource "aws_s3_bucket_lifecycle_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id

  rule {
    id     = "expire-results"
    status = "Enabled"
    filter {
      prefix = "results/"
    }
    expiration {
      days = 1
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

resource "aws_s3_bucket_policy" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "cloudfront.amazonaws.com" }
      Action    = "s3:GetObject"
      Resource  = "${aws_s3_bucket.artifacts.arn}/results/*"
      Condition = { StringEquals = { "AWS:SourceArn" = aws_cloudfront_distribution.query.arn } }
    }]
  })
}

resource "aws_s3_object" "lambda_package" {
  bucket      = aws_s3_bucket.artifacts.id
  key         = "lambda.zip"
  source      = var.lambda_package
  source_hash = filemd5(var.lambda_package)
}

# Lambda can only read Common Crawl
resource "aws_iam_role" "lambda" {
  name_prefix = "${var.name}-lambda-"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "lambda_s3" {
  role = aws_iam_role.lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject"]
        # cc-index has the parquet files, crawl-data the WARC files read by warc_record()
        Resource = ["arn:aws:s3:::commoncrawl/cc-index/*", "arn:aws:s3:::commoncrawl/crawl-data/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:AbortMultipartUpload"]
        Resource = ["${aws_s3_bucket.artifacts.arn}/results/*"]
      },
      {
        # Billed duration of this month for gb_seconds_limit, the action doesn't support resource restrictions
        Effect   = "Allow"
        Action   = ["cloudwatch:GetMetricStatistics"]
        Resource = ["*"]
      },
    ]
  })
}

locals {
  lambda_common = {
    runtime          = "provided.al2023"
    handler          = "bootstrap"
    architectures    = ["arm64"]
    s3_bucket        = aws_s3_bucket.artifacts.id
    s3_key           = aws_s3_object.lambda_package.key
    source_code_hash = filebase64sha256(var.lambda_package)
  }
  origin_read_timeout = coalesce(var.origin_read_timeout, data.aws_servicequotas_service_quota.cloudfront_response_timeout.value)

  catalog_env = {
    # The catalog is in the lambda package, see lambda/build.sh. Part of the result cache key.
    CATALOG_MD5 = filemd5(var.catalog_path)
  }
}

# --- Query lambda: GET /?sql=... behind CloudFront, returns zstd compressed Arrow IPC ---

resource "aws_cloudwatch_log_group" "query" {
  name              = "/aws/lambda/${var.name}-query"
  retention_in_days = 7
}

resource "aws_lambda_function" "query" {
  function_name    = "${var.name}-query"
  role             = aws_iam_role.lambda.arn
  runtime          = local.lambda_common.runtime
  handler          = local.lambda_common.handler
  architectures    = local.lambda_common.architectures
  s3_bucket        = local.lambda_common.s3_bucket
  s3_key           = local.lambda_common.s3_key
  source_code_hash = local.lambda_common.source_code_hash
  memory_size      = var.memory_size
  # CloudFront gives up after this so there's no reason to keep running and spending lambda seconds
  timeout = local.origin_read_timeout

  # DuckDB spills larger than memory operators into /tmp/duckdb
  ephemeral_storage {
    size = 2048
  }

  environment {
    variables = merge(local.catalog_env, {
      RESULTS_BUCKET   = aws_s3_bucket.artifacts.id
      WARC_CONCURRENCY = tostring(var.warc_concurrency)
      GB_SECONDS_LIMIT = tostring(var.gb_seconds_limit)
      TIMEOUT_SECONDS  = tostring(local.origin_read_timeout)
      DUCKDB_THREADS   = tostring(var.duckdb_threads)
    })
  }

  depends_on = [aws_cloudwatch_log_group.query, aws_iam_role_policy.lambda_s3]
}

resource "aws_lambda_function_url" "query" {
  function_name      = aws_lambda_function.query.function_name
  authorization_type = "AWS_IAM"
  invoke_mode        = "RESPONSE_STREAM"
}

# Only CloudFront can call the function URL
resource "aws_lambda_permission" "cloudfront_url" {
  statement_id  = "AllowCloudFrontInvokeFunctionUrl"
  action        = "lambda:InvokeFunctionUrl"
  function_name = aws_lambda_function.query.function_name
  principal     = "cloudfront.amazonaws.com"
  source_arn    = aws_cloudfront_distribution.query.arn
}

resource "aws_lambda_permission" "cloudfront_invoke" {
  statement_id  = "AllowCloudFrontInvokeFunction"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.query.function_name
  principal     = "cloudfront.amazonaws.com"
  source_arn    = aws_cloudfront_distribution.query.arn
}

# Signed cookies for https://github.com/midwork-finds-jobs/duckdb-cloudfront
resource "tls_private_key" "cloudfront" {
  algorithm = "RSA"
  rsa_bits  = 2048
}

resource "aws_cloudfront_public_key" "signing" {
  name_prefix = "${var.name}-"
  encoded_key = tls_private_key.cloudfront.public_key_pem
}

resource "aws_cloudfront_key_group" "signing" {
  name  = "${var.name}-signing"
  items = [aws_cloudfront_public_key.signing.id]
}

resource "aws_cloudfront_origin_access_control" "results" {
  name                              = "${var.name}-results"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

resource "aws_cloudfront_origin_access_control" "lambda" {
  name                              = "${var.name}-lambda"
  origin_access_control_origin_type = "lambda"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

# The lake is frozen so the same SQL always returns the same result
resource "aws_cloudfront_cache_policy" "query" {
  name        = "${var.name}-query"
  min_ttl     = 0
  # Only responses with Cache-Control are cached: successful queries. Lambda runtime errors have none.
  default_ttl = 0
  max_ttl     = 31536000

  parameters_in_cache_key_and_forwarded_to_origin {
    # Arrow IPC is already zstd compressed
    enable_accept_encoding_gzip   = false
    enable_accept_encoding_brotli = false
    cookies_config {
      cookie_behavior = "none"
    }
    headers_config {
      header_behavior = "none"
    }
    query_strings_config {
      query_string_behavior = "all"
    }
  }
}

resource "aws_cloudfront_distribution" "query" {
  enabled     = true
  comment     = "Common Crawl DuckLake query lambda"
  price_class = "PriceClass_100"

  origin {
    domain_name              = replace(replace(aws_lambda_function_url.query.function_url, "https://", ""), "/", "")
    origin_id                = "lambda"
    origin_access_control_id = aws_cloudfront_origin_access_control.lambda.id

    custom_origin_config {
      http_port              = 80
      https_port             = 443
      origin_protocol_policy = "https-only"
      origin_ssl_protocols   = ["TLSv1.2"]
      # Slow queries need to produce the first bytes before this
      origin_read_timeout = local.origin_read_timeout
    }
  }

  # Large query results uploaded by the lambda
  origin {
    domain_name              = aws_s3_bucket.artifacts.bucket_regional_domain_name
    origin_id                = "results"
    origin_access_control_id = aws_cloudfront_origin_access_control.results.id
  }

  ordered_cache_behavior {
    path_pattern           = "/results/*"
    target_origin_id       = "results"
    viewer_protocol_policy = "https-only"
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]
    # Managed CachingOptimized
    cache_policy_id    = "658327ea-f89d-4fab-a63d-7e88639e58f6"
    trusted_key_groups = [aws_cloudfront_key_group.signing.id]
  }

  default_cache_behavior {
    target_origin_id       = "lambda"
    viewer_protocol_policy = "https-only"
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]
    cache_policy_id        = aws_cloudfront_cache_policy.query.id
    # Managed AllViewerExceptHostHeader
    origin_request_policy_id = "b689b0a8-53d0-40ab-baf2-68738e2966ac"
    trusted_key_groups       = [aws_cloudfront_key_group.signing.id]
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    cloudfront_default_certificate = true
  }
}

output "cloudfront_url" {
  value = "https://${aws_cloudfront_distribution.query.domain_name}/"
}

output "cloudfront_key_pair_id" {
  value = aws_cloudfront_public_key.signing.id
}

output "cloudfront_private_key_pem" {
  value     = tls_private_key.cloudfront.private_key_pem
  sensitive = true
}

output "query_function_name" {
  value = aws_lambda_function.query.function_name
}

output "quotas" {
  value = {
    cloudfront_response_timeout_seconds = data.aws_servicequotas_service_quota.cloudfront_response_timeout.value
    lambda_network_bandwidth_mbps       = data.aws_servicequotas_service_quota.lambda_network_bandwidth.value
  }
}
