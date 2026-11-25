# The finops_mock.json resources, provisioned through floci (http://localhost:4566, test/test).
# Same resources as `python -m finops.seed`; use one or the other. See finops/README.md to target a
# real account.

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.80"
    }
  }
}

variable "endpoint" {
  description = "AWS endpoint override; set to an empty string for a real account"
  type        = string
  default     = "http://localhost:4566"
}

variable "create_snapshots" {
  description = "floci 2.5.0 has no CreateSnapshot, so snapshots are off by default"
  type        = bool
  default     = false
}

locals {
  floci    = var.endpoint != ""
  endpoint = local.floci ? var.endpoint : null
}

provider "aws" {
  region                      = "us-east-1"
  access_key                  = local.floci ? "test" : null
  secret_key                  = local.floci ? "test" : null
  skip_credentials_validation = local.floci
  skip_metadata_api_check     = local.floci
  skip_requesting_account_id  = local.floci
  s3_use_path_style           = local.floci

  endpoints {
    ec2     = local.endpoint
    s3      = local.endpoint
    budgets = local.endpoint
    kms     = local.endpoint
    sts     = local.endpoint
  }
}

locals {
  instances = {
    "i-idle-001" = { state = "stopped", tags = { team = "data" } }
    "i-keep-001" = { state = "stopped", tags = { keep = "true" } }
    "i-run-001"  = { state = "running", tags = { team = "web" } }
  }
  volumes = {
    "vol-idle-001" = {}
    "vol-keep-001" = { keep = "true" }
  }
  snapshots = ["snap-old-001", "snap-new-001"]
  buckets = {
    "empty-bucket-001"  = { objects = 0, tags = {} }
    "keep-empty-bucket" = { objects = 0, tags = { keep = "true" } }
    "data-bucket-001"   = { objects = 12, tags = { team = "data" } }
  }
  bucket_objects = merge([
    for name, b in local.buckets : { for n in range(b.objects) : "${name}/${n}" => { bucket = name, n = n } }
  ]...)
}

# one customer managed key encrypts every volume and bucket
resource "aws_kms_key" "data" {
  description         = "pipeline data at rest"
  enable_key_rotation = true
}

resource "aws_instance" "this" {
  for_each      = local.instances
  ami           = "ami-0abcdef1234567890" # floci built-in Amazon Linux 2; change for a real account
  instance_type = "t3.micro"
  tags          = merge(each.value.tags, { Name = each.key })

  metadata_options {
    http_tokens = "required" # IMDSv2 only
  }

  root_block_device {
    encrypted  = true
    kms_key_id = aws_kms_key.data.arn
  }
}

resource "aws_ec2_instance_state" "this" {
  for_each    = local.instances
  instance_id = aws_instance.this[each.key].id
  state       = each.value.state
}

resource "aws_ebs_volume" "this" {
  for_each          = local.volumes
  availability_zone = "us-east-1a"
  size              = 1
  encrypted         = true
  kms_key_id        = aws_kms_key.data.arn
  tags              = merge(each.value, { Name = each.key })
}

resource "aws_ebs_snapshot" "this" {
  for_each  = var.create_snapshots ? toset(local.snapshots) : toset([])
  volume_id = aws_ebs_volume.this["vol-keep-001"].id
  tags      = { Name = each.key }
}

resource "aws_s3_bucket" "this" {
  for_each      = local.buckets
  bucket        = each.key
  force_destroy = true
  tags          = each.value.tags
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each                = aws_s3_bucket.this
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "this" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.data.arn
    }
  }
}

resource "aws_s3_object" "seed" {
  for_each = local.bucket_objects
  bucket   = aws_s3_bucket.this[each.value.bucket].id
  key      = format("seed/%03d.txt", each.value.n)
  content  = "seed"
}

resource "aws_budgets_budget" "monthly" {
  name         = "monthly-data-platform"
  budget_type  = "COST"
  limit_amount = "750"
  limit_unit   = "USD"
  time_unit    = "MONTHLY"
}
