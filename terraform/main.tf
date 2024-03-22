# Configure the AWS Provider

provider "aws" {
  region  = "eu-north-1"
}

#Creating s3 bucket 
resource "aws_s3_bucket" "kende-project-bucket" {
  bucket = "myde-tf-bucket"

  tags = {
    Name        = "MyTfBucket"
    Environment = "Dev"
  }
}


terraform {
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "0.47.0"
    }
  }
}

provider "snowflake" {
  account  = var.snowflake_account
  # region   = var.snowflake_region
  username = var.snowflake_user
  password = var.snowflake_password
  role     = var.snowflake_role
}


resource "snowflake_database" "db" {
  name = "de_project"
}

resource "snowflake_warehouse" "warehouse" {
  name           = "de_project"
  warehouse_size = "X-Small"
}

resource "snowflake_schema" "raw_schema" {
  database = "de_project"
  name     = "raw_schema"

  is_transient        = false
  is_managed          = false
  data_retention_days = 1
}

resource "snowflake_schema" "analytics_schema" {
  database = "de_project"
  name     = "analytics_schema"

  is_transient        = false
  is_managed          = false
  data_retention_days = 1
}