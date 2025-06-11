resource "aws_s3_bucket" "bucket_script_teste" {
  bucket = "bucket-script-teste"
  acl = "private"
}

resource "aws_s3_object" "upload_script" {
  bucket = aws_s3_bucket.bucket_script_teste.bucket
  key    = "script.py"  # Caminho no bucket
  source = "${path.module}/arquivos/script.py"
}

resource "aws_glue_connection" "glue_job_teste_connection" {
  name = "glue-job-teste-connection"

  connection_type = "NETWORK"

  physical_connection_requirements {
    subnet_id         = "subnet-0a30d6e678abba14b"    # substituir por uma subnet_id da conta a ser utilizada     
    security_group_id_list = ["sg-09eb4e167c37377e8"]   # substituir por um security group da conta a ser utilizada      
    availability_zone = "us-east-1a"            
  }
}

resource "aws_kms_key" "glue_job_teste_kms_key" {
  deletion_window_in_days = 10
  enable_key_rotation     = false
}

resource "aws_glue_security_configuration" "glue_job_teste_sg" {
  name = "glue-job-teste-sg"

  encryption_configuration {
    cloudwatch_encryption {
      cloudwatch_encryption_mode = "SSE-KMS"
      kms_key_arn                = aws_kms_key.glue_job_teste_kms_key.arn
    }

    job_bookmarks_encryption {
      job_bookmarks_encryption_mode = "CSE-KMS"
      kms_key_arn                   = aws_kms_key.glue_job_teste_kms_key.arn
    }

    s3_encryption {
      s3_encryption_mode = "SSE-KMS"
      kms_key_arn        = aws_kms_key.glue_job_teste_kms_key.arn
    }
  }
}

resource "aws_iam_role" "glue_job_teste_role" {
  name = "glue-job-teste-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "glue_job_teste_policy" {
  name = "glue-job-teste-policy"
  role = aws_iam_role.glue_job_teste_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          "arn:aws:s3:::input-file",
          "arn:aws:s3:::input-file/*",
          "arn:aws:s3:::bronze-files",
          "arn:aws:s3:::bronze-files/*",
          "arn:aws:s3:::silver-files",
          "arn:aws:s3:::silver-files/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:Encrypt",
          "kms:GenerateDataKey",
          "kms:DescribeKey"
        ]
        Resource = aws_kms_key.glue_job_teste_kms_key.arn
      }
    ]
  })
}

resource "aws_glue_job" "glue_job_teste" {
  name         = "glue-job-teste"
  glue_version = "5.0"
  role_arn     = aws_iam_role.glue_job_teste_role.arn 
  number_of_workers = 10
  worker_type = "G.1X"
  timeout = 2
  max_retries = 0 

  execution_property {
    max_concurrent_runs = 1 
  }

  tags = {
    "Nome" : "projeto",
    "Valor" : "teste_eng_dados"
  }

  command {
    script_location = "s3://${aws_s3_bucket.bucket_script_teste.bucket}/script.py"
    python_version  = "3"
  }

default_arguments = {
    "--job-language"    = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"  = "true"
    "--enable-job-insights" = "true"   
    "--enable-glue-datacatalog" = "true"
    "--JOB_NAME" = "glue-job-teste"
    "--S3_INPUT_FILE_PATH" = "s3://input-file/"
    "--BRONZE_DATABASE_NAME" = "db_bronze"
    "--BRONZE_TABLE_NAME" = "tb_bronze"
    "--SILVER_DATABASE_NAME" = "db_silver"
    "--SILVER_TABLE_NAME"= "tb_silver"
  }

  security_configuration = aws_glue_security_configuration.glue_job_teste_sg.name
  connections = [aws_glue_connection.glue_job_teste_connection.name]

}