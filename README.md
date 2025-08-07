# Project 6: Data Ingestion from S3 to RDS with AWS Glue Fallback

## Project Title
**Data Ingestion from S3 to RDS with Fallback to AWS Glue using Dockerized Python Application**

## Objective
Build a fault-tolerant ingestion pipeline to:

- **Ingest CSV from S3 to RDS (MySQL-compatible)**
- **Fallback to AWS Glue if ingestion fails**

## Services & Tools Used
- **Data Source:** Amazon S3
- **Primary DB:** Amazon RDS (MySQL-compatible)
- **Fallback Service:** AWS Glue
- **Containerization:** Docker
- **Scripting:** Python
- **Access Control:** AWS IAM
- **Utilities:** AWS CLI

## Key Steps

### IAM Configuration
- IAM user with access to S3, RDS, Glue

### Dockerized Application
- Python script containerized via Docker

### Primary Ingestion
- Attempts inserting CSV data into RDS

### Fallback Logic
- Fails over to AWS Glue Data Catalog for processing
