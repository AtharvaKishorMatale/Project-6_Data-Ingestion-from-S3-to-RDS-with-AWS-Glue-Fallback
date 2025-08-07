import os
import pandas as pd
import boto3
import pymysql
from sqlalchemy import create_engine
from botocore.exceptions import ClientError

# Load from environment
aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
aws_region = os.environ.get("AWS_DEFAULT_REGION")

s3_bucket = os.environ.get("S3_BUCKET_NAME")
csv_key = os.environ.get("CSV_FILE_KEY")

rds_host = os.environ.get("RDS_HOST")
rds_user = os.environ.get("RDS_USER")
rds_pass = os.environ.get("RDS_PASSWORD")
rds_db = os.environ.get("RDS_DB_NAME")
rds_table = os.environ.get("RDS_TABLE_NAME")

glue_db = os.environ.get("GLUE_DB_NAME")
glue_table = os.environ.get("GLUE_TABLE_NAME")
glue_s3_path = os.environ.get("GLUE_S3_PATH")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=aws_region,
)

glue_client = boto3.client(
    "glue",
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=aws_region,
)

def read_csv_from_s3():
    print(" Reading CSV from S3...")
    obj = s3_client.get_object(Bucket=s3_bucket, Key=csv_key)
    return pd.read_csv(obj["Body"])

def upload_to_rds(df):
    print("🔗 Uploading data to RDS...")
    conn_str = f"mysql+pymysql://{rds_user}:{rds_pass}@{rds_host}/{rds_db}"
    engine = create_engine(conn_str)
    df.to_sql(name=rds_table, con=engine, if_exists="append", index=False)
    print(" Data uploaded to RDS.")

def fallback_to_glue(df):
    print("⚠ RDS failed. Falling back to Glue...")

    columns = [{"Name": col, "Type": "string"} for col in df.columns]

    try:
        glue_client.create_table(
            DatabaseName=glue_db,
            TableInput={
                "Name": glue_table,
                "StorageDescriptor": {
                    "Columns": columns,
                    "Location": glue_s3_path,
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                        "Parameters": {"field.delim": ","}
                    }
                },
                "TableType": "EXTERNAL_TABLE",
            }
        )
        print(" Fallback succeeded: Glue table created.")
    except ClientError as e:
        print(f" Glue fallback failed: {e.response['Error']['Message']}")

def main():
    try:
        df = read_csv_from_s3()
        upload_to_rds(df)
    except Exception as e:
        print(f" Error uploading to RDS: {str(e)}")
        try:
            df = read_csv_from_s3()
            fallback_to_glue(df)
        except Exception as fallback_error:
            print(f" Fallback to Glue also failed: {str(fallback_error)}")

if __name__ == "__main__":
    main()
