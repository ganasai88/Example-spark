import argparse
import boto3
import json
from urllib.parse import urlparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from botocore.exceptions import NoCredentialsError, ClientError
import s3fs

def load_config_from_s3(file_path: str) -> dict:
    """
    Loads a configuration file from an S3 bucket using the full S3 path.

    Args:
        file_path (str): Full S3 path (e.g., s3://bucket-name/key/to/config.json).

    Returns:
        dict: Parsed configuration data.
    """
    if file_path.startswith("s3://"):
        # Initialize the S3 filesystem
        fs = s3fs.S3FileSystem()
        
        # Open the file from S3 and load JSON data
        with fs.open(file_path, 'r') as file:
            json_data = json.load(file)
            
        return json_data
    else:
        raise ValueError(f"The provided path {file_path} is not an S3 path.")

def main(config_path):
    # Initialize Spark session
    spark = (
        SparkSession.builder
        .appName("Spark Job on EMR")
        .getOrCreate()
    )

    try:
        config = load_config_from_s3(config_path)
        
        # Check if required keys exist
        if "data_source" not in config or "output_path" not in config:
            print("Error: Configuration file must contain 'data_source' and 'output_path'.")
            return
        
        data_source = config["data_source"]
        output_path = config["output_path"]

        # Read CSV into a DataFrame
        df = spark.read.option("header", "true").csv(data_source)
        print("Data loaded successfully, here are a few rows:")
        print(df.show())

        # Write DataFrame as Parquet to the output folder
        df.write.option("header", "true").mode("overwrite").parquet(output_path)
        print("Job completed successfully!")

    except Exception as e:
        print(f"Error in main execution: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Spark Job with Config")
    parser.add_argument("--config", required=True, help="Path to configuration file")
    args = parser.parse_args()

    main(args.config)
