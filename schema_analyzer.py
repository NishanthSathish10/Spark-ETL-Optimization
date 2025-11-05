import pyspark
from pyspark.sql import SparkSession
import glob
import os

def create_spark_session():
    """
    Creates a simple Spark Session.
    We MUST enable caseSensitivity to see the
    real column name differences (e.g., 'passenger_count' vs 'Passenger_Count').
    """
    spark = SparkSession.builder \
        .appName("SchemaAnalyzer") \
        .config("spark.sql.caseSensitive", "true") \
        .getOrCreate()
    return spark

def analyze_schemas(spark):
    """
    Iterates through all Parquet files, reads their
    schemas, and prints a new schema whenever one
    is found to be different from the previous one.
    """
    print("Starting schema analysis...")
    print("This will read the metadata (footer) of each file.")
    
    # Find all the parquet files in the raw data directory
    # We sort them to get them in chronological order (very important!)
    file_paths = sorted(glob.glob("data/raw/yellow_tripdata_*.parquet"))
    
    if not file_paths:
        print("Error: No Parquet files found in 'data/raw/'.")
        print("Please run the data_downloader.py script first.")
        return

    print(f"Found {len(file_paths)} total files to analyze.")
    
    last_schema = None
    files_with_this_schema = []

    for file_path in file_paths:
        try:
            # --- FIX: Get the DataFrame first (lazy operation) ---
            current_df = spark.read.parquet(file_path)
            # --- FIX: Get the schema from the DataFrame ---
            current_schema = current_df.schema
            
            if current_schema != last_schema:
                # If this isn't the first file, print the summary
                # for the *previous* schema group.
                if last_schema is not None:
                    # Print the *last* file of the previous group
                    print(f"    ...schema consistent up to: {files_with_this_schema[-1]}")
                
                # A new schema is found! Print it.
                print("\n" + "="*80)
                print(f"NEW SCHEMA FOUND in file: {file_path}")
                print("="*80)
                # --- FIX: Called printSchema() on the DataFrame, not the schema object ---
                # This method prints to stdout and returns None.
                current_df.printSchema()
                
                # Reset our trackers
                last_schema = current_schema
                files_with_this_schema = [file_path]
            else:
                # This file has the same schema as the last one
                files_with_this_schema.append(file_path)

        except Exception as e:
            print("\n" + "!"*80)
            print(f"ERROR: Could not read schema for file: {file_path}")
            print(f"Error details: {e}")
            print("!"*80)
    
    # Print the summary for the very last group of files
    if files_with_this_schema:
        print(f"    ...schema consistent up to: {files_with_this_schema[-1]}")

    print("\nSchema analysis complete.")

if __name__ == "__main__":
    spark = create_spark_session()
    analyze_schemas(spark)
    spark.stop()