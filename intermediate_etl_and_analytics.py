import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, round, avg, count, max, to_timestamp, coalesce
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, TimestampNTZType, TimestampType
import glob # Import glob to find files

def create_spark_session():
    """
    Creates a Spark Session based on our schema_analyzer.py findings.
    
    1. spark.sql.caseSensitive=true:
       Required because we have both 'airport_fee' and 'Airport_fee'.
       
    2. spark.sql.shuffle.partitions=8:
       Kept from the naive script to ensure our joins and
       aggregations are inefficient and spill to disk.
    
    NOTE: We've removed reader configs and the MASTER_SCHEMA
    as we are now reading files one-by-one.
    """
    spark = SparkSession.builder \
        .appName("Intermediate_ETL_and_Analytics") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.sql.caseSensitive", "true") \
        .getOrCreate()
    return spark

def standardize_schema(df, file_name):
    """
    Takes a DataFrame with *any* of the 2011-2024 schemas
    and standardizes it to a single, common format.
    
    --- THIS IS THE KEY FIX ---
    We dynamically check which columns exist in the current DataFrame
    before trying to use them.
    """
    
    df_cols = df.columns
    
    # --- 1. Standardize Pickup Datetime ---
    if "tpep_pickup_datetime" in df_cols:
        df = df.withColumn("std_pickup_datetime", col("tpep_pickup_datetime"))
    elif "pickup_datetime" in df_cols: # For 2010 files (if they existed)
        df = df.withColumn("std_pickup_datetime", to_timestamp(col("pickup_datetime")))
    elif "Trip_Pickup_DateTime" in df_cols: # For 2009 files (if they existed)
         df = df.withColumn("std_pickup_datetime", to_timestamp(col("Trip_Pickup_DateTime")))
    else:
        # Fallback if no known column is found
        df = df.withColumn("std_pickup_datetime", lit(None).cast(TimestampType()))

    # --- 2. Standardize Dropoff Datetime ---
    if "tpep_dropoff_datetime" in df_cols:
        df = df.withColumn("std_dropoff_datetime", col("tpep_dropoff_datetime"))
    elif "dropoff_datetime" in df_cols:
        df = df.withColumn("std_dropoff_datetime", to_timestamp(col("dropoff_datetime")))
    elif "Trip_Dropoff_DateTime" in df_cols:
         df = df.withColumn("std_dropoff_datetime", to_timestamp(col("Trip_Dropoff_DateTime")))
    else:
        df = df.withColumn("std_dropoff_datetime", lit(None).cast(TimestampType()))

    # --- 3. Standardize PULocationID ---
    if "PULocationID" in df_cols:
        df = df.withColumn("std_PULocationID", col("PULocationID").cast(LongType()))
    else:
        # This will apply to 2010 files, creating a null column
        df = df.withColumn("std_PULocationID", lit(None).cast(LongType()))
        
    # --- 4. Standardize DOLocationID ---
    if "DOLocationID" in df_cols:
        df = df.withColumn("std_DOLocationID", col("DOLocationID").cast(LongType()))
    else:
        df = df.withColumn("std_DOLocationID", lit(None).cast(LongType()))

    # --- 5. Standardize Trip Distance ---
    if "trip_distance" in df_cols and "Trip_Distance" in df_cols:
        df = df.withColumn("std_trip_distance", coalesce(col("trip_distance"), col("Trip_Distance")).cast(DoubleType()))
    elif "trip_distance" in df_cols:
        df = df.withColumn("std_trip_distance", col("trip_distance").cast(DoubleType()))
    elif "Trip_Distance" in df_cols:
        df = df.withColumn("std_trip_distance", col("Trip_Distance").cast(DoubleType()))
    else:
        df = df.withColumn("std_trip_distance", lit(None).cast(DoubleType()))
        
    # --- 6. Standardize Total Amount ---
    if "total_amount" in df_cols and "Total_Amt" in df_cols:
        df = df.withColumn("std_total_amount", coalesce(col("total_amount"), col("Total_Amt")).cast(DoubleType()))
    elif "total_amount" in df_cols:
        df = df.withColumn("std_total_amount", col("total_amount").cast(DoubleType()))
    elif "Total_Amt" in df_cols:
        df = df.withColumn("std_total_amount", col("Total_Amt").cast(DoubleType()))
    else:
        df = df.withColumn("std_total_amount", lit(None).cast(DoubleType()))

    # --- 7. Standardize Passenger Count ---
    if "passenger_count" in df_cols and "Passenger_Count" in df_cols:
         df = df.withColumn("std_passenger_count", coalesce(col("passenger_count"), col("Passenger_Count")).cast(IntegerType()))
    elif "passenger_count" in df_cols:
         df = df.withColumn("std_passenger_count", col("passenger_count").cast(IntegerType()))
    elif "Passenger_Count" in df_cols:
         df = df.withColumn("std_passenger_count", col("Passenger_Count").cast(IntegerType()))
    else:
         df = df.withColumn("std_passenger_count", lit(None).cast(IntegerType()))

    
    # --- 8. Select *only* the standardized columns ---
    # This ensures every DataFrame has the *exact* same schema
    # before we try to union them.
    
    final_cols = [
        "std_pickup_datetime",
        "std_dropoff_datetime",
        "std_PULocationID",
        "std_DOLocationID",
        "std_trip_distance",
        "std_total_amount",
        "std_passenger_count"
    ]
    
    return df.select(final_cols)


def run_intermediate_etl_and_analytics(spark):
    """
    Runs the 'intermediate' ETL job.
    This version successfully reads all data from 2011-2024
    by reading *one file at a time* and unioning them.
    This is a huge performance bottleneck.
    """
    
    # --- 1. Intermediate Data Loading ---
    
    print("Reading taxi zone lookup data...")
    taxi_zone_lookup_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/taxi_zone_lookup.csv")
    
    # --- INTERMEDIATE FIX: Read files one-by-one ---
    # We abandon the master schema and read each file,
    # letting Spark infer its schema, then immediately
    # standardizing it.
    
    print("Finding all yellow taxi data files (2011-2024)...")
    file_paths = sorted(
        glob.glob("data/raw/yellow_tripdata_201*.parquet") + \
        glob.glob("data/raw/yellow_tripdata_202*.parquet")
    )
    
    if not file_paths:
        raise FileNotFoundError("No Parquet files found in 'data/raw/' for 2011-2024.")

    print(f"Found {len(file_paths)} files. Reading and standardizing one by one...")
    
    all_dfs = [] # A list to hold all our standardized DataFrames
    
    for i, file_path in enumerate(file_paths):
        if (i % 20) == 0:
            print(f"  ...processing file {i+1} of {len(file_paths)}: {file_path}")
        
        try:
            # 1. Read one file, inferring its schema
            df = spark.read.parquet(file_path)
            
            # 2. Standardize it
            df_std = standardize_schema(df, file_path)
            
            # 3. Add to our list
            all_dfs.append(df_std)
            
        except Exception as e:
            print(f"!! FAILED to process file {file_path}: {e}")
            # We "naively" skip this file and continue
            pass

    print("...all files processed. Unioning into one large DataFrame...")
    
    # This is a "naive" way to union. A more optimal
    # way is to use reduce, but this is fine for our
    # intermediate script.
    if not all_dfs:
        raise Exception("No dataframes were successfully read and standardized.")
        
    df_unified = all_dfs[0]
    for i in range(1, len(all_dfs)):
        df_unified = df_unified.union(all_dfs[i])

    # DEBUG: See the new unified schema
    print("--- Unified Schema (Selected Columns) ---")
    df_unified.printSchema()
    
    
    # --- 3. "Naive" Transformations (Bottlenecks Kept) ---
    
    print("Applying transformations with Shuffle Joins...")
    
    # BAD PRACTICE 4 (Still Naive): Triggering a "SortMergeJoin".
    # This 'inner' join will drop any 2011+ rows where PULocationID is null.
    df_with_pu_location = df_unified.join(
        taxi_zone_lookup_df,
        col("std_PULocationID") == col("LocationID"),
        "inner"
    )
    
    # Rename for clarity
    df_with_pu_location = df_with_pu_location.withColumnRenamed("Borough", "pickup_borough") \
                                             .withColumnRenamed("Zone", "pickup_zone") \
                                             .withColumnRenamed("service_zone", "pickup_service_zone") \
                                             .drop(col("LocationID"))

    # BAD PRACTICE 5 (Still Naive): Another massive shuffle.
    df_with_locations = df_with_pu_location.join(
        taxi_zone_lookup_df,
        col("std_DOLocationID") == col("LocationID"),
        "inner"
    )

    df_with_locations = df_with_locations.withColumnRenamed("Borough", "dropoff_borough") \
                                         .withColumnRenamed("Zone", "dropoff_zone") \
                                         .withColumnRenamed("service_zone", "dropoff_service_zone") \
                                         .drop(col("LocationID"))
    
    
    # --- 4. Data Cleaning and Feature Engineering ---
    
    print("Cleaning data and engineering features...")
    
    # We must cast to TimestampType() *first* before casting to double.
    df_cleaned = df_with_locations.withColumn(
        "trip_duration_mins",
        round(
            (col("std_dropoff_datetime").cast(TimestampType()).cast("double") - \
             col("std_pickup_datetime").cast(TimestampType()).cast("double")) / 60, 
            2
        )
    )
    
    # Filter out "bad" data using standardized columns
    df_cleaned = df_cleaned.filter(
        (col("std_trip_distance") > 0) &
        (col("std_total_amount") > 0) &
        (col("std_passenger_count") > 0) &
        (col("trip_duration_mins") > 1) &
        (col("trip_duration_mins") < 240) # Filter trips longer than 4 hours
    )
    
    # Select only the columns we need for the final analytics
    final_df = df_cleaned.select(
        col("std_pickup_datetime").alias("tpep_pickup_datetime"),
        "trip_duration_mins",
        col("std_trip_distance").alias("trip_distance"),
        col("std_total_amount").alias("total_amount"),
        col("std_passenger_count").alias("passenger_count"),
        "pickup_borough",
        "dropoff_borough"
    )

    # --- 5. Baseline Analytics (Bottleneck Kept) ---
    
    print("Running baseline analytics...")
    
    # BAD PRACTICE 6 (Still Naive): A wide aggregation (groupBy)
    analytics_df = final_df.groupBy("pickup_borough").agg(
        avg("trip_distance").alias("avg_trip_distance"),
        avg("total_amount").alias("avg_total_amount"),
        max("trip_duration_mins").alias("max_trip_duration"),
        count(lit(1)).alias("total_trips")
    )
    
    
    # --- 6. Action: Show and Write Results ---
    
    # BAD PRACTICE 7 (Still Naive): Triggering the *entire* DAG (computation) twice.
    print("--- Analytics Results (Intermediate) ---")
    analytics_df.show(truncate=False)
    
    print("Writing cleaned data to 'data/cleaned_trips_intermediate'...")
    final_df.write \
        .mode("overwrite") \
        .parquet("data/cleaned_trips_intermediate")

    print("--- Intermediate Job Complete ---")

if __name__ == "__main__":
    spark_session = create_spark_session()
    run_intermediate_etl_and_analytics(spark_session)
    spark_session.stop()