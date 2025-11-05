import pyspark
from pyspark.sql import SparkSession
# Import col() and coalesce()
from pyspark.sql.functions import col, lit, round, avg, count, max, to_timestamp, coalesce
# --- FIX: Import the specific PySpark exception we need to catch ---
from pyspark.errors.exceptions.captured import AnalysisException
# --- FIX: Import the Py4J error to catch the Java-level SparkException ---
from py4j.protocol import Py4JJavaError

def create_spark_session():
    """
    Creates a Spark Session with 'bad' default configurations for our baseline.
    We set a low number of shuffle partitions to intentionally cause memory issues.
    """
    spark = SparkSession.builder \
        .appName("Naive_ETL_and_Analytics_Baseline") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    return spark

def run_naive_etl_and_analytics(spark):
    """
    Runs the 'naive' ETL job and performs initial analytics.
    This job is intentionally designed with common performance bottlenecks.
    """
    
    # --- 1. "Naive" Data Loading ---
    
    print("Reading taxi zone lookup data...")
    # --- FIX: Corrected CSV file path ---
    taxi_zone_lookup_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/taxi_zone_lookup.csv")
    
    # BAD PRACTICE 2: Manually reading and unioning different schemas.
    # This is "naive" because it's slow, brittle, and scans the data twice.
    # We're doing this to solve the 'CANNOT_MERGE_SCHEMAS' error.
    
    # We assume "new" data is from 2017-2024 (which has PULocationID)
    # The INT/DOUBLE error is *within* these files, so mergeSchema will
    # correctly up-cast to DOUBLE when merging *within* this read.
    print("Reading all 'new' taxi data (2017-2024) with mergeSchema...")
    
    # --- FIX: Catch 'AnalysisException' AND 'Py4JJavaError' ---
    try:
        # --- FIX: Added 'data/raw/' prefix to parquet paths ---
        df_new = spark.read.option("mergeSchema", "true") \
            .parquet("data/raw/yellow_tripdata_2017*.parquet",
                     "data/raw/yellow_tripdata_2018*.parquet",
                     "data/raw/yellow_tripdata_2019*.parquet",
                     "data/raw/yellow_tripdata_202*.parquet") # 2020-2024
    except (AnalysisException, Py4JJavaError) as e:
        print(f"Error reading 'new' data (2017+): {e}")
        print("This is the expected 'CANNOT_MERGE_SCHEMAS' error. Naively falling back to a single file.")
        # A common naive fix is to just read one file that works to proceed
        # --- FIX: Added 'data/raw/' prefix to parquet path ---
        df_new = spark.read.parquet("data/raw/yellow_tripdata_2023-01.parquet")

    # We assume "old" data is 2011-2016
    print("Reading all 'old' taxi data (2011-2016) with mergeSchema...")
    
    # --- FIX: Catch 'AnalysisException' AND 'Py4JJavaError' ---
    try:
        # --- FIX: Added 'data/raw/' prefix to parquet paths ---
        df_old = spark.read.option("mergeSchema", "true") \
            .parquet("data/raw/yellow_tripdata_2011*.parquet",
                     "data/raw/yellow_tripdata_2012*.parquet",
                     "data/raw/yellow_tripdata_2013*.parquet",
                     "data/raw/yellow_tripdata_2014*.parquet",
                     "data/raw/yellow_tripdata_2015*.parquet",
                     "data/raw/yellow_tripdata_2016*.parquet")
    except (AnalysisException, Py4JJavaError) as e:
        print(f"Error reading 'old' data (2011-2016): {e}")
        print("This is the expected 'CANNOT_MERGE_SCHEMAS' error. Naively falling back to an empty DataFrame.")
        # If old data is missing or fails to read, create an empty DF with the expected schema
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
        old_schema = StructType([
            StructField("Trip_Pickup_DateTime", StringType(), True),
            StructField("Trip_Dropoff_DateTime", StringType(), True),
            StructField("Trip_Distance", DoubleType(), True),
            StructField("Total_Amt", DoubleType(), True),
            StructField("Passenger_Count", IntegerType(), True),
        ])
        df_old = spark.createDataFrame([], old_schema)


    # --- 2. "Naive" Schema Unification ---
    
    # We must unify the column names from the two separate DataFrames.
    
    print("Unifying 'new' data columns...")
    df_new_std = df_new.select(
        col("tpep_pickup_datetime").alias("std_pickup_datetime"),
        col("tpep_dropoff_datetime").alias("std_dropoff_datetime"),
        col("PULocationID").alias("std_PULocationID"),
        col("DOLocationID").alias("std_DOLocationID"),
        col("trip_distance").alias("std_trip_distance"),
        col("total_amount").alias("std_total_amount"),
        col("passenger_count").alias("std_passenger_count")
    )

    print("Unifying 'old' data columns...")
    df_old_std = df_old.select(
        col("Trip_Pickup_DateTime").alias("std_pickup_datetime"),
        col("Trip_Dropoff_DateTime").alias("std_dropoff_datetime"),
        # Old data has no PULocationID/DOLocationID. We create null columns to match the schema.
        lit(None).cast("int").alias("std_PULocationID"),
        lit(None).cast("int").alias("std_DOLocationID"),
        col("Trip_Distance").alias("std_trip_distance"),
        # --- FIX: Corrected typo 'std_total_amout' to 'Total_Amt' ---
        col("Total_Amt").alias("std_total_amount"), 
        col("Passenger_Count").alias("std_passenger_count")
    )
    # The .withColumnRenamed() is no longer needed as we fixed the alias above.
    
    # BAD PRACTICE 3: A massive, full-dataset union
    print("Unioning old and new data...")
    df_unified = df_new_std.unionByName(df_old_std)

    # DEBUG: See the new unified schema
    print("--- Unified Schema ---")
    df_unified.printSchema()
    
    
    # --- 3. "Naive" Transformations (The Bottlenecks) ---
    
    print("Applying transformations with Shuffle Joins...")
    
    # BAD PRACTICE 4: Triggering a "SortMergeJoin" (a massive shuffle).
    # This 'inner' join will *silently drop all 2009-2016 data*
    # because 'std_PULocationID' will be NULL for those rows.
    # This is a major (and common) "naive" data loss bug.
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

    # BAD PRACTICE 5: Another massive shuffle.
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
    
    # Convert string timestamps to actual timestamp type
    # We use our new standardized columns
    df_cleaned = df_with_locations.withColumn("tpep_pickup_datetime", to_timestamp(col("std_pickup_datetime"))) \
                                  .withColumn("tpep_dropoff_datetime", to_timestamp(col("std_dropoff_datetime")))
    
    # Create a new feature: trip duration in minutes
    df_cleaned = df_cleaned.withColumn(
        "trip_duration_mins",
        round((col("tpep_dropoff_datetime").cast("long") - col("tpep_pickup_datetime").cast("long")) / 60, 2)
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
        "tpep_pickup_datetime",
        "trip_duration_mins",
        col("std_trip_distance").alias("trip_distance"),
        col("std_total_amount").alias("total_amount"),
        col("std_passenger_count").alias("passenger_count"),
        "pickup_borough",
        "dropoff_borough"
    )

    # --- 5. Baseline Analytics (Another Bottleneck) ---
    
    print("Running baseline analytics...")
    
    # BAD PRACTICE 6: A wide aggregation (groupBy)
    analytics_df = final_df.groupBy("pickup_borough").agg(
        avg("trip_distance").alias("avg_trip_distance"),
        avg("total_amount").alias("avg_total_amount"),
        max("trip_duration_mins").alias("max_trip_duration"),
        count(lit(1)).alias("total_trips")
    )
    
    
    # --- 6. Action: Show and Write Results ---
    
    # BAD PRACTICE 7: Triggering the *entire* DAG (computation) twice.
    print("--- Analytics Results (Naive) ---")
    analytics_df.show(truncate=False)
    
    print("Writing cleaned data to 'data/cleaned_trips_baseline'...")
    final_df.write \
        .mode("overwrite") \
        .parquet("data/cleaned_trips_baseline")

    print("--- Naive Baseline Job Complete ---")

if __name__ == "__main__":
    spark_session = create_spark_session()
    run_naive_etl_and_analytics(spark_session)
    spark_session.stop()