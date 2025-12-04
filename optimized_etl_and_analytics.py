import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, round, avg, count, max, to_timestamp, coalesce, broadcast
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, TimestampNTZType, TimestampType

def create_optimized_spark_session():
    """
    Creates a highly optimized Spark Session.
    
    1. AQE (Adaptive Query Execution): Enabled. Allows Spark to re-plan queries 
       at runtime to handle data skew and small partitions.
    2. Shuffle Partitions: Set to 'auto' (via AQE) or a reasonable default (200).
       We removed the 'bad' setting of 8.
    3. Vectorized Reader: DISABLED. 
       Why? This dataset has schema conflicts (INT vs DOUBLE) inside Parquet files.
       The fast vectorized reader crashes on these. The standard reader is slower 
       per-row but robust enough to read ALL files in parallel. 
       Parallelism > Vectorization for this specific dirty dataset.
    """
    spark = SparkSession.builder \
        .appName("Optimized_ETL_and_Analytics") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.sql.caseSensitive", "true") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .getOrCreate()
    return spark

# --- OPTIMIZED SCHEMA STRATEGY ---
# We read all conflicting columns as StringType first.
# This ensures the read operation never fails, no matter what 
# weird data types (Int64, Double, Dict) are in the old files.
MASTER_SCHEMA = StructType([
    StructField("VendorID", StringType(), True),
    StructField("tpep_pickup_datetime", TimestampNTZType(), True),
    StructField("tpep_dropoff_datetime", TimestampNTZType(), True),
    StructField("passenger_count", StringType(), True),
    StructField("trip_distance", StringType(), True),
    StructField("RatecodeID", StringType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", StringType(), True),
    StructField("DOLocationID", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("fare_amount", StringType(), True),
    StructField("extra", StringType(), True),
    StructField("mta_tax", StringType(), True),
    StructField("tip_amount", StringType(), True),
    StructField("tolls_amount", StringType(), True),
    StructField("improvement_surcharge", StringType(), True),
    StructField("total_amount", StringType(), True),
    StructField("congestion_surcharge", StringType(), True),
    StructField("airport_fee", StringType(), True),
    StructField("Airport_fee", StringType(), True),
    
    # Old 2010 columns (included so we don't crash, but ignored)
    StructField("pickup_datetime", StringType(), True),
    StructField("dropoff_datetime", StringType(), True),
])

def run_optimized_etl(spark):
    print("--- Starting Optimized ETL Job ---")

    # 1. Load Lookup Table
    print("Reading lookup table...")
    taxi_zone_lookup_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/taxi_zone_lookup.csv")

    # 2. Parallel Read of All Data (2011-2024)
    # We ignore 2009/2010 (different schema) and read everything else in one go.
    # This is the massive parallel IO operation.
    print("Reading 13 years of taxi data in parallel...")
    raw_df = spark.read \
        .schema(MASTER_SCHEMA) \
        .parquet("data/raw/yellow_tripdata_201*.parquet",
                 "data/raw/yellow_tripdata_202*.parquet")

    # 3. Projection & Casting (The "Fix" Phase)
    # We efficiently select and cast only the columns we need.
    # This keeps the memory footprint low ("Column Pruning").
    print("Projecting and casting columns...")
    df_std = raw_df.select(
        # Coalesce timestamps (handles schema drift if any old formats slip in)
        coalesce(
            col("tpep_pickup_datetime"), 
            to_timestamp(col("pickup_datetime"))
        ).alias("pickup_datetime"),
        
        coalesce(
            col("tpep_dropoff_datetime"), 
            to_timestamp(col("dropoff_datetime"))
        ).alias("dropoff_datetime"),
        
        # Cast IDs to Long for joining
        col("PULocationID").cast(LongType()),
        col("DOLocationID").cast(LongType()),
        
        # Cast metrics to Double/Int for math
        col("trip_distance").cast(DoubleType()),
        col("total_amount").cast(DoubleType()),
        col("passenger_count").cast(IntegerType())
    )

    # 4. Feature Engineering
    # Calculate duration in minutes. 
    # FIX: TimestampNTZ -> Timestamp -> Double -> Math
    print("Calculating trip duration...")
    df_features = df_std.withColumn(
        "trip_duration_mins",
        round(
            (col("dropoff_datetime").cast(TimestampType()).cast("double") - \
             col("pickup_datetime").cast(TimestampType()).cast("double")) / 60, 
            2
        )
    )

    # 5. Filtering (Data Cleaning)
    # Filter out bad data early to reduce data volume for the join.
    print("Filtering invalid rows...")
    df_filtered = df_features.filter(
        (col("trip_distance") > 0) & 
        (col("total_amount") > 0) & 
        (col("trip_duration_mins") > 1) & 
        (col("trip_duration_mins") < 240) & # < 4 hours
        (col("PULocationID").isNotNull()) & # Must have location to join
        (col("DOLocationID").isNotNull())
    )

    # 6. The Optimized Join (Broadcast)
    # We broadcast the small lookup table. This prevents a Shuffle.
    print("Performing BROADCAST Joins...")
    
    # Join for Pickup Location
    df_joined = df_filtered.join(
        broadcast(taxi_zone_lookup_df), # <--- OPTIMIZATION
        df_filtered.PULocationID == taxi_zone_lookup_df.LocationID,
        "inner"
    ).select(
        df_filtered["*"],
        taxi_zone_lookup_df["Borough"].alias("pickup_borough")
    )

    # Join for Dropoff Location (Reuse broadcast)
    # Note: We do a second join. Since the lookup table is tiny and broadcasted,
    # this is extremely cheap.
    df_final = df_joined.join(
        broadcast(taxi_zone_lookup_df), # <--- OPTIMIZATION
        df_joined.DOLocationID == taxi_zone_lookup_df.LocationID,
        "inner"
    ).select(
        df_joined["*"],
        taxi_zone_lookup_df["Borough"].alias("dropoff_borough")
    )

    # 7. Caching
    # We cache this dataset because we are about to run multiple
    # actions on it (analytics + write).
    print("Caching transformed dataset...")
    df_final.cache()
    
    # Trigger cache with a count (cheap action)
    count_rows = df_final.count()
    print(f"Total processed rows: {count_rows}")

    # --- ANALYTICS PHASE (Runs fast from Cache) ---
    
    print("Running Analytics...")
    
    # A. Top Boroughs by Volume
    print("--- Top Pickup Boroughs ---")
    df_final.groupBy("pickup_borough").count().orderBy(col("count").desc()).show(truncate=False)

    # B. Average Metrics by Borough
    print("--- Avg Metrics by Borough ---")
    df_final.groupBy("pickup_borough").agg(
        avg("trip_distance").alias("avg_dist"),
        avg("total_amount").alias("avg_cost"),
        avg("trip_duration_mins").alias("avg_time")
    ).orderBy(col("avg_cost").desc()).show(truncate=False)

    # --- WRITE PHASE ---
    
    # 8. Optimized Write
    # coalesce(10) combines the thousands of tiny shuffle partitions
    # into 10 large files. This solves the "Small File Problem".
    print("Writing final optimized dataset (coalesced)...")
    df_final.coalesce(10).write \
        .mode("overwrite") \
        .parquet("data/cleaned_trips_optimized")
        
    print("Optimization Job Complete.")

if __name__ == "__main__":
    spark = create_optimized_spark_session()
    run_optimized_etl(spark)
    spark.stop()