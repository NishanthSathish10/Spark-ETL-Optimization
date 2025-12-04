import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, round, avg, count, max, to_timestamp, coalesce, broadcast
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, TimestampNTZType, TimestampType

def create_optimized_spark_session():
    spark = SparkSession.builder \
        .appName("Optimized_ETL_and_Analytics") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.sql.caseSensitive", "true") \
        # Constraint: Must use non-vectorized to handle Int->Double promotion safely
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .getOrCreate()
    return spark

# --- SCHEMAS ---
# We use DoubleType for fees/metrics because Int32->Double is safe in non-vectorized reader.
# We MUST match passenger_count to the file's physical type (Long vs Double) to avoid crashes.

# ERA 1: Long Passenger Count
SCHEMA_LONG = StructType([
    StructField("tpep_pickup_datetime", TimestampNTZType(), True),
    StructField("tpep_dropoff_datetime", TimestampNTZType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("passenger_count", LongType(), True), # <--- MATCHES FILE
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("airport_fee", DoubleType(), True),
    StructField("pickup_datetime", StringType(), True),
    StructField("dropoff_datetime", StringType(), True),
])

# ERA 2: Double Passenger Count
SCHEMA_DOUBLE = StructType([
    StructField("tpep_pickup_datetime", TimestampNTZType(), True),
    StructField("tpep_dropoff_datetime", TimestampNTZType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("passenger_count", DoubleType(), True), # <--- MATCHES FILE
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("airport_fee", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True)
])

def generate_file_list(start_year, start_month, end_year, end_month):
    """
    Generates a precise list of file paths month-by-month.
    This avoids wildcard errors where 2018*.parquet grabs the wrong months.
    """
    files = []
    # Handle start year
    s_m = start_month
    e_m = 12 if start_year < end_year else end_month
    
    for year in range(start_year, end_year + 1):
        # Determine month range for this year
        start_m = start_month if year == start_year else 1
        end_m = end_month if year == end_year else 12
        
        for month in range(start_m, end_m + 1):
            files.append(f"data/raw/yellow_tripdata_{year}-{month:02d}.parquet")
            
    return files

def run_optimized_etl(spark):
    print("--- Starting Optimized ETL Job (Precise Month-Batch Strategy) ---")

    print("Reading lookup table...")
    taxi_zone_lookup_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/taxi_zone_lookup.csv") \
        .alias("zones") 

    # --- BATCH 1: The "Long" Era (2011-01 to 2018-06) ---
    print("Reading Batch 1 (2011-01 to 2018-06)...")
    files_1 = generate_file_list(2011, 1, 2018, 6)
    
    df_1 = spark.read.schema(SCHEMA_LONG).parquet(*files_1).select(
        coalesce(col("tpep_pickup_datetime"), to_timestamp(col("pickup_datetime"))).alias("pickup_datetime"),
        coalesce(col("tpep_dropoff_datetime"), to_timestamp(col("dropoff_datetime"))).alias("dropoff_datetime"),
        col("PULocationID"), col("DOLocationID"), col("trip_distance"), col("total_amount"),
        col("passenger_count").cast(IntegerType()),
        col("airport_fee") # Already Double
    )

    # --- BATCH 2: The "Double" Era (2018-07 to 2024-09) ---
    print("Reading Batch 2 (2018-07 to 2024-09)...")
    files_2 = generate_file_list(2018, 7, 2024, 9)
    
    df_2 = spark.read.schema(SCHEMA_DOUBLE).parquet(*files_2).select(
        col("tpep_pickup_datetime").alias("pickup_datetime"),
        col("tpep_dropoff_datetime").alias("dropoff_datetime"),
        col("PULocationID"), col("DOLocationID"), col("trip_distance"), col("total_amount"),
        col("passenger_count").cast(IntegerType()), # Cast Double -> Int
        coalesce(col("airport_fee"), col("Airport_fee")).alias("airport_fee")
    )

    # --- BATCH 3: The "Regression" Era (2024-10 to 2024-12) ---
    # This uses SCHEMA_LONG again because passenger_count reverted to Long
    print("Reading Batch 3 (2024-10 to 2024-12)...")
    files_3 = generate_file_list(2024, 10, 2024, 12)
    
    df_3 = spark.read.schema(SCHEMA_LONG).parquet(*files_3).select(
        coalesce(col("tpep_pickup_datetime"), to_timestamp(col("pickup_datetime"))).alias("pickup_datetime"),
        coalesce(col("tpep_dropoff_datetime"), to_timestamp(col("dropoff_datetime"))).alias("dropoff_datetime"),
        col("PULocationID"), col("DOLocationID"), col("trip_distance"), col("total_amount"),
        col("passenger_count").cast(IntegerType()),
        col("airport_fee")
    )

    # --- UNION ---
    print("Unioning datasets...")
    raw_df = df_1.unionByName(df_2).unionByName(df_3)

    # --- FEATURE ENGINEERING ---
    print("Calculating trip duration...")
    # Use TimestampType cast to fix NTZ error
    df_features = raw_df.withColumn(
        "trip_duration_mins",
        round(
            (col("dropoff_datetime").cast(TimestampType()).cast("double") - \
             col("pickup_datetime").cast(TimestampType()).cast("double")) / 60, 
            2
        )
    )

    # --- FILTERING ---
    print("Filtering invalid rows...")
    df_filtered = df_features.filter(
        (col("trip_distance") > 0) & 
        (col("total_amount") > 0) & 
        (col("trip_duration_mins") > 1) & 
        (col("trip_duration_mins") < 240) & 
        (col("PULocationID").isNotNull()) & 
        (col("DOLocationID").isNotNull())
    )

    # --- JOIN ---
    print("Performing BROADCAST Joins...")
    pickup_zones = taxi_zone_lookup_df.alias("pickup_zones")
    dropoff_zones = taxi_zone_lookup_df.alias("dropoff_zones")
    
    df_joined = df_filtered.join(
        broadcast(pickup_zones), 
        col("PULocationID") == col("pickup_zones.LocationID"),
        "inner"
    ).join(
        broadcast(dropoff_zones), 
        col("DOLocationID") == col("dropoff_zones.LocationID"),
        "inner"
    ).select(
        df_filtered["*"],
        col("pickup_zones.Borough").alias("pickup_borough"),
        col("dropoff_zones.Borough").alias("dropoff_borough")
    )

    # --- CACHING & ANALYTICS ---
    print("Caching transformed dataset...")
    df_joined.cache()
    
    # Just one analytics action to verify correctness and trigger cache
    print("--- Top Pickup Boroughs ---")
    df_joined.groupBy("pickup_borough").count().orderBy(col("count").desc()).show(truncate=False)
    
    print("--- Avg Metrics by Borough ---")
    df_joined.groupBy("pickup_borough").agg(
        avg("trip_distance").alias("avg_dist"),
        avg("total_amount").alias("avg_cost"),
        avg("trip_duration_mins").alias("avg_time"),
        avg("passenger_count").alias("avg_passengers"),
        avg("airport_fee").alias("avg_airport_fee")
    ).orderBy(col("avg_cost").desc()).show(truncate=False)

    # --- WRITE ---
    print("Writing final optimized dataset...")
    df_joined.coalesce(10).write \
        .mode("overwrite") \
        .parquet("data/cleaned_trips_optimized")
        
    print("Optimization Job Complete.")

if __name__ == "__main__":
    spark = create_optimized_spark_session()
    run_optimized_etl(spark)
    spark.stop()