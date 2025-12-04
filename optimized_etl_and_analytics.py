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
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .getOrCreate()
    return spark

# --- SCHEMAS (Aggressively Pruned) ---

# Schema A: 2011 - mid-2018 + Late 2024
# Strategy: EXCLUDE all unstable fees (airport_fee, congestion_surcharge).
# We only read the core columns which we know are Long/Int compatible.
SCHEMA_LONG_PASSENGERS = StructType([
    StructField("tpep_pickup_datetime", TimestampNTZType(), True),
    StructField("tpep_dropoff_datetime", TimestampNTZType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("passenger_count", LongType(), True), # <--- LONG
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    # Legacy time columns for 2011-2016 compatibility
    StructField("pickup_datetime", StringType(), True),
    StructField("dropoff_datetime", StringType(), True),
])

# Schema B: mid-2018 - mid-2024
# Strategy: Passenger count is Double here.
# We read airport_fee as String to avoid Int/Double crashes in modern files.
SCHEMA_DOUBLE_PASSENGERS = StructType([
    StructField("tpep_pickup_datetime", TimestampNTZType(), True),
    StructField("tpep_dropoff_datetime", TimestampNTZType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("passenger_count", DoubleType(), True), # <--- DOUBLE
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("airport_fee", StringType(), True), # Read as String for safety
    StructField("Airport_fee", StringType(), True)
])

def run_optimized_etl(spark):
    print("--- Starting Optimized ETL Job (Pruned Schema Strategy) ---")

    print("Reading lookup table...")
    taxi_zone_lookup_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/taxi_zone_lookup.csv") \
        .alias("zones") 

    # --- DEFINE FILE GROUPS ---
    
    # Group 1: Long Integers
    files_long_1 = []
    for year in range(2011, 2018):
        files_long_1.append(f"data/raw/yellow_tripdata_{year}*.parquet")
    for i in range(1, 7):
        files_long_1.append(f"data/raw/yellow_tripdata_2018-0{i}.parquet")
    # The 2024 Regression files
    files_long_1.append("data/raw/yellow_tripdata_2024-10.parquet")
    files_long_1.append("data/raw/yellow_tripdata_2024-11.parquet")
    files_long_1.append("data/raw/yellow_tripdata_2024-12.parquet")

    # Group 2: Doubles
    files_double = []
    for i in range(7, 13):
        files_double.append(f"data/raw/yellow_tripdata_2018-{i:02d}.parquet")
    for year in range(2019, 2024):
        files_double.append(f"data/raw/yellow_tripdata_{year}*.parquet")
    for i in range(1, 10):
        files_double.append(f"data/raw/yellow_tripdata_2024-0{i}.parquet")

    # --- READ BATCH 1 ---
    print("Reading Group 1 (Long Passengers)...")
    # We use the strict schema which EXCLUDES airport_fee
    df_long = spark.read.schema(SCHEMA_LONG_PASSENGERS).parquet(*files_long_1) \
        .select(
            coalesce(col("tpep_pickup_datetime"), to_timestamp(col("pickup_datetime"))).alias("pickup_datetime"),
            coalesce(col("tpep_dropoff_datetime"), to_timestamp(col("dropoff_datetime"))).alias("dropoff_datetime"),
            col("PULocationID"),
            col("DOLocationID"),
            col("trip_distance"),
            col("total_amount"),
            col("passenger_count").cast(IntegerType()),
            # Manually add airport_fee as 0.0 since we skipped reading it
            lit(0.0).alias("airport_fee")
        )

    # --- READ BATCH 2 ---
    print("Reading Group 2 (Double Passengers)...")
    df_double = spark.read.schema(SCHEMA_DOUBLE_PASSENGERS).parquet(*files_double) \
        .select(
            col("tpep_pickup_datetime").alias("pickup_datetime"),
            col("tpep_dropoff_datetime").alias("dropoff_datetime"),
            col("PULocationID"),
            col("DOLocationID"),
            col("trip_distance"),
            col("total_amount"),
            col("passenger_count").cast(IntegerType()), 
            # Cast String -> Double for the fee
            coalesce(col("airport_fee"), col("Airport_fee")).cast(DoubleType()).alias("airport_fee")
        )

    # --- UNION ---
    print("Unioning datasets...")
    raw_df = df_long.unionByName(df_double)

    # --- FEATURE ENGINEERING ---
    print("Calculating trip duration...")
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
    
    # Only count if you have resources, otherwise skip this line
    # print(f"Total processed rows: {df_joined.count()}")

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
    print("Writing final optimized dataset (coalesced)...")
    df_joined.coalesce(10).write \
        .mode("overwrite") \
        .parquet("data/cleaned_trips_optimized")
        
    print("Optimization Job Complete.")

if __name__ == "__main__":
    spark = create_optimized_spark_session()
    run_optimized_etl(spark)
    spark.stop()