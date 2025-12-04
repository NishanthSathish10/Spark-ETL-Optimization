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

# --- SCHEMAS ---
# We need TWO schemas because the data type of 'passenger_count' 
# changed from Long to Double in July 2018.

# Schema A: 2011 - June 2018 (Passenger Count is Long)
SCHEMA_ERA_1 = StructType([
    StructField("tpep_pickup_datetime", TimestampNTZType(), True),
    StructField("tpep_dropoff_datetime", TimestampNTZType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("passenger_count", LongType(), True), # <--- LONG
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    # Compatibility fields
    StructField("pickup_datetime", StringType(), True),
    StructField("dropoff_datetime", StringType(), True),
])

# Schema B: July 2018 - 2024 (Passenger Count is Double)
SCHEMA_ERA_2 = StructType([
    StructField("tpep_pickup_datetime", TimestampNTZType(), True),
    StructField("tpep_dropoff_datetime", TimestampNTZType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("passenger_count", DoubleType(), True), # <--- DOUBLE
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("airport_fee", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True)
])

def run_optimized_etl(spark):
    print("--- Starting Optimized ETL Job (Split-Era Strategy) ---")

    print("Reading lookup table...")
    taxi_zone_lookup_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/taxi_zone_lookup.csv") \
        .alias("zones") # Alias early to prevent ambiguity

    # --- STEP 1: READ BATCH 1 (The Integer Era: 2011 - June 2018) ---
    print("Reading Batch 1: 2011 to June 2018 (Integer Schema)...")
    
    # We construct the list of files for this era manually to be precise
    files_era_1 = []
    # 2011-2017 (Full years)
    files_era_1.extend(["data/raw/yellow_tripdata_201*.parquet"])
    # 2018 Jan-Jun (Specific months)
    files_era_1.extend([f"data/raw/yellow_tripdata_2018-0{i}.parquet" for i in range(1, 7)])

    df_era_1 = spark.read \
        .schema(SCHEMA_ERA_1) \
        .parquet(*files_era_1) \
        .select(
            coalesce(col("tpep_pickup_datetime"), to_timestamp(col("pickup_datetime"))).alias("pickup_datetime"),
            coalesce(col("tpep_dropoff_datetime"), to_timestamp(col("dropoff_datetime"))).alias("dropoff_datetime"),
            col("PULocationID"),
            col("DOLocationID"),
            col("trip_distance"),
            col("total_amount"),
            col("passenger_count").cast(IntegerType()) # Already Long, safe cast
        )

    # --- STEP 2: READ BATCH 2 (The Double Era: July 2018 - 2024) ---
    print("Reading Batch 2: July 2018 to 2024 (Double Schema)...")
    
    files_era_2 = []
    # 2018 Jul-Dec
    files_era_2.extend([f"data/raw/yellow_tripdata_2018-{i:02d}.parquet" for i in range(7, 13)])
    # 2019-2024 (Full years)
    files_era_2.extend(["data/raw/yellow_tripdata_2019*.parquet", "data/raw/yellow_tripdata_202*.parquet"])

    df_era_2 = spark.read \
        .schema(SCHEMA_ERA_2) \
        .parquet(*files_era_2) \
        .select(
            col("tpep_pickup_datetime").alias("pickup_datetime"),
            col("tpep_dropoff_datetime").alias("dropoff_datetime"),
            col("PULocationID"),
            col("DOLocationID"),
            col("trip_distance"),
            col("total_amount"),
            col("passenger_count").cast(IntegerType()) # Cast Double -> Int
        )

    # --- STEP 3: UNION ---
    print("Unioning datasets...")
    raw_df = df_era_1.unionByName(df_era_2)

    # --- STEP 4: FEATURE ENGINEERING ---
    print("Calculating trip duration...")
    # FIX: Cast TimestampNTZ -> Timestamp -> Double to get epoch seconds
    df_features = raw_df.withColumn(
        "trip_duration_mins",
        round(
            (col("dropoff_datetime").cast(TimestampType()).cast("double") - \
             col("pickup_datetime").cast(TimestampType()).cast("double")) / 60, 
            2
        )
    )

    # --- STEP 5: FILTERING ---
    print("Filtering invalid rows...")
    df_filtered = df_features.filter(
        (col("trip_distance") > 0) & 
        (col("total_amount") > 0) & 
        (col("trip_duration_mins") > 1) & 
        (col("trip_duration_mins") < 240) & 
        (col("PULocationID").isNotNull()) & 
        (col("DOLocationID").isNotNull())
    )

    # --- STEP 6: OPTIMIZED JOIN (Broadcast) ---
    print("Performing BROADCAST Joins...")
    
    # Create Aliases for the lookup table
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

    # --- STEP 7: CACHING & ANALYTICS ---
    print("Caching transformed dataset...")
    df_joined.cache()
    
    print(f"Total processed rows: {df_joined.count()}")

    print("--- Top Pickup Boroughs ---")
    df_joined.groupBy("pickup_borough").count().orderBy(col("count").desc()).show(truncate=False)

    print("--- Avg Metrics by Borough ---")
    df_joined.groupBy("pickup_borough").agg(
        avg("trip_distance").alias("avg_dist"),
        avg("total_amount").alias("avg_cost"),
        avg("trip_duration_mins").alias("avg_time"),
        avg("passenger_count").alias("avg_passengers")
    ).orderBy(col("avg_cost").desc()).show(truncate=False)

    # --- STEP 8: WRITE ---
    print("Writing final optimized dataset (coalesced)...")
    df_joined.coalesce(10).write \
        .mode("overwrite") \
        .parquet("data/cleaned_trips_optimized")
        
    print("Optimization Job Complete.")

if __name__ == "__main__":
    spark = create_optimized_spark_session()
    run_optimized_etl(spark)
    spark.stop() 