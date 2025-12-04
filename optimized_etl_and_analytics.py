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

# Schema 1: Legacy (2011 - June 2018)
# - Passenger Count is LONG
# - No Airport Fee (Exclude to avoid Int/Double crashes in early files)
SCHEMA_LEGACY = StructType([
    StructField("tpep_pickup_datetime", TimestampNTZType(), True),
    StructField("tpep_dropoff_datetime", TimestampNTZType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("passenger_count", LongType(), True), 
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("pickup_datetime", StringType(), True),
    StructField("dropoff_datetime", StringType(), True),
])

# Schema 2: Middle Era (July 2018 - Jan 2023)
# - Passenger Count is DOUBLE
# - Airport Fee exists (Double)
SCHEMA_MIDDLE = StructType([
    StructField("tpep_pickup_datetime", TimestampNTZType(), True),
    StructField("tpep_dropoff_datetime", TimestampNTZType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("passenger_count", DoubleType(), True), 
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("airport_fee", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True)
])

# Schema 3: Modern Era (Feb 2023 - 2024)
# - Passenger Count reverted to LONG
# - Airport Fee exists (Double)
SCHEMA_MODERN = StructType([
    StructField("tpep_pickup_datetime", TimestampNTZType(), True),
    StructField("tpep_dropoff_datetime", TimestampNTZType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("passenger_count", LongType(), True), 
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("airport_fee", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True)
])

def run_optimized_etl(spark):
    print("--- Starting Optimized ETL Job (3-Era Strategy) ---")

    print("Reading lookup table...")
    taxi_zone_lookup_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/taxi_zone_lookup.csv") \
        .alias("zones") 

    # --- DEFINE FILE LISTS ---
    
    # 1. Legacy: 2011 - June 2018
    files_1 = []
    for year in range(2011, 2018):
        files_1.append(f"data/raw/yellow_tripdata_{year}*.parquet")
    for i in range(1, 7):
        files_1.append(f"data/raw/yellow_tripdata_2018-0{i}.parquet")

    # 2. Middle: July 2018 - Jan 2023
    files_2 = []
    for i in range(7, 13):
        files_2.append(f"data/raw/yellow_tripdata_2018-{i:02d}.parquet")
    for year in range(2019, 2023):
        files_2.append(f"data/raw/yellow_tripdata_{year}*.parquet")
    files_2.append("data/raw/yellow_tripdata_2023-01.parquet")

    # 3. Modern: Feb 2023 - 2024
    files_3 = []
    for i in range(2, 13):
        files_3.append(f"data/raw/yellow_tripdata_2023-{i:02d}.parquet")
    files_3.append("data/raw/yellow_tripdata_2024*.parquet")

    # --- READ AND STANDARDIZE ---

    print("Reading Batch 1: Legacy (Longs)...")
    df_1 = spark.read.schema(SCHEMA_LEGACY).parquet(*files_1).select(
        coalesce(col("tpep_pickup_datetime"), to_timestamp(col("pickup_datetime"))).alias("pickup_datetime"),
        coalesce(col("tpep_dropoff_datetime"), to_timestamp(col("dropoff_datetime"))).alias("dropoff_datetime"),
        col("PULocationID"), col("DOLocationID"), col("trip_distance"), col("total_amount"),
        col("passenger_count").cast(IntegerType()),
        lit(0.0).alias("airport_fee") # Add missing fee as 0.0
    )

    print("Reading Batch 2: Middle (Doubles)...")
    df_2 = spark.read.schema(SCHEMA_MIDDLE).parquet(*files_2).select(
        col("tpep_pickup_datetime").alias("pickup_datetime"),
        col("tpep_dropoff_datetime").alias("dropoff_datetime"),
        col("PULocationID"), col("DOLocationID"), col("trip_distance"), col("total_amount"),
        col("passenger_count").cast(IntegerType()), # Cast Double -> Int
        coalesce(col("airport_fee"), col("Airport_fee")).alias("airport_fee")
    )

    print("Reading Batch 3: Modern (Longs)...")
    df_3 = spark.read.schema(SCHEMA_MODERN).parquet(*files_3).select(
        col("tpep_pickup_datetime").alias("pickup_datetime"),
        col("tpep_dropoff_datetime").alias("dropoff_datetime"),
        col("PULocationID"), col("DOLocationID"), col("trip_distance"), col("total_amount"),
        col("passenger_count").cast(IntegerType()), # Already Long
        coalesce(col("airport_fee"), col("Airport_fee")).alias("airport_fee")
    )

    # --- UNION ---
    print("Unioning datasets...")
    raw_df = df_1.unionByName(df_2).unionByName(df_3)

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
    
    print(f"Total processed rows: {df_joined.count()}")

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