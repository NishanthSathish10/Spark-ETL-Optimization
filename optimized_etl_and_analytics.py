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
        .config("spark.sql.parquet.enableVectorizedReader", "true") \
        .getOrCreate()
        # Vectorized reader works for 2011 (Double dicts) and 2018+ (Double types)
        # It might fail on 2012-2017 (Long types cast to Double), so we might need
        # to be careful, but let's try keeping it enabled as it's fastest.
    return spark

# --- SCHEMAS ---

# Schema A: 2011 ONLY (Many doubles, dict encoding)
SCHEMA_2011 = StructType([
    StructField("tpep_pickup_datetime", TimestampNTZType(), True),
    StructField("tpep_dropoff_datetime", TimestampNTZType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("passenger_count", LongType(), True),
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    # 2011 files are weird, exclude airport_fee entirely
    StructField("pickup_datetime", StringType(), True),
    StructField("dropoff_datetime", StringType(), True),
])

# Schema B: 2012 - June 2018 (Stable Long types)
SCHEMA_2012_2018 = StructType([
    StructField("tpep_pickup_datetime", TimestampNTZType(), True),
    StructField("tpep_dropoff_datetime", TimestampNTZType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("passenger_count", LongType(), True),
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    # airport_fee absent or unstable, exclude
    StructField("pickup_datetime", StringType(), True),
    StructField("dropoff_datetime", StringType(), True),
])

# Schema C: July 2018 - 2024 (Double types)
SCHEMA_2018_2024 = StructType([
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

def run_optimized_etl(spark):
    print("--- Starting Optimized ETL Job (Triple-Split Strategy) ---")

    print("Reading lookup table...")
    taxi_zone_lookup_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/taxi_zone_lookup.csv") \
        .alias("zones") 

    # --- BATCH 1: 2011 ---
    print("Reading Batch 1: 2011...")
    df_2011 = spark.read.schema(SCHEMA_2011).parquet("data/raw/yellow_tripdata_2011*.parquet") \
        .select(
            coalesce(col("tpep_pickup_datetime"), to_timestamp(col("pickup_datetime"))).alias("pickup_datetime"),
            coalesce(col("tpep_dropoff_datetime"), to_timestamp(col("dropoff_datetime"))).alias("dropoff_datetime"),
            col("PULocationID"), col("DOLocationID"), col("trip_distance"), col("total_amount"),
            col("passenger_count").cast(IntegerType()),
            lit(0.0).alias("airport_fee")
        )

    # --- BATCH 2: 2012 - June 2018 ---
    print("Reading Batch 2: 2012 to June 2018...")
    files_2012_2018 = []
    for year in range(2012, 2018):
        files_2012_2018.append(f"data/raw/yellow_tripdata_{year}*.parquet")
    for i in range(1, 7):
        files_2012_2018.append(f"data/raw/yellow_tripdata_2018-0{i}.parquet")

    df_2012_2018 = spark.read.schema(SCHEMA_2012_2018).parquet(*files_2012_2018) \
        .select(
            coalesce(col("tpep_pickup_datetime"), to_timestamp(col("pickup_datetime"))).alias("pickup_datetime"),
            coalesce(col("tpep_dropoff_datetime"), to_timestamp(col("dropoff_datetime"))).alias("dropoff_datetime"),
            col("PULocationID"), col("DOLocationID"), col("trip_distance"), col("total_amount"),
            col("passenger_count").cast(IntegerType()),
            lit(0.0).alias("airport_fee")
        )

    # --- BATCH 3: July 2018 - 2024 ---
    print("Reading Batch 3: July 2018 to 2024...")
    files_2018_2024 = []
    for i in range(7, 13):
        files_2018_2024.append(f"data/raw/yellow_tripdata_2018-{i:02d}.parquet")
    files_2018_2024.append("data/raw/yellow_tripdata_2019*.parquet")
    files_2018_2024.append("data/raw/yellow_tripdata_202*.parquet")

    df_2018_2024 = spark.read.schema(SCHEMA_2018_2024).parquet(*files_2018_2024) \
        .select(
            col("tpep_pickup_datetime").alias("pickup_datetime"),
            col("tpep_dropoff_datetime").alias("dropoff_datetime"),
            col("PULocationID"), col("DOLocationID"), col("trip_distance"), col("total_amount"),
            col("passenger_count").cast(IntegerType()),
            coalesce(col("airport_fee"), col("Airport_fee")).cast(DoubleType()).alias("airport_fee")
        )

    # --- UNION ---
    print("Unioning datasets...")
    raw_df = df_2011.unionByName(df_2012_2018).unionByName(df_2018_2024)

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
    # We do NOT call count() here to avoid triggering the job prematurely.
    # We let the analytics actions trigger the cache.
    df_joined.cache()
    
    print("Running Analytics...")
    
    # Top Boroughs
    print("--- Top Pickup Boroughs ---")
    df_joined.groupBy("pickup_borough").count().orderBy(col("count").desc()).show(truncate=False)

    # Avg Metrics
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