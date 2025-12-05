import time
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, hour, dayofweek, month, round, year, when, max, min, sum, desc

def create_analytics_session():
    """Create optimized Spark session for analytics with proper memory configuration."""
    return SparkSession.builder \
        .appName("Final_Analytics_Deep_Dive") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.driver.memory", "16g") \
        .config("spark.driver.maxResultSize", "8g") \
        .config("spark.executor.memory", "16g") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.2") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
        .config("spark.sql.files.maxPartitionBytes", "256MB") \
        .getOrCreate()

def format_dataframe(df, max_rows=None):
    """Format DataFrame for output."""
    rows = df.collect()
    if max_rows:
        rows = rows[:max_rows]
    
    # Get column names
    cols = df.columns
    
    # Calculate column widths using Python's built-in max (avoid conflict with Spark's max)
    import builtins
    col_widths = []
    for i, col in enumerate(cols):
        col_name_len = len(str(col))
        if rows:
            max_val_len = builtins.max((len(str(row[i])) for row in rows), default=0)
        else:
            max_val_len = 0
        col_widths.append(builtins.max(col_name_len, max_val_len))
    
    # Build output
    output = []
    # Header
    header = "|" + "|".join(f" {col:<{col_widths[i]}} " for i, col in enumerate(cols)) + "|"
    output.append(header)
    output.append("|" + "|".join("-" * (w + 2) for w in col_widths) + "|")
    
    # Rows
    for row in rows:
        row_str = "|" + "|".join(f" {str(row[i]):<{col_widths[i]}} " for i in range(len(cols))) + "|"
        output.append(row_str)
    
    return "\n".join(output)

def run_analytics(spark, output_file="analytics_results.txt"):
    """
    Run analytics and save results to a file.
    
    Args:
        spark: SparkSession
        output_file: Path to output file (default: "analytics_results.txt")
    """
    output_path = output_file
    output_lines = []
    
    def log(message):
        """Log message to both console and output buffer."""
        print(message)
        output_lines.append(message)
    
    log("=" * 80)
    log("--- Starting Final Analytics ---")
    log(f"Results will be saved to: {output_path}")
    log("=" * 80)
    start_time = time.time()

    # 1. READ the Optimized Data (Result of your teammates' work)
    # OPTIMIZATION: Don't cache the entire dataset - Parquet is columnar and efficient to read
    # Instead, we'll create a lazy DataFrame with enriched columns that Spark can optimize
    log("\n[Loading] Reading optimized data...")
    load_start = time.time()
    df = spark.read.parquet("data/cleaned_trips_optimized")
    
    # OPTIMIZATION: Create enriched DataFrame lazily (no cache)
    # Spark will optimize reads from Parquet files which are already columnar and compressed
    # Extract temporal features that will be used in multiple queries
    # Note: Column name is "pickup_datetime" based on optimized_etl.py output
    df_enriched = df.withColumn("pickup_hour", hour("pickup_datetime")) \
                    .withColumn("pickup_day_of_week", dayofweek("pickup_datetime")) \
                    .withColumn("pickup_month", month("pickup_datetime")) \
                    .withColumn("pickup_year", year("pickup_datetime")) \
                    .withColumn("speed_mph", 
                               when(col("trip_duration_mins") > 0,
                                    col("trip_distance") / (col("trip_duration_mins") / 60))
                               .otherwise(None))
    
    # OPTIMIZATION: Skip full count to avoid materializing entire dataset
    # Parquet metadata can give us approximate count, or we can estimate from file sizes
    # For exact count, we'll compute it lazily during first query if needed
    load_end = time.time()
    log(f"✓ Data schema loaded successfully")
    log(f"  Load Time: {load_end - load_start:.2f} seconds")
    log("  NOTE: Using lazy evaluation - Spark will optimize reads from Parquet files")
    log("  NOTE: Row count will be computed during first aggregation query")

    # --- INSIGHT 1: Borough Popularity & Profitability ---
    log("\n" + "=" * 80)
    log("[1] Borough Analysis: Popularity & Profitability")
    log("=" * 80)
    insight1_start = time.time()
    borough_stats = df_enriched.groupBy("pickup_borough") \
      .agg(
          count("*").alias("trip_count"),
          round(avg("total_amount"), 2).alias("avg_fare"),
          round(avg("trip_distance"), 2).alias("avg_distance"),
          round(avg("trip_duration_mins"), 2).alias("avg_duration_mins"),
          round(sum("total_amount"), 2).alias("total_revenue"),
          round(avg("passenger_count"), 2).alias("avg_passengers")
      ) \
      .orderBy(desc("avg_fare"))
    
    log("\n" + format_dataframe(borough_stats))
    insight1_end = time.time()
    log(f"  Query Time: {insight1_end - insight1_start:.2f} seconds")

    # --- INSIGHT 2: Traffic/Congestion Analysis (MPH) ---
    # OPTIMIZATION: Use pre-computed speed_mph column instead of recalculating
    log("\n" + "=" * 80)
    log("[2] Congestion Analysis: Average Speed (MPH) by Hour of Day")
    log("=" * 80)
    insight2_start = time.time()
    
    congestion_by_hour = df_enriched.filter(col("speed_mph").isNotNull()) \
            .groupBy("pickup_hour") \
            .agg(
                round(avg("speed_mph"), 2).alias("avg_speed_mph"),
                count("*").alias("trip_count"),
                round(avg("trip_duration_mins"), 2).alias("avg_duration_mins")
            ) \
            .orderBy("pickup_hour")
    
    log("\n" + format_dataframe(congestion_by_hour, max_rows=24))
    insight2_end = time.time()
    log(f"  Query Time: {insight2_end - insight2_start:.2f} seconds")
    log("  NOTE: Lower speeds during 8am-9am and 5pm-6pm indicate rush hour congestion")

    # --- INSIGHT 3: Yearly Trend (Did COVID kill taxis?) ---
    log("\n" + "=" * 80)
    log("[3] Yearly Trip Trends (2011-2024) - Impact Analysis")
    log("=" * 80)
    insight3_start = time.time()
    
    # OPTIMIZATION: Use pre-computed pickup_year column
    # DATA QUALITY FIX: Filter out invalid years (only keep 2011-2024)
    yearly_trends = df_enriched.filter(
        (col("pickup_year") >= 2011) & (col("pickup_year") <= 2024)
    ).groupBy("pickup_year") \
      .agg(
          count("*").alias("trip_count"),
          round(avg("total_amount"), 2).alias("avg_fare"),
          round(avg("trip_distance"), 2).alias("avg_distance")
      ) \
      .orderBy("pickup_year")
    
    log("\n" + format_dataframe(yearly_trends))
    insight3_end = time.time()
    log(f"  Query Time: {insight3_end - insight3_start:.2f} seconds")
    log("  NOTE: Look for drop in 2020-2021 (COVID impact)")

    # --- INSIGHT 4: Trip Distance Categories ---
    log("\n" + "=" * 80)
    log("[4] Trip Distance Categories Analysis")
    log("=" * 80)
    insight4_start = time.time()
    
    # FIX: Correct syntax for when() function
    trip_categories = df_enriched.withColumn("trip_category",
        when(col("trip_distance") > 10, "Long Haul (>10mi)")
        .when(col("trip_distance") > 5, "Medium (5-10mi)")
        .when(col("trip_distance") > 2, "Short (2-5mi)")
        .otherwise("Very Short (<2mi)")) \
      .groupBy("trip_category") \
      .agg(
          count("*").alias("trip_count"),
          round(avg("total_amount"), 2).alias("avg_cost"),
          round(avg("trip_distance"), 2).alias("avg_distance"),
          round(avg("passenger_count"), 1).alias("avg_passengers"),
          round(avg("trip_duration_mins"), 2).alias("avg_duration_mins")
      ) \
      .orderBy(desc("avg_cost"))
    
    log("\n" + format_dataframe(trip_categories))
    insight4_end = time.time()
    log(f"  Query Time: {insight4_end - insight4_start:.2f} seconds")
    
    # --- INSIGHT 5: Day of Week Patterns ---
    log("\n" + "=" * 80)
    log("[5] Day of Week Patterns")
    log("=" * 80)
    insight5_start = time.time()
    
    day_names = {1: "Sunday", 2: "Monday", 3: "Tuesday", 4: "Wednesday", 
                 5: "Thursday", 6: "Friday", 7: "Saturday"}
    
    dow_patterns = df_enriched.groupBy("pickup_day_of_week") \
      .agg(
          count("*").alias("trip_count"),
          round(avg("total_amount"), 2).alias("avg_fare"),
          round(avg("speed_mph"), 2).alias("avg_speed_mph")
      ) \
      .orderBy("pickup_day_of_week")
    
    # Add day names for readability
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType
    
    day_name_udf = udf(lambda x: day_names.get(x, "Unknown"), StringType())
    dow_patterns = dow_patterns.withColumn("day_name", day_name_udf(col("pickup_day_of_week")))
    
    dow_display = dow_patterns.select("day_name", "trip_count", "avg_fare", "avg_speed_mph") \
                .orderBy("pickup_day_of_week")
    log("\n" + format_dataframe(dow_display))
    insight5_end = time.time()
    log(f"  Query Time: {insight5_end - insight5_start:.2f} seconds")
    
    # --- INSIGHT 6: Borough-to-Borough Popular Routes ---
    log("\n" + "=" * 80)
    log("[6] Top Borough-to-Borough Routes")
    log("=" * 80)
    insight6_start = time.time()
    
    top_routes = df_enriched.groupBy("pickup_borough", "dropoff_borough") \
      .agg(
          count("*").alias("trip_count"),
          round(avg("total_amount"), 2).alias("avg_fare"),
          round(avg("trip_distance"), 2).alias("avg_distance")
      ) \
      .orderBy(desc("trip_count")) \
      .limit(10)
    
    log("\n" + format_dataframe(top_routes))
    insight6_end = time.time()
    log(f"  Query Time: {insight6_end - insight6_start:.2f} seconds")
    
    # --- INSIGHT 7: Monthly/Seasonal Patterns ---
    log("\n" + "=" * 80)
    log("[7] Monthly/Seasonal Patterns Analysis")
    log("=" * 80)
    insight7_start = time.time()
    
    monthly_patterns = df_enriched.groupBy("pickup_month") \
      .agg(
          count("*").alias("trip_count"),
          round(avg("total_amount"), 2).alias("avg_fare"),
          round(avg("trip_distance"), 2).alias("avg_distance"),
          round(avg("speed_mph"), 2).alias("avg_speed_mph")
      ) \
      .orderBy("pickup_month")
    
    month_names = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
                   7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"}
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType
    month_name_udf = udf(lambda x: month_names.get(x, "Unknown"), StringType())
    monthly_display = monthly_patterns.withColumn("month_name", month_name_udf(col("pickup_month"))) \
                                     .select("month_name", "trip_count", "avg_fare", "avg_distance", "avg_speed_mph") \
                                     .orderBy("pickup_month")
    
    log("\n" + format_dataframe(monthly_display))
    insight7_end = time.time()
    log(f"  Query Time: {insight7_end - insight7_start:.2f} seconds")
    log("  NOTE: Look for seasonal patterns (summer vs winter)")
    
    # --- INSIGHT 8: Airport Trips Analysis ---
    log("\n" + "=" * 80)
    log("[8] Airport Trips Analysis (EWR, JFK, LGA)")
    log("=" * 80)
    insight8_start = time.time()
    
    airport_trips = df_enriched.filter(
        (col("pickup_borough") == "EWR") | 
        (col("dropoff_borough") == "EWR") |
        (col("airport_fee") > 0)
    ).groupBy("pickup_borough", "dropoff_borough") \
     .agg(
         count("*").alias("trip_count"),
         round(avg("total_amount"), 2).alias("avg_fare"),
         round(avg("trip_distance"), 2).alias("avg_distance"),
         round(avg("trip_duration_mins"), 2).alias("avg_duration_mins"),
         round(sum("airport_fee"), 2).alias("total_airport_fees")
     ) \
     .orderBy(desc("trip_count")) \
     .limit(15)
    
    log("\n" + format_dataframe(airport_trips))
    insight8_end = time.time()
    log(f"  Query Time: {insight8_end - insight8_start:.2f} seconds")
    
    # --- INSIGHT 9: Passenger Count Patterns ---
    log("\n" + "=" * 80)
    log("[9] Passenger Count Patterns")
    log("=" * 80)
    insight9_start = time.time()
    
    passenger_patterns = df_enriched.groupBy("passenger_count") \
      .agg(
          count("*").alias("trip_count"),
          round(avg("total_amount"), 2).alias("avg_fare"),
          round(avg("trip_distance"), 2).alias("avg_distance"),
          round(avg("trip_duration_mins"), 2).alias("avg_duration_mins")
      ) \
      .orderBy("passenger_count")
    
    log("\n" + format_dataframe(passenger_patterns))
    insight9_end = time.time()
    log(f"  Query Time: {insight9_end - insight9_start:.2f} seconds")
    log("  NOTE: Most trips are solo (1 passenger)")
    
    # --- INSIGHT 10: Fare Efficiency (Fare per Mile) ---
    log("\n" + "=" * 80)
    log("[10] Fare Efficiency Analysis (Fare per Mile by Borough)")
    log("=" * 80)
    insight10_start = time.time()
    
    fare_efficiency = df_enriched.filter(col("trip_distance") > 0) \
      .withColumn("fare_per_mile", col("total_amount") / col("trip_distance")) \
      .groupBy("pickup_borough") \
      .agg(
          count("*").alias("trip_count"),
          round(avg("fare_per_mile"), 2).alias("avg_fare_per_mile"),
          round(avg("total_amount"), 2).alias("avg_fare"),
          round(avg("trip_distance"), 2).alias("avg_distance")
      ) \
      .orderBy(desc("avg_fare_per_mile"))
    
    log("\n" + format_dataframe(fare_efficiency))
    insight10_end = time.time()
    log(f"  Query Time: {insight10_end - insight10_start:.2f} seconds")
    log("  NOTE: Higher fare/mile indicates premium routes or traffic")
    
    # --- INSIGHT 11: Weekend vs Weekday Comparison ---
    log("\n" + "=" * 80)
    log("[11] Weekend vs Weekday Patterns")
    log("=" * 80)
    insight11_start = time.time()
    
    weekend_comparison = df_enriched.withColumn("is_weekend",
        when((col("pickup_day_of_week") == 1) | (col("pickup_day_of_week") == 7), "Weekend")
        .otherwise("Weekday")) \
      .groupBy("is_weekend") \
      .agg(
          count("*").alias("trip_count"),
          round(avg("total_amount"), 2).alias("avg_fare"),
          round(avg("trip_distance"), 2).alias("avg_distance"),
          round(avg("speed_mph"), 2).alias("avg_speed_mph"),
          round(avg("trip_duration_mins"), 2).alias("avg_duration_mins")
      ) \
      .orderBy("is_weekend")
    
    log("\n" + format_dataframe(weekend_comparison))
    insight11_end = time.time()
    log(f"  Query Time: {insight11_end - insight11_start:.2f} seconds")
    
    # --- INSIGHT 12: Peak Hours Analysis (Top 5 Busiest Hours) ---
    log("\n" + "=" * 80)
    log("[12] Peak Hours Analysis (Top 10 Busiest Hours)")
    log("=" * 80)
    insight12_start = time.time()
    
    peak_hours = df_enriched.groupBy("pickup_hour") \
      .agg(
          count("*").alias("trip_count"),
          round(avg("total_amount"), 2).alias("avg_fare"),
          round(avg("speed_mph"), 2).alias("avg_speed_mph"),
          round(sum("total_amount"), 2).alias("total_revenue")
      ) \
      .orderBy(desc("trip_count")) \
      .limit(10)
    
    log("\n" + format_dataframe(peak_hours))
    insight12_end = time.time()
    log(f"  Query Time: {insight12_end - insight12_start:.2f} seconds")
    log("  NOTE: Identifies busiest hours for demand planning")

    end_time = time.time()
    total_time = end_time - start_time
    log("\n" + "=" * 80)
    log(f"✓ Analytics Complete in {total_time:.2f} seconds")
    log("=" * 80)
    
    # Write all output to file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(output_lines))
    
    print(f"\n✓ Results saved to: {output_path}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Run NYC Taxi Analytics')
    parser.add_argument('--output', '-o', default='analytics_results.txt',
                       help='Output file path (default: analytics_results.txt)')
    args = parser.parse_args()
    
    spark = create_analytics_session()
    run_analytics(spark, args.output)
    spark.stop()