import time
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, hour, dayofweek, month, round, year, when, max, min, sum, desc
import pandas as pd
import builtins

# Optional plotting support - only import if matplotlib is available
try:
    from plot import plot_map
    PLOTTING_AVAILABLE = True
except ImportError:
    PLOTTING_AVAILABLE = False
    plot_map = {}

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
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .getOrCreate()

def format_dataframe(df, max_rows=None, plotter=None):
    """
    Converts a PySpark DataFrame to Pandas, formats large numbers to prevent 
    scientific notation, and generates a column-aligned string output.
    """
    pandas_df = df.toPandas()
    if max_rows is not None:
        pandas_df = pandas_df.head(max_rows)
    
    if plotter and PLOTTING_AVAILABLE and plot_map.get(plotter, None):
        plot_map[plotter](pandas_df)
    elif plotter and not PLOTTING_AVAILABLE:
        # Silently skip plotting if matplotlib is not available
        pass

    formatted_data = {}
    col_widths = []
    
    for col in pandas_df.columns:
        if pd.api.types.is_numeric_dtype(pandas_df[col]):
            # Apply formatting to prevent scientific notation (e.g., 1,234,567.89)
            formatted_series = pandas_df[col].apply(lambda x: f'{x:,.2f}')
        else:
            formatted_series = pandas_df[col].astype(str)
            
        formatted_data[col] = formatted_series
        
        # Width Calculation: Find the max of header length and max data length.
        col_name_len = len(str(col))
        max_val_len = formatted_series.str.len().max()
        
        col_widths.append(builtins.max(col_name_len, max_val_len))

    df_formatted = pd.DataFrame(formatted_data)
    cols = df_formatted.columns.tolist()

    # Build the output string
    output = []
    
    # Header
    header = "|" + "|".join(f" {col:<{col_widths[i]}} " for i, col in enumerate(cols)) + "|"
    output.append(header)
    output.append("|" + "|".join("-" * (w + 2) for w in col_widths) + "|")
    
    # Rows
    for _, row in df_formatted.iterrows():
        row_str = "|" + "|".join(
            f" {row[col]:<{col_widths[i]}} " for i, col in enumerate(cols)
        ) + "|"
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
    
    log("\n" + format_dataframe(borough_stats, plotter='borough_stats'))
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
    
    log("\n" + format_dataframe(congestion_by_hour, max_rows=24, plotter='congestion_by_hour'))
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
    # TAIL LATENCY OPTIMIZATION: Use partition pruning if data is partitioned by year
    # Also use column pruning - only select needed columns
    yearly_trends = df_enriched.filter(
        (col("pickup_year") >= 2011) & (col("pickup_year") <= 2024)
    ).select("pickup_year", "total_amount", "trip_distance") \
     .groupBy("pickup_year") \
     .agg(
         count("*").alias("trip_count"),
         round(avg("total_amount"), 2).alias("avg_fare"),
         round(avg("trip_distance"), 2).alias("avg_distance")
     ) \
     .orderBy("pickup_year")
    
    log("\n" + format_dataframe(yearly_trends, plotter='yearly_trends'))
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
    log("\n" + format_dataframe(dow_display, plotter='dow_patterns'))
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
    
    log("\n" + format_dataframe(top_routes, plotter='top_routes'))
    insight6_end = time.time()
    log(f"  Query Time: {insight6_end - insight6_start:.2f} seconds")
    
    # --- INSIGHT 7: Monthly/Seasonal Patterns ---
    log("\n" + "=" * 80)
    log("[7] Monthly/Seasonal Patterns Analysis")
    log("=" * 80)
    insight7_start = time.time()
    
    # TAIL LATENCY OPTIMIZATION: Column pruning - only select needed columns
    # Partition pruning will help if data is partitioned by month
    monthly_patterns = df_enriched.select("pickup_month", "total_amount", "trip_distance", "speed_mph") \
      .groupBy("pickup_month") \
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
    
    log("\n" + format_dataframe(monthly_display, plotter='monthly_patterns'))
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
    
    log("\n" + format_dataframe(airport_trips, plotter='airport_trips'))
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
    
    log("\n" + format_dataframe(passenger_patterns, plotter='passenger_patterns'))
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
    
    log("\n" + format_dataframe(fare_efficiency, plotter='fare_efficiency'))
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
    
    log("\n" + format_dataframe(weekend_comparison, plotter='weekend_comparison'))
    insight11_end = time.time()
    log(f"  Query Time: {insight11_end - insight11_start:.2f} seconds")
    
    # --- INSIGHT 12: Peak Hours Analysis (Top 5 Busiest Hours) ---
    log("\n" + "=" * 80)
    log("[12] Peak Hours Analysis (Top 10 Busiest Hours)")
    log("=" * 80)
    insight12_start = time.time()
    
    # TAIL LATENCY OPTIMIZATION: Column pruning - only select needed columns
    # This reduces data scanned during aggregation
    peak_hours = df_enriched.select("pickup_hour", "total_amount", "speed_mph") \
      .groupBy("pickup_hour") \
      .agg(
          count("*").alias("trip_count"),
          round(avg("total_amount"), 2).alias("avg_fare"),
          round(avg("speed_mph"), 2).alias("avg_speed_mph"),
          round(sum("total_amount"), 2).alias("total_revenue")
      ) \
      .orderBy(desc("trip_count")) \
      .limit(10)
    
    log("\n" + format_dataframe(peak_hours, plotter='peak_hours'))
    insight12_end = time.time()
    log(f"  Query Time: {insight12_end - insight12_start:.2f} seconds")
    log("  NOTE: Identifies busiest hours for demand planning")

    end_time = time.time()
    total_time = end_time - start_time

        # --- TAIL LATENCY ANALYSIS ---
    log("\n" + "=" * 80)
    log("TAIL LATENCY ANALYSIS")
    log("=" * 80)
    
    # Collect all query times
    query_times = [
        ("Insight 1: Borough Analysis", insight1_end - insight1_start),
        ("Insight 2: Congestion Analysis", insight2_end - insight2_start),
        ("Insight 3: Yearly Trends", insight3_end - insight3_start),
        ("Insight 4: Trip Categories", insight4_end - insight4_start),
        ("Insight 5: Day of Week", insight5_end - insight5_start),
        ("Insight 6: Top Routes", insight6_end - insight6_start),
        ("Insight 7: Monthly Patterns", insight7_end - insight7_start),
        ("Insight 8: Airport Trips", insight8_end - insight8_start),
        ("Insight 9: Passenger Patterns", insight9_end - insight9_start),
        ("Insight 10: Fare Efficiency", insight10_end - insight10_start),
        ("Insight 11: Weekend vs Weekday", insight11_end - insight11_start),
        ("Insight 12: Peak Hours", insight12_end - insight12_start),
    ]
    
    # Calculate percentiles
    import statistics
    import builtins
    times_only = [t[1] for t in query_times]
    times_sorted = sorted(times_only)
    n = len(times_sorted)
    
    p50 = times_sorted[int(n * 0.50)] if n > 0 else 0
    p95 = times_sorted[int(n * 0.95)] if n > 0 else 0
    p99 = times_sorted[int(n * 0.99)] if n > 0 else 0
    
    avg_time = statistics.mean(times_only) if times_only else 0
    min_time = builtins.min(times_only) if times_only else 0
    max_time = builtins.max(times_only) if times_only else 0
    
    log(f"\nQuery Execution Time Statistics:")
    log(f"  Total Queries: {len(query_times)}")
    log(f"  Min Query Time: {min_time:.2f} seconds")
    log(f"  Max Query Time: {max_time:.2f} seconds")
    log(f"  Average Query Time: {avg_time:.2f} seconds")
    log(f"  Median (p50): {p50:.2f} seconds")
    log(f"  p95 Latency: {p95:.2f} seconds")
    log(f"  p99 Latency: {p99:.2f} seconds")
    
    # Identify slowest queries
    log(f"\nSlowest Queries (Tail Latency):")
    slowest = sorted(query_times, key=lambda x: x[1], reverse=True)[:3]
    for i, (name, time_val) in enumerate(slowest, 1):
        log(f"  {i}. {name}: {time_val:.2f} seconds")
    
    # Calculate tail latency ratio (p99/p50)
    tail_ratio = p99 / p50 if p50 > 0 else 0
    log(f"\nTail Latency Ratio (p99/p50): {tail_ratio:.2f}x")
    if tail_ratio > 3.0:
        log("  ⚠️  WARNING: High tail latency - some queries are significantly slower")
    elif tail_ratio > 2.0:
        log("  ⚠️  CAUTION: Moderate tail latency - some optimization may be needed")
    else:
        log("  ✓ Good: Low tail latency - queries are consistent")
    
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