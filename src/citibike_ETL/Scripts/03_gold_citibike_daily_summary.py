from pyspark.sql.functions import min, max, avg, count, round
import sys

catalog = sys.argv[5]

df = spark.read.table(f"{catalog}.02_silver.jc_citibike")

df = df.groupBy("trip_start_date").agg(
            round(max("trip_duration_mins"), 2).alias("max_trip_duration_mins"),
            round(min("trip_duration_mins"), 2).alias("min_trip_duration_mins"),
            round(avg("trip_duration_mins"), 2).alias("avg_trip_duration_mins"),
            count("ride_id").alias("total_trips")
            ).orderBy("trip_start_date")

(df.write
    .mode("overwrite")
    .option("overwriteSchema","true")
    .saveAsTable(f"{catalog}.03_gold.daily_ride_summary")
    )