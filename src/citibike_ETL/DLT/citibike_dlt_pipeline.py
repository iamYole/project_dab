import dlt 
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
from pyspark.sql.functions import create_map, lit,unix_timestamp,to_date,col,round,count,avg,max,min

catalog = spark.conf.get("catalog")

schema = StructType([
    StructField("ride_id", StringType(), True),
    StructField("rideable_type", StringType(), True),
    StructField("started_at", TimestampType(), True),
    StructField("ended_at", TimestampType(), True),
    StructField("start_station_name", StringType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_station_name", StringType(), True),
    StructField("end_station_id", StringType(), True),
    StructField("start_lat", DecimalType(), True),
    StructField("start_lng", DecimalType(), True),
    StructField("end_lat", DecimalType(), True),
    StructField("end_lng", DecimalType(), True),
    StructField("member_casual", StringType(), True)
])

# =================================================================================================================================== ##
# Bronze Layer
## =================================================================================================================================== ##

@dlt.table(
    name="bronze_jc_citibike",
    comment="Bronze Layer: raw Citibike data",
    table_properties={
        "quality": "bronze",
        "pipelines.table_type": "TABLE"
    }
)
def jc_citibike():
    return (spark
            .read
            .format("CSV")
            .schema(schema)
            .option("header","true")
            .load(f"/Volumes/{catalog}/00_landing/src_citibank_data")
        )

# =================================================================================================================================== ##
# Silver Layer
## =================================================================================================================================== ##
@dlt.table(
    name="silver_jc_citibike",
    comment="Silver Layer: cleaned Citibike data",
    table_properties={
        "quality":"silver",
        "pipelines.table_type": "TABLE"
    }
)
def jc_citibike():
    return(
        dlt.read("bronze_jc_citibike")
            .withColumn("trip_duration_mins",
                        (unix_timestamp(col("ended_at")) - unix_timestamp(col("started_at"))) / 60)
            .withColumn("trip_start_date", to_date(col("started_at")))
            .select(
               "ride_id",
                "trip_start_date",
                "started_at",
                "ended_at",
                "start_station_name",
                "end_station_name",
                "trip_duration_mins"
            )
    )

# =================================================================================================================================== ##
# Gold Layer
## =================================================================================================================================== ##

# Daily Ride Summary
@dlt.table(
    name="gold_daily_ride_summary",
    table_properties={
        "quality":"gold",
        "pipelines.table_type": "TABLE"
    }
)
def daily_ride_summary():
    return(
        dlt
            .read("silver_jc_citibike")
            .groupBy("trip_start_date")
            .agg(
                round(max("trip_duration_mins"), 2).alias("max_trip_duration_mins"),
                round(min("trip_duration_mins"), 2).alias("min_trip_duration_mins"),
                round(avg("trip_duration_mins"), 2).alias("avg_trip_duration_mins"),
                count("ride_id").alias("total_trips"))
            .orderBy("trip_start_date")
    )

# Daily Station Performance
@dlt.table(
    name="gold_daily_station_performance",
    table_properties={
        "quality":"gold",
        "pipelines.table_type": "TABLE"
    }
)
def daily_station_performance():
    return(
        dlt
        .read("silver_jc_citibike")
        .groupBy("trip_start_date","start_station_name")
        .agg(
            round(avg("trip_duration_mins"), 2).alias("avg_trip_duration_mins"),
            count("ride_id").alias("total_trips"))
        .orderBy("trip_start_date","start_station_name")
    )
