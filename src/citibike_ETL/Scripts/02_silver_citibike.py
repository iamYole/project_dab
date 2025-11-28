# import os
import sys
from utils.citibike_utils import get_trip_duration_mins
from utils.datetime_utils import timestamp_to_date_col
from pyspark.sql.functions import create_map, lit

# current_dir = os.getcwd()
# project_root = os.path.abspath(os.path.join(current_dir,"..","..",".."))
# sys.path.append(project_root)

pipeline_id = sys.argv[1]
run_id = sys.argv[2]
task_id = sys.argv[3]
processed_timestamp = sys.argv[4]
catalog = sys.argv[5]

df_silver = spark.read.table(f"{catalog}.01_bronze.jc_citibike")

df_silver = get_trip_duration_mins(spark, df_silver, "started_at","ended_at","trip_duration_mins")

df_silver = timestamp_to_date_col(spark, df_silver, "started_at", "trip_start_date")

df_silver = df_silver.withColumn("metadata",
                    create_map(
                        lit("pipeline_id"), lit(pipeline_id),
                        lit("run_id"), lit(run_id),
                        lit("task_id"), lit(task_id),
                        lit("processed_timestamp"), lit(processed_timestamp)
                    ))

df_silver = df_silver.select(
    "ride_id",
    "trip_start_date",
    "started_at",
    "ended_at",
    "start_station_name",
    "end_station_name",
    "trip_duration_mins",
    "metadata"
)

(df_silver
    .write
    .mode("overwrite")
    .option("overwriteSchema","true")
    .saveAsTable(f"{catalog}.02_silver.jc_citibike")
    
)