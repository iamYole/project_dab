from databricks.connect import DatabricksSession


spark = DatabricksSession.builder.getOrCreate()

spark.sql("SELECT 1").show()