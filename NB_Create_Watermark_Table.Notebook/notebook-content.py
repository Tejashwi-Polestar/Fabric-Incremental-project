# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "175cb864-18f8-4e82-adad-cb6794a79985",
# META       "default_lakehouse_name": "LH_INCREMENTAL",
# META       "default_lakehouse_workspace_id": "ca7e8beb-e1ce-4d19-b434-d0a60aa1bbc4",
# META       "known_lakehouses": [
# META         {
# META           "id": "175cb864-18f8-4e82-adad-cb6794a79985"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Watermark table tracks every file that has been successfully processed
# Before processing a file, pipeline checks this table
# If file name exists here — skip it
# If file name does not exist — process it, then insert a record here

spark.sql("""
    CREATE TABLE IF NOT EXISTS pipeline_watermark (
        file_name        STRING,
        processed_date   TIMESTAMP,
        status           STRING
    )
    USING DELTA
""")

print("Watermark table created successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SELECT * FROM pipeline_watermark").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# We manually loaded sales_2024_01_01.csv earlier
# Insert a record here so the pipeline knows to skip it

from pyspark.sql import Row
from datetime import datetime

already_processed = spark.createDataFrame([
    Row(
        file_name="sales_2024_01_01.csv",
        processed_date=datetime.now(),
        status="completed"
    )
])

already_processed.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("pipeline_watermark")

print("Marked sales_2024_01_01.csv as already processed")
spark.sql("SELECT * FROM pipeline_watermark").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
