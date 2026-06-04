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

# PARAMETERS CELL ********************

file_name = "sales_2024_01_01.csv"  # default value for manual runs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# file_name is injected by the pipeline via the parameters cell tag
# When run manually, it uses the default value set above

staging_path  = f"Files/staging/{file_name}"
staging_table = "sales_staging"
silver_table  = "sales_silver"

print(f"File to process : {staging_path}")
print(f"Staging table   : {staging_table}")
print(f"Silver table    : {silver_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fabric passes pipeline parameters via the 'parameters' cell tag
# We use mssparkutils to read the parameter passed from the pipeline

# file_name = mssparkutils.runtime.context.get("parameterized_notebook_inputs", {}).get("file_name", "sales_2024_01_01.csv")

# staging_path  = f"Files/staging/{file_name}"
# staging_table = "sales_staging"
# silver_table  = "sales_silver"

# print(f"File to process : {staging_path}")
# print(f"Staging table   : {staging_table}")
# print(f"Silver table    : {silver_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# file_name will be passed dynamically from the pipeline ForEach loop
# The fallback value lets you run this notebook manually for testing

# file_name = getArgument("file_name") if "file_name" in getArguments() \
#             else "sales_2024_01_01.csv"

# staging_path  = f"Files/staging/{file_name}"
# staging_table = "sales_staging"
# silver_table  = "sales_silver"

# print(f"File to process : {staging_path}")
# print(f"Staging table   : {staging_table}")
# print(f"Silver table    : {silver_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

from pyspark.sql import functions as F

# Read the CSV file that Copy activity dropped into Files/staging/
df = spark.read.csv(
    staging_path,
    header=True,
    inferSchema=True
)

print(f"Schema of incoming file:")
df.printSchema()
print(f"Row count: {df.count()}")
df.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add metadata columns so you know when and from which file each row came
df_staging = df \
    .withColumn("_source_file",    F.lit(file_name)) \
    .withColumn("_load_timestamp", F.current_timestamp()) \
    .withColumn("_batch_date",     F.current_date())

# Append into staging Delta table
# Even though we truncated before, we use append here
# because ForEach may loop multiple files in one run
df_staging.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(staging_table)

print(f"Written to staging table: {staging_table}")
df_staging.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable

# Check if silver table exists
# First ever run — create it directly from staging
# All subsequent runs — run MERGE (upsert)

if not spark.catalog.tableExists(silver_table):
    print(f"Silver table does not exist — creating from staging (first load)")
    
    df_staging.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(silver_table)
    
    print(f"Silver table created with {df_staging.count()} rows")

else:
    print(f"Silver table exists — running MERGE")
    
    # Load the existing silver Delta table as the target
    target = DeltaTable.forName(spark, silver_table)
    
    # Read current staging data as the source of changes
    source = spark.table(staging_table)
    
    # MERGE logic:
    # Match on txn_id (your unique key)
    # If match found AND any column has changed → update that row
    # If no match found → insert as new row
    target.alias("t").merge(
        source.alias("s"),
        "t.txn_id = s.txn_id"
    ).whenMatchedUpdate(
        condition="""
            t.quantity     != s.quantity  OR
            t.unit_price   != s.unit_price OR
            t.total        != s.total     OR
            t.last_updated != s.last_updated
        """,
        set={
            "store_id"        : "s.store_id",
            "customer_id"     : "s.customer_id",
            "txn_date"        : "s.txn_date",
            "product_id"      : "s.product_id",
            "quantity"        : "s.quantity",
            "unit_price"      : "s.unit_price",
            "total"           : "s.total",
            "last_updated"    : "s.last_updated",
            "_source_file"    : "s._source_file",
            "_load_timestamp" : "s._load_timestamp",
            "_batch_date"     : "s._batch_date"
        }
    ).whenNotMatchedInsertAll() \
     .execute()
    
    print("MERGE complete")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("=== Silver Table — Current State ===")
df_silver = spark.sql(f"SELECT * FROM {silver_table} ORDER BY txn_id")
df_silver.show(truncate=False)

total = spark.sql(f"SELECT COUNT(*) AS total FROM {silver_table}").collect()[0]["total"]
print(f"Total rows in silver table: {total}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
