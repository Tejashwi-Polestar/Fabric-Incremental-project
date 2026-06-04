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

# Check if staging table exists — if yes truncate it, if no create empty shell
# This ensures staging is clean before every pipeline run

from delta.tables import DeltaTable

staging_table = "sales_staging"

if spark.catalog.tableExists(staging_table):
    # Table exists — truncate it (delete all rows, keep structure)
    spark.sql(f"TRUNCATE TABLE {staging_table}")
    print(f"Truncated existing table: {staging_table}")
else:
    # First ever run — table doesn't exist yet, nothing to truncate
    print(f"Table {staging_table} does not exist yet — will be created by pipeline")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if spark.catalog.tableExists(staging_table):
    count = spark.sql(f"SELECT COUNT(*) AS total FROM {staging_table}").collect()[0]["total"]
    print(f"Rows in {staging_table} after truncate: {count}")
else:
    print("Staging table does not exist yet — skipping verification")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
