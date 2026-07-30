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

# Create the SCD2 table with three extra columns:
# valid_from  → date this version became active
# valid_to    → date this version was closed (NULL means still active)
# is_current  → True = this is the latest version of this row

spark.sql("""CREATE TABLE IF NOT EXISTs sales_silver_scd2 (
    txn_id     String,
    store_id    STRING,
    customer_id STRING,
    txn_date    date,
    product_id    string,
    quantity   int,
    unit_price  int,
    total  int,
    last_updated  date,
    _source_file  string,
    _batch_date   date,
    valid_from  date,
    valid_to  date,
    is_current  BOOLEAN
) 
USING DELTA""" )

print("SCD2 table created successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SELECT * FROM sales_silver_scd2").show()
print("table is ready")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
