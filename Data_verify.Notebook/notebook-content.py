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

spark.sql("SELECT * FROM sales_silver ORDER BY txn_id").show(truncate=False)
spark.sql("SELECT COUNT(*) AS total FROM sales_silver").show()# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
