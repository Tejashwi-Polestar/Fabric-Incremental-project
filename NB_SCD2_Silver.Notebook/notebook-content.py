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

# This cell is tagged as a "Parameters" cell in Fabric
# When the pipeline runs this notebook, it injects the file_name value here
# When you run manually, this default value is used as fallback
file_name = "sales_2024_01_01.csv"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# All config in one place at the top
# Good practice — if paths change, you only update here, not across every cell

staging_path = f"Files/staging/{file_name}"  # where Copy activity dropped the file
scd2_table   = "sales_silver_scd2"           # target Delta table we are writing to

print(f"Processing file  : {staging_path}")
print(f"Target SCD2 table: {scd2_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# StructType = the full schema of your CSV file
# StructField = one column definition: (column_name, data_type, nullable)
# We import these from pyspark.sql.types

from pyspark.sql.types import StructType, StructField, StringType

# Step 1: Read ALL columns as StringType (plain text)
# Why StringType for everything?
#   - Nothing can fail at read time — every value is just text
#   - We get the raw data exactly as it appears in the CSV
#   - We then cast to proper types ourselves in the next cell
#   - This is the production pattern — you never let Spark guess types

raw_schema = StructType([
    StructField("txn_id",       StringType(), True),  # True = this column can be null
    StructField("store_id",     StringType(), True),
    StructField("customer_id",  StringType(), True),
    StructField("txn_date",     StringType(), True),  # looks like a date but read as string first
    StructField("product_id",   StringType(), True),
    StructField("quantity",     StringType(), True),  # looks like int but read as string first
    StructField("unit_price",   StringType(), True),
    StructField("total",        StringType(), True),
    StructField("last_updated", StringType(), True)
])

print("Raw schema defined — all columns as StringType")
print(f"Total columns defined: {len(raw_schema.fields)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# spark.read.csv() reads the CSV file from the staging folder
# header=True         → first row of CSV is column names, not data
# schema=raw_schema   → use our manually defined schema, NOT inferSchema
# inferSchema=False   → explicitly tell Spark DO NOT guess types
# nullValue="NULL"    → if a cell contains the text "NULL", treat it as actual null

from pyspark.sql import functions as F

df_raw = spark.read\
.option("header",  "true")\
.option("inferschema", "false")\
.option("nullvaue", "NULL")\
.schema(raw_schema)\
.csv(staging_path)

# Why show printSchema here?
# So you can visually confirm every column is StringType before casting
# Helps catch issues early — if a column is missing, you see it here
print("Schema after reading (all strings):")
df_raw.printSchema()

print(f"Row count from file: {df_raw.count()}")
df_raw.show(truncate=False)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Step 2: Now cast each column from String to its actual correct type
# # .cast() converts the column to the type you specify
# # Why do this separately from reading?
# #   - You can see exactly where a cast fails if data is bad
# #   - You can add logic like "if cast fails, keep null" rather than crashing
# # 
# # Common types:
# #   IntegerType()  → whole numbers (1, 2, 100)
# #   DateType()     → dates (2024-01-01) — format must match
# #   StringType()   → text (keep as string)
# #   LongType()     → large numbers (use for totals/amounts to avoid overflow)

# from pyspark.sql.types import IntegerType, DateType, LongType

# df_typed = df_raw \
#     .withColumn("txn_date",    
#         F.to_date(F.col("txn_date"), "yyyy-MM-dd")) \
#         # to_date() converts string "2024-01-01" to actual DateType
#         # "yyyy-MM-dd" is the format pattern — must match your CSV date format
#         # If format doesn't match, column becomes null (no crash)
#     \
#     .withColumn("last_updated", 
#         F.to_date(F.col("last_updated"), "yyyy-MM-dd")) \
#         # Same pattern for last_updated column
#     \
#     .withColumn("quantity",    
#         F.col("quantity").cast(IntegerType())) \
#         # .cast() converts the string "10" to integer 10
#         # IntegerType = whole number, no decimals
#     \
#     .withColumn("unit_price",  
#         F.col("unit_price").cast(LongType())) \
#         # LongType for price — larger range than IntegerType
#         # Avoids overflow if price is a very large number
#     \
#     .withColumn("total",       
#         F.col("total").cast(LongType()))
#         # Same for total — calculated field, could be large

# # Confirm the schema now shows correct types, not StringType
# print("Schema after casting to proper types:")
# df_typed.printSchema()

# # Show the data to visually confirm dates look like dates, numbers like numbers
# df_typed.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

from pyspark.sql.types import IntegerType, LongType

# to_date() converts string "2024-01-01" to actual DateType
# "yyyy-MM-dd" is the format pattern — must match your CSV date format
# If format does not match, column becomes null instead of crashing
df_typed = df_raw \
    .withColumn("txn_date",     F.to_date(F.col("txn_date"),     "yyyy-MM-dd")) \
    .withColumn("last_updated", F.to_date(F.col("last_updated"), "yyyy-MM-dd")) \
    .withColumn("quantity",     F.col("quantity").cast(IntegerType())) \
    .withColumn("unit_price",   F.col("unit_price").cast(IntegerType())) \
    .withColumn("total",        F.col("total").cast(IntegerType()))

# printSchema confirms every column is now its correct type, not StringType
df_typed.printSchema()

# show confirms actual data looks correct after casting
df_typed.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Before writing anything, check for basic data issues
# In production you never write bad data to your Delta table
# Better to catch it here and fail loudly than silently write nulls

# Check 1: txn_id must never be null — it is our unique key
# If txn_id is null we cannot do SCD2 matching — row is useless
null_txn_ids = df_typed.filter(F.col("txn_id").isNull()).count()

# Check 2: txn_date must not be null — it is a core business column
null_dates = df_typed.filter(F.col("txn_date").isNull()).count()

# Check 3: total must be positive — negative totals are likely bad data
negative_totals = df_typed.filter(F.col("total") < 0).count()

print(f"Null txn_id count    : {null_txn_ids}")
print(f"Null txn_date count  : {null_dates}")
print(f"Negative total count : {negative_totals}")

# If any check fails, raise an error — stop the pipeline here
# This is called a "hard stop" — better than writing dirty data
if null_txn_ids > 0:
    raise ValueError(f"QUALITY FAIL: {null_txn_ids} rows have null txn_id — cannot proceed")

if null_dates > 0:
    raise ValueError(f"QUALITY FAIL: {null_dates} rows have null txn_date — cannot proceed")

print("All quality checks passed — safe to proceed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Check if the SCD2 table already has data
# # If count = 0, this is the very first time we are loading — no MERGE needed
# # We just insert everything as current version

# row_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {scd2_table}") \
#                  .collect()[0]["cnt"]
# # .collect() brings the result from Spark back to Python as a list
# # [0] gets the first (only) row
# # ["cnt"] gets the value of the cnt column

# print(f"Existing rows in SCD2 table: {row_count}")

# if row_count == 0:
#     print("First load — inserting all rows as current active versions")
    
#     df_first = df_typed \
#         .withColumn("_source_file", F.lit(file_name)) \
#         # F.lit() creates a column with the same literal value for every row
#         # Here every row gets the file name it came from
#         \
#         .withColumn("_batch_date",  F.current_date()) \
#         # current_date() = today's date — same for all rows in this run
#         \
#         .withColumn("valid_from",   F.current_date()) \
#         # valid_from = when this version became active = today
#         \
#         .withColumn("valid_to",     F.lit(None).cast("date")) \
#         # valid_to = NULL means this version is still active (not closed yet)
#         # F.lit(None) creates a null value, .cast("date") sets its type
#         \
#         .withColumn("is_current",   F.lit(True))
#         # True = this is the active/current version of this row

#     df_first.write \
#         .format("delta") \
#         # format("delta") = write as Delta table, not plain parquet
#         # Delta gives us versioning, time travel, MERGE support
#         \
#         .mode("append") \
#         # append = add rows to existing table, never delete existing rows
#         \
#         .saveAsTable(scd2_table)
#         # saveAsTable = register it in the Lakehouse metastore by name

#     print(f"First load complete — inserted {df_first.count()} rows")
#     df_first.show(truncate=False)
    
#     dbutils.notebook.exit("First load complete — exiting early")
#     # exit early — cells below are for incremental loads only
#     # dbutils.notebook.exit() stops notebook execution cleanly
#     # The pipeline sees this as a success, not a failure

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# Check if SCD2 table already has data
# count = 0 means first ever load — skip MERGE, just insert everything
row_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {scd2_table}").collect()[0]["cnt"]

# .collect() brings Spark result to Python as a list
# [0] gets the first row, ["cnt"] gets the count value
print(f"Existing rows in SCD2 table: {row_count}")

if row_count == 0:
    print("First load — inserting all rows as current active versions")

    # F.lit(file_name) adds the same file name value to every row
    # F.current_date() adds today's date to every row
    # valid_to = None means this version is still active, not closed yet
    # is_current = True marks this as the latest version of this row
    df_first = df_typed \
        .withColumn("_source_file", F.lit(file_name)) \
        .withColumn("_batch_date",  F.current_date()) \
        .withColumn("valid_from",   F.current_date()) \
        .withColumn("valid_to",     F.lit(None).cast("date")) \
        .withColumn("is_current",   F.lit(True))

    # format("delta") writes as Delta table — gives versioning and MERGE support
    # mode("append") adds rows without touching existing data
    # saveAsTable registers it in Lakehouse by name
    df_first.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(scd2_table)

    print(f"First load complete — inserted {df_first.count()} rows")
    df_first.show(truncate=False)

    # Stop notebook here cleanly — cells below are for incremental loads only
    # Pipeline treats this as success not failure
    mssparkutils.notebook.exit("First load complete")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *
df_types = df_raw \
.withColumn("txn_date",
F.todate9f.col("txn_date"),"yyyy-MM-DD"))\
\
df_types = df_raw.withcolumn("txn_date",f.to_date(f.col("txn_date"),"yyyy-mm-dd")).withcolumn("quantity", f.col("quantity"),cast(IntegerType())).withcolumn("unit_price",f.col("unit_price").cast(LongType()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# This cell only runs on 2nd load onwards (first load exits above)
# We compare incoming data against what is currently active in SCD2
# to find: what changed and what is brand new

from delta.tables import DeltaTable

# Load only the CURRENT active rows from SCD2 table
# is_current = True means these are the latest versions
# We only compare against current versions — historical rows are irrelevant
# df_current = spark.sql(f"""
#     SELECT txn_id, quantity, unit_price, total, last_updated
#     FROM {scd2_table}
#     WHERE is_current = True
# """)

df_current = spark.sql(f"""
select txn_id,quantity,unit_price,total, last_updated
from {scd2_table}
where is_current = true""")

print(f"Current active rows in SCD2: {df_current.count()}")

# Left join incoming data to current SCD2 rows on txn_id
# left join = keep ALL incoming rows, match current rows where txn_id exists
# After join:
#   - if cur.txn_id is NOT null → this txn exists in SCD2 already
#   - if cur.txn_id IS null     → this txn is brand new, never seen before
# df_joined = df_typed.alias("inc").join(
#     df_current.alias("cur"),
#     on="txn_id",
#     how="left"
# )

df_joined = df_typed.alias("inc").join(
    df_current.alias("cur"),
    on="txn_id",
    how="left"

)

# df_joined = df_typed.alias("inc").join(df_current.alias("cur") on="txn_id", how= "left")

# CHANGED rows: txn_id already exists in SCD2 AND at least one value is different
# We check all columns that could change in a sales transaction
# | means OR — if ANY of these changed, we treat the whole row as changed
df_changed = df_joined.filter(
    F.col("cur.txn_id").isNotNull() &   # exists in SCD2
    (
        (F.col("inc.quantity")     != F.col("cur.quantity"))     |
        (F.col("inc.unit_price")   != F.col("cur.unit_price"))   |
        (F.col("inc.total")        != F.col("cur.total"))        |
        (F.col("inc.last_updated") != F.col("cur.last_updated"))
    )
).select("inc.*")  # keep only the incoming columns, drop the cur.* comparison columns

# NEW rows: txn_id does not exist in SCD2 at all
# cur.txn_id is null because left join found no match
df_new = df_joined.filter(
    F.col("cur.txn_id").isNull()
).select("inc.*")

print(f"Changed rows : {df_changed.count()}")
print(f"New rows     : {df_new.count()}")
df_changed.show(truncate=False)
df_new.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# SCD2 rule: when a row changes, you do NOT delete or overwrite the old version
# Instead you "close" it by setting:
#   is_current = False  → this is no longer the active version
#   valid_to = today    → this version was valid until today

if df_changed.count() > 0:
    
    # Collect the list of txn_ids that have changes
    # We need this list to tell the UPDATE which rows to close
    changed_ids = [row.txn_id for row in df_changed.select("txn_id").collect()]
    # This is a Python list comprehension
    # For each row in df_changed, extract the txn_id value
    # Result: ["TXN002", "TXN004"] for example
    
    print(f"Closing old versions for txn_ids: {changed_ids}")
    
    # DeltaTable.forName() loads the existing Delta table as a DeltaTable object
    # We need this object to run UPDATE and MERGE operations
    scd2_target = DeltaTable.forName(spark, scd2_table)
    
    # .update() runs an UPDATE statement on the Delta table
    # condition = which rows to update (changed txn_ids that are currently active)
    # set = what to change those rows to
    scd2_target.update(
        condition = (
            F.col("txn_id").isin(changed_ids) &  
            # isin() = WHERE txn_id IN ('TXN002', 'TXN004')
            (F.col("is_current") == True)         
            # only close the CURRENT version, not already-closed historical rows
        ),
        set = {
            "is_current" : F.lit(False),        # mark as no longer active
            "valid_to"   : F.current_date()     # record when it was closed
        }
    )
    
    print("Old versions closed successfully")
    
    # Verify the update worked — these rows should now show is_current = False
    spark.sql(f"""
        SELECT txn_id, quantity, valid_from, valid_to, is_current 
        FROM {scd2_table} 
        WHERE txn_id IN ({str(changed_ids)[1:-1]})
        ORDER BY txn_id, valid_from
    """).show(truncate=False)

else:
    print("No changed rows — skipping close step")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Now insert two types of rows:
# # 1. New versions of changed rows (replacing the ones we just closed)
# # 2. Brand new rows that never existed in SCD2

# # union() combines two DataFrames with the same columns into one
# # Like SQL UNION ALL — keeps all rows from both
# df_to_insert = df_changed.union(df_new)

# if df_to_insert.count() > 0:

#     df_insert_final = df_to_insert \
#         .withColumn("_source_file", F.lit(file_name)) \
#         .withColumn("_batch_date",  F.current_date()) \
#         .withColumn("valid_from",   F.current_date()) \
#         # valid_from = today = when this new version became active
#         .withColumn("valid_to",     F.lit(None).cast("date")) \
#         # valid_to = NULL = still active, not closed yet
#         .withColumn("is_current",   F.lit(True))
#         # is_current = True = this is the active version

#     df_insert_final.write \
#         .format("delta") \
#         .mode("append") \
#         # append = only add new rows, never touch existing rows
#         # This is safe — we already closed the old versions above
#         .saveAsTable(scd2_table)

#     print(f"Inserted {df_insert_final.count()} rows into SCD2 table")
#     df_insert_final.show(truncate=False)

# else:
#     print("No rows to insert — data unchanged from last load")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# union() combines changed rows and new rows into one DataFrame
# Like SQL UNION ALL — keeps all rows from both sets
df_to_insert = df_changed.union(df_new)

if df_to_insert.count() > 0:

    # valid_from = today = when this new version became active
    # valid_to = None = still active, not closed yet
    # is_current = True = this is the current active version
    df_insert_final = df_to_insert \
        .withColumn("_source_file", F.lit(file_name)) \
        .withColumn("_batch_date",  F.current_date()) \
        .withColumn("valid_from",   F.current_date()) \
        .withColumn("valid_to",     F.lit(None).cast("date")) \
        .withColumn("is_current",   F.lit(True))

    # append = only add new rows, never touch existing rows
    # safe to append here because old versions were already closed in Cell 9
    df_insert_final.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(scd2_table)

    print(f"Inserted {df_insert_final.count()} rows into SCD2 table")
    df_insert_final.show(truncate=False)

else:
    print("No rows to insert — data unchanged from last load")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Always verify after every load — confirms everything worked as expected
# These are the queries you would also run in an interview to prove SCD2 works

print("=== ALL VERSIONS (full history) ===")
spark.sql(f"""
    SELECT txn_id, quantity, last_updated, 
           valid_from, valid_to, is_current, _source_file
    FROM {scd2_table}
    ORDER BY txn_id, valid_from
""").show(truncate=False)

print("=== CURRENT ACTIVE ROWS ONLY ===")
spark.sql(f"""
    SELECT txn_id, quantity, last_updated, valid_from, is_current
    FROM {scd2_table}
    WHERE is_current = True
    ORDER BY txn_id
""").show(truncate=False)

# Summary counts — quick sanity check
total   = spark.sql(f"SELECT COUNT(*) AS cnt FROM {scd2_table}").collect()[0]["cnt"]
current = spark.sql(f"SELECT COUNT(*) AS cnt FROM {scd2_table} WHERE is_current = True").collect()[0]["cnt"]
history = total - current

print(f"Total rows (all versions) : {total}")
print(f"Current active rows       : {current}")
print(f"Historical closed rows    : {history}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
