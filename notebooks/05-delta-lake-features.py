# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.telcomlakehouse.dfs.core.windows.net",
  "YOUR_STORAGE_KEY_HERE"
)
silver_path = "abfss://silver@telcomlakehouse.dfs.core.windows.net/telecom_churn"
gold_path   = "abfss://gold@telcomlakehouse.dfs.core.windows.net/final_dashboard"


# COMMAND ----------

spark.sql(f"""
    DESCRIBE HISTORY delta.`{silver_path}`
""").select("version", "timestamp", "operation", "operationMetrics").display()


# COMMAND ----------

df_v0 = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load(silver_path)

# Read current Silver
df_current = spark.read.format("delta").load(silver_path)

print(f"Version 0 row count  : {df_v0.count()}")
print(f"Current row count    : {df_current.count()}")
print(f"Version 0 columns    : {len(df_v0.columns)}")
print(f"Current columns      : {len(df_current.columns)}")


# COMMAND ----------

from delta.tables import DeltaTable

updates_data = [
    ("0002-ORFOZ", "No",  55.20, "Month-to-month"),
    ("0003-MKNFE", "Yes", 89.75, "Two year"),
    ("9999-NEWCU", "No",  45.00, "One year")
]

updates_df = spark.createDataFrame(
    updates_data,
    ["customerID", "Churn", "TotalCharges", "Contract"]
)

silver_delta = DeltaTable.forPath(spark, silver_path)

silver_delta.alias("target").merge(
    updates_df.alias("source"),
    "target.customerID = source.customerID"
).whenMatchedUpdate(set={
    "Churn":        "source.Churn",
    "TotalCharges": "source.TotalCharges",
    "Contract":     "source.Contract"
}).whenNotMatchedInsert(values={          # ← changed from InsertAll
    "customerID":   "source.customerID",
    "Churn":        "source.Churn",
    "TotalCharges": "source.TotalCharges",
    "Contract":     "source.Contract"
}).execute()

print("✅ MERGE complete")

# COMMAND ----------

spark.sql(f"DESCRIBE HISTORY delta.`{silver_path}`") \
    .select("version", "operation", "operationMetrics") \
    .display()


# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp

# Read current silver
df_silver = spark.read.format("delta").load(silver_path)

# Add 2 new columns — simulating upstream schema change
df_evolved = df_silver \
    .withColumn("DataSource", lit("Kaggle_Telco_v1")) \
    .withColumn("LoadTimestamp", current_timestamp())

# Write back with mergeSchema — pipeline doesn't break
df_evolved.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save(silver_path)

print("✅ Schema evolved successfully")


# COMMAND ----------

spark.read.format("delta").load(silver_path).display()

# COMMAND ----------

history_df = spark.sql(f"DESCRIBE HISTORY delta.`{silver_path}`")
history_df.select("version", "timestamp", "operation").show(truncate=False)

# Restore to Version 0 — before MERGE and schema evolution
spark.sql(f"""
    RESTORE TABLE delta.`{silver_path}` TO VERSION AS OF 0
""")

print("✅ Table restored to Version 0")


# COMMAND ----------

df_restored = spark.read.format("delta").load(silver_path)
print(f"Row count after restore: {df_restored.count()}")
print(f"Columns after restore  : {df_restored.columns}")


# COMMAND ----------

spark.sql(f"OPTIMIZE delta.`{silver_path}`")

# ZOrder on most-filtered columns
spark.sql(f"""
    OPTIMIZE delta.`{silver_path}`
    ZORDER BY (Contract, Churn)
""")
print("✅ OPTIMIZE + ZORDER complete")