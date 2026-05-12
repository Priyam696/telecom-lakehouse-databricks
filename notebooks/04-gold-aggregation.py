# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.telcomlakehouse.dfs.core.windows.net",
  "YOUR_STORAGE_KEY_HERE")

# COMMAND ----------

from pyspark.sql.functions import count, avg, sum as _sum, col, when

silver_path = "abfss://silver@telcomlakehouse.dfs.core.windows.net/telecom_churn"
gold_path   = "abfss://gold@telcomlakehouse.dfs.core.windows.net/telecom_churn_gold"

df_silver = spark.read.format("delta").load(silver_path)

# Check what columns actually exist
print("Columns in Silver:", df_silver.columns)

# Create Churn_Flag on the fly — don't depend on it being saved
df_silver = df_silver.withColumn(
    "Churn_Flag",
    when(col("Churn") == "Yes", 1).otherwise(0)
)

# Gold aggregation
df_gold = df_silver.groupBy("Contract", "InternetService") \
    .agg(
        count("customerID").alias("Total_Customers"),
        _sum("Churn_Flag").alias("Total_Churned"),
        avg("MonthlyCharges").alias("Avg_Monthly_Charges"),
        avg("TotalCharges").alias("Avg_Total_Charges")
    ) \
    .withColumn("Churn_Rate_Pct",
        (col("Total_Churned") / col("Total_Customers") * 100).cast("double"))

df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(gold_path)

print("✅ Gold layer written successfully")
df_gold.show()