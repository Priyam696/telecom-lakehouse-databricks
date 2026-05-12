# Databricks notebook source
spark.conf.set("fs.azure.account.key.telcomlakehouse.dfs.core.windows.net",
                 "YOUR_STORAGE_KEY_HERE")

# COMMAND ----------

spark.conf.set("fs.azure.account.key.telecomlakehouse.dfs.core.windows.net", "YOUR_STORAGE_KEY_HERE")
df = spark.read.csv(
    "abfss://bronze@telcomlakehouse.dfs.core.windows.net/telecom_churn", 
    header=True,
    inferSchema=True
)
df.display()


# COMMAND ----------

df.printSchema()
df.count()


# COMMAND ----------

from pyspark.sql.functions import col, when

# Remove duplicates
df_clean = df.dropDuplicates()

# Handle nulls (example)
df_clean = df_clean.fillna({
    "TotalCharges": 0
})

# Convert data types (important)
df_clean = df_clean.withColumn(
    "TotalCharges",
    col("TotalCharges").cast("double")
)


# COMMAND ----------

df_clean = df_clean.withColumn(
    "SeniorCitizen",
    when(col("SeniorCitizen") == 1, "Yes").otherwise("No")
)


# COMMAND ----------

# DBTITLE 1,Write silver Delta output
from pyspark.sql.functions import col, when, expr

df_clean = (
    df.dropDuplicates()
      .withColumn(
          "TotalCharges",
          expr("coalesce(try_cast(nullif(trim(TotalCharges), '') as double), 0D)")
      )
      .withColumn(
          "SeniorCitizen",
          when(col("SeniorCitizen") == 1, "Yes").otherwise("No")
      )
)

df_clean.write.format("delta").mode("overwrite").save(
    "abfss://silver@telcomlakehouse.dfs.core.windows.net/telecom_churn"
)
df_silver = spark.read.format("delta").load(
    "abfss://silver@telcomlakehouse.dfs.core.windows.net/telecom_churn"
)

df_silver.display()
