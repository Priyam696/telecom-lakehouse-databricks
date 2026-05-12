# Databricks notebook source
spark.conf.set("fs.azure.account.key.telcomlakehouse.dfs.core.windows.net",
                 "YOUR_STORAGE_KEY_HERE")

# COMMAND ----------

silver_path="abfss://silver@telcomlakehouse.dfs.core.windows.net/telecom_churn"
gold_path="abfss://gold@telcomlakehouse.dfs.core.windows.net/telecom_churn_gold"
audit_path="abfss://gold@telcomlakehouse.dfs.core.windows.net/audit_log"

df=spark.read.format("delta").load(silver_path)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Filter customers with high charges

# COMMAND ----------

from pyspark.sql.functions import col, sum as _sum, round as _round, count

total_rows = df.count()

null_counts = df.select([
    _sum(col(c).isNull().cast("int")).alias(c)
    for c in df.columns
]).collect()[0].asDict()
print(null_counts)

print("\n--- NULL REPORT ---")
print(f"{'Column':<30} {'Null Count':>12} {'Null %':>10}")
print("-" * 55)
for col_name, null_count in null_counts.items():
    pct = round((null_count / total_rows) * 100, 2)
    flag = " *** HIGH ***" if pct > 5 else ""
    print(f"{col_name:<30} {null_count:>12} {pct:>9}%{flag}")

# COMMAND ----------

total     = df.count()
distinct  = df.dropDuplicates(["customerID"]).count()
dupes     = total - distinct

print("\n--- DUPLICATE REPORT ---")
print(f"Total records    : {total}")
print(f"Distinct records : {distinct}")
print(f"Duplicates found : {dupes}")

if dupes > 0:
    print("*** ACTION REQUIRED: Duplicates detected on customerID ***")
else:
    print("PASS: No duplicates found")


# COMMAND ----------

from pyspark.sql.functions import min as _min, max as _max, avg

print("\n--- VALUE RANGE REPORT ---")

# MonthlyCharges must be > 0
invalid_monthly = df.filter(col("MonthlyCharges") <= 0).count()
print(f"MonthlyCharges <= 0    : {invalid_monthly} records {'*** FAIL ***' if invalid_monthly > 0 else 'PASS'}")

# TotalCharges must be >= 0
invalid_total = df.filter(col("TotalCharges") < 0).count()
print(f"TotalCharges < 0       : {invalid_total} records {'*** FAIL ***' if invalid_total > 0 else 'PASS'}")

# Tenure must be between 0 and 72
invalid_tenure = df.filter((col("tenure") < 0) | (col("tenure") > 72)).count()
print(f"Tenure out of 0-72     : {invalid_tenure} records {'*** FAIL ***' if invalid_tenure > 0 else 'PASS'}")


# COMMAND ----------

invalid_churn = df.filter(~col("Churn").isin(["Yes", "No"])).count()
print(f"Churn not Yes/No       : {invalid_churn} records {'*** FAIL ***' if invalid_churn > 0 else 'PASS'}")

# COMMAND ----------

df.select(
    _min("MonthlyCharges").alias("Min_Monthly"),
    _max("MonthlyCharges").alias("Max_Monthly"),
    avg("MonthlyCharges").alias("Avg_Monthly"),
    _min("TotalCharges").alias("Min_Total"),
    _max("TotalCharges").alias("Max_Total")
).show()


# COMMAND ----------

from pyspark.sql.functions import countDistinct

print("\n--- CATEGORICAL VALIDATION ---")

# Contract must be one of 3 valid values
valid_contracts = ["Month-to-month", "One year", "Two year"]
invalid_contract = df.filter(~col("Contract").isin(valid_contracts)).count()
print(f"Invalid Contract values : {invalid_contract} {'*** FAIL ***' if invalid_contract > 0 else 'PASS'}")


# COMMAND ----------

print("\nUnique values check:")
for col_name in ["Contract", "PaymentMethod", "InternetService", "gender"]:
    unique_vals = [r[col_name] for r in df.select(col_name).distinct().collect()]
    print(f"  {col_name}: {unique_vals}")


# COMMAND ----------

print("\n--- DATA QUALITY SCORE ---")
checks = {
    "No nulls in customerID"    : df.filter(col("customerID").isNull()).count() == 0,
    "No duplicate customerIDs"  : dupes == 0,
    "MonthlyCharges > 0"        : invalid_monthly == 0,
    "TotalCharges >= 0"         : invalid_total == 0,
    "Tenure in valid range"     : invalid_tenure == 0,
    "Churn values valid"        : invalid_churn == 0,
    "Contract values valid"     : invalid_contract == 0,
}

passed = sum(checks.values())
total_checks = len(checks)
score = round((passed / total_checks) * 100, 1)

print(f"\n{'Check':<35} {'Status':>10}")
print("-" * 48)
for check, result in checks.items():
    status = "PASS" if result else "FAIL"
    print(f"{check:<35} {status:>10}")

print(f"\nOverall Quality Score: {score}% ({passed}/{total_checks} checks passed)")

if score == 100:
    print("RESULT: Silver layer is production ready")
elif score >= 80:
    print("RESULT: Acceptable — minor issues to review")
else:
    print("RESULT: *** FAIL — pipeline should not proceed to Gold ***")


# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

print("\n--- WRITING AUDIT LOG ---")

audit_data = []
for check_name, result in checks.items():
    audit_data.append((
        check_name,
        "PASS" if result else "FAIL",
        score,
        total_rows
    ))

audit_schema = StructType([
    StructField("check_name",    StringType(),  True),
    StructField("status",        StringType(),  True),
    StructField("quality_score", FloatType(),   True),
    StructField("total_records", IntegerType(), True)
])

audit_df = spark.createDataFrame(audit_data, audit_schema) \
    .withColumn("run_timestamp", current_timestamp()) \
    .withColumn("layer",         lit("Silver")) \
    .withColumn("dataset",       lit("telecom_churn"))

audit_df.write.format("delta") \
    .mode("append") \
    .save(audit_path)

print(f"Audit log written to: {audit_path}")
audit_df.show(truncate=False)


# COMMAND ----------

# See all audit runs over time
spark.read.format("delta").load(audit_path) \
    .orderBy("run_timestamp", ascending=False) \
    .show(20, truncate=False)


# COMMAND ----------

spark.read.format("delta").load(audit_path) \
    .groupBy("run_timestamp", "layer") \
    .agg({"quality_score": "avg"}) \
    .orderBy("run_timestamp", ascending=False) \
    .show()


# COMMAND ----------

QUALITY_THRESHOLD = 85.0

if score < QUALITY_THRESHOLD:
    raise Exception(
        f"Data quality score {score}% is below threshold {QUALITY_THRESHOLD}%. "
        f"Pipeline halted. Gold layer NOT updated. Review audit log."
    )
else:
    print(f"Quality gate PASSED ({score}%). Proceeding to Gold layer.")
