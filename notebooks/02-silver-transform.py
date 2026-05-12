# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.telcomlakehouse.dfs.core.windows.net",
  "YOUR_STORAGE_KEY_HERE"
)


# COMMAND ----------

df=spark.read.csv("abfss://bronze@telcomlakehouse.dfs.core.windows.net/telecom_churn/",header=True,inferSchema=True)
df.display()

# COMMAND ----------

#df.printSchema()
df.count()

# COMMAND ----------

from pyspark.sql.functions import col, when, trim
df_clean=df.withColumn("TotalCharges",when(trim(col("TotalCharges"))=="",None).otherwise(col("TotalCharges")))
df_clean = df_clean.withColumn(
    "TotalCharges",
    col("TotalCharges").cast("double")
)
df_clean=df_clean.dropDuplicates()

df_clean=df_clean.fillna({
    'TotalCharges': 0
})
df_clean=df_clean.withColumn("SeniorCitizen",when(col("SeniorCitizen")==1,"Yes").otherwise("No"))
df_clean.display()

# COMMAND ----------

df_clean.write.format("delta").option("overwriteSchema", "true").partitionBy("Contract").mode("overwrite").save("abfss://silver@telcomlakehouse.dfs.core.windows.net/telecom_churn")

# COMMAND ----------

df_silver=spark.read.format("delta").load("abfss://silver@telcomlakehouse.dfs.core.windows.net/telecom_churn")
df_silver.display()

# COMMAND ----------

from pyspark.sql.functions import sum, col
df_clean.select([

    sum(col(c).isNull().cast("int")).alias(c) for c in df_clean.columns
]).display()


# COMMAND ----------

df__silver=spark.read.format("delta").load("abfss://silver@telcomlakehouse.dfs.core.windows.net/telecom_churn")
df__silver.display()

# COMMAND ----------

#Total Customer
total_customer=df_silver.count()
print("Total Customer: ",total_customer)

# COMMAND ----------

#Churn Rate
from pyspark.sql.functions import col
churn_rate=df_silver.filter(col("Churn")=='Yes').count()/total_customer
print("Chrn Rate: ",churn_rate)

# COMMAND ----------

#Revenue Total Charges
from pyspark.sql.functions import sum
revenue=df_silver.select(sum(col("TotalCharges"))).collect()[0][0]
print("Revenue: " ,revenue)

# COMMAND ----------

#Churn by Contract Type
from pyspark.sql.functions import count
df_contract=df_silver.groupBy("Contract").agg(count("*").alias("Customer_Count"))
df_contract.display()

# COMMAND ----------

#Revenue by Payment Method
from pyspark.sql.functions import lit
df_payment=df_silver.groupBy("PaymentMethod").sum("TotalCharges").withColumnRenamed("sum(TotalCharges)", "total_revenue")

df_payment.display()

# COMMAND ----------

#Churn Distribution
df_churn=df_silver.groupBy("Churn").count()
df_churn.display()

# COMMAND ----------

# DBTITLE 1,Cell 15
df_contract.write.format("delta").mode("overwrite").save(
    "abfss://gold@telcomlakehouse.dfs.core.windows.net/contract_analysis/"
)

# COMMAND ----------

df_payment.write.format("delta").mode("overwrite").save(
    "abfss://gold@telcomlakehouse.dfs.core.windows.net/payment_analysis/"
)

df_churn.write.format("delta").mode("overwrite").save(
    "abfss://gold@telcomlakehouse.dfs.core.windows.net/churn_summary/"
)

# COMMAND ----------

#Customers with month-to-month contracts show higher churn
from pyspark.sql.functions import col, sum, count, when

df_contract_churn=df_silver.groupBy("Contract").agg(
    count("*").alias("total_customer"),
    sum(when(col("churn")=="Yes",1).otherwise(0)).alias("churn_customer")
)
df_contract_churn=df_contract_churn.withColumn("churn_rate",col("churn_customer")/col("total_customer"))

df_contract_churn.display()

# COMMAND ----------

#Auto-payment users generate higher revenue stability / Revenue Stability by Payment Method
df_payment=df_silver.groupBy("PaymentMethod").agg(
    count("*").alias("Total_Customer"),
    sum("TotalCharges").alias("Total_Revenue")
)
df_payment.display()