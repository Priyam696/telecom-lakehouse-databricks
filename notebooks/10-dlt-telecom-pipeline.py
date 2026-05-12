# Databricks notebook source
import dlt
from pyspark.sql.functions import col, when, trim, lit,current_timestamp
from pyspark.sql.types import DoubleType

#Bronze Layer-Raw ingestion

@dlt.table(
    name="bronze_raw_churn",
    comment="Raw telecom churn data ingested from ADLS  Bronze Container",
    table_properties = {
        "quality" : "bronze",
        "pipelines.reset.allowed": "true"
    }
)

def bronze_raw_churn():
    return(
        spark.read.format("csv")
        .option("header", "true")
        .option("inferschema","true")
        .load("abfss://bronze@telkomlakehouse.dfs.core.windows.net/telecom_churn/")
    )

#Silver Layer-Cleaned & Validated

@dlt.table(
    name="silver_subscribers",
    comment="Cleaned, validated subscriber data with churn flags",
    table_properties = {"quality" : "silver"}
)

#Data Quality Expectations

@dlt.expect("Valid_customer_id","CustomerId IS NOT NULL")
@dlt.expect("Valid_Gender","gender IN ('Male','Female')")
@dlt.expect_or_drop("Valid_churn_value","Churn IN ('Yes','No')")
@dlt.expect_or_drop("Positive_monthly_charges","MonthlyCharges > 0")

def silver_subscrivers():
    return(
        dlt.read("bronze_raw_churn")
        .dropDuplicates(["CustomerId"])
        # Fix TotalCharges — blank strings to null
        .withColumn("Total_charges",when(trim(col("TotalCharges"))=="", None)
                    .otherwise(col("TotalCharges").cast(DoubleType())))
        # Fix SeniorCitizen — 0/1 to No/Yes
        .withColumn("SeniorCitizen",when(col("SeniorCitizen")==1,"Yes")
                    .otherwise("No"))
        # Add Churn Flag
        .withColum("ChurnFlag",when(col("Churn")=="Yes",1)
                   .otherwise(0))
        # Add Ingestion Timestamp
        .withColumn("LoadTimeStamp",current_timestamp())
        .withcolumn("DataSource",lit("Kaggle_Telco_v1"))
    )

# GOLD LAYER — Business Aggregations

@dlt.table(
    name="gold_churn_by_contract",
    comment="Churn analysis aggregated by contract type and internet service",
    table_properties={"quality":"gold"}
)

def gold_churn_by_contract():
    from pyspark.sql.functions import count, avg, sum as _sum, round as _round
    return(
        dlt.read("silver_subscribers")
        .groupBy("Contact","InternetServices")
        .agg(
            count("CustomerId").alias("customer_total"),
            _sum("ChurnFlag").alias("Total_Churned"),
            _round(avg("MonthlyCharges"), 2),alias("Avg_Monthly_charges"),
            _round("TotalCharges", 2).alia("Avg_total_charges")
        )
        .withColumn("Churn_rate_PCT",_round(col("Total_Churned") / col("customer_total") * 100, 2))
    )

@dlt.table(
    name="Gold_revenue_by_payment",
    comment="Revenue analysis by payment method",
    table_properties={"quality":"gold"}
)

def gold_revenue_by_payment():
    from pyspark.sql.functions import sum as _sum, round as _round, count
    return(
        dlt.read("silver_subscribers")
        .groupBy("PaymentMethod")
        .agg(
            count("customerID")              .alias("Total_Customers"),
            _round(_sum("MonthlyCharges"), 2).alias("Total_Monthly_Revenue"),
            _round(_sum("TotalCharges"),   2).alias("Total_Revenue")
        )
        .orderBy("Total_Revenue",ascending=False)
)
    
@dlt.table(
    name="gold_churn_summary",
    comment="Overall churn KPIs for dashboard",
    table_properties={"quality":"gold"}
)

def gold_churn_summary():
    from pyspark.sql.functions import count, avg, sum as _sum, round as _round
    return(
        dlt.read("silver_subscribers")
        .agg(
            count("CustomerId").alias("Total_Customers"),
            _sum("ChurnFlag").alias("Total_Churned"),
            _round(
                _sum("churnFlag")/count("customerId") * 100, 2
            ).alias("Overall_Churn_Rate_Pct")
        )
        )


# COMMAND ----------

