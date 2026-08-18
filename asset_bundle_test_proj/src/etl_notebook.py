# Databricks notebook source
# DBTITLE 1,ETL Pipeline Notebook
# MAGIC %md
# MAGIC # ETL Pipeline Notebook
# MAGIC
# MAGIC This notebook defines streaming tables for a Spark Declarative Pipeline.
# MAGIC
# MAGIC It creates a simple bronze → silver → gold architecture.

# COMMAND ----------

# DBTITLE 1,Bronze layer - raw data
import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="bronze_sales",
    comment="Raw sales data ingested from source"
)
def bronze_sales():
    # In a real scenario, this would read from a source
    # For demo purposes, creating sample data
    return (
        spark.createDataFrame([
            (1, "2024-01-15", 100.0, "USD"),
            (2, "2024-01-15", 200.0, "USD"),
            (3, "2024-01-16", 150.0, "EUR"),
        ], ["transaction_id", "date", "amount", "currency"])
    )

# COMMAND ----------

# DBTITLE 1,Silver layer - cleaned data
@dlt.table(
    name="silver_sales",
    comment="Cleaned and validated sales data"
)
@dlt.expect_or_drop("valid_amount", "amount > 0")
@dlt.expect_or_drop("valid_currency", "currency IN ('USD', 'EUR', 'GBP')")
def silver_sales():
    return (
        dlt.read("bronze_sales")
        .withColumn("date", F.to_date("date"))
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
    )

# COMMAND ----------

# DBTITLE 1,Gold layer - aggregated metrics
@dlt.table(
    name="gold_daily_sales",
    comment="Daily aggregated sales metrics"
)
def gold_daily_sales():
    return (
        dlt.read("silver_sales")
        .groupBy("date", "currency")
        .agg(
            F.sum("amount").alias("total_sales"),
            F.count("transaction_id").alias("transaction_count"),
            F.avg("amount").alias("avg_transaction_value")
        )
    )

# COMMAND ----------

