# Databricks notebook source
# DBTITLE 1,Data Processing Job Notebook
# MAGIC %md
# MAGIC # Data Processing Notebook
# MAGIC
# MAGIC This notebook is executed by the `data_processing_job` defined in the bundle.
# MAGIC
# MAGIC It receives parameters from the job: `catalog` and `schema`.

# COMMAND ----------

# DBTITLE 1,Get parameters
# Parameters passed from the job 999
dbutils.widgets.text("catalog", "main", "Catalog Name")
dbutils.widgets.text("schema", "default", "Schema Name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"Processing data in {catalog}.{schema}")

# COMMAND ----------

# DBTITLE 1,Create sample data
from pyspark.sql import functions as F
from datetime import datetime

# Create sample data
data = [
    (1, "Product A", 100, "2024-01-15"),
    (2, "Product B", 200, "2024-01-15"),
    (3, "Product C", 150, "2024-01-16"),
    (4, "Product D", 300, "2024-01-16"),
    (5, "Product E", 250, "2024-01-17")
]

df = spark.createDataFrame(data, ["id", "product_name", "sales_amount", "sale_date"])

# Add processing timestamp
df = df.withColumn("processed_at", F.lit(datetime.now()))

display(df)

# COMMAND ----------

# DBTITLE 1,Transform data
# Perform some transformations
result_df = (
    df
    .withColumn("sale_date", F.to_date("sale_date"))
    .withColumn("sales_with_tax", F.col("sales_amount") * 1.1)
    .withColumn("category", 
        F.when(F.col("sales_amount") > 200, "High")
         .when(F.col("sales_amount") > 100, "Medium")
         .otherwise("Low")
    )
)

print(f"Processed {result_df.count()} records")
display(result_df)

# COMMAND ----------

