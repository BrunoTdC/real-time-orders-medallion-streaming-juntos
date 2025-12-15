# Databricks notebook source
# 04_gold_dimensions_facts_metrics

%run ./00_config_paths_and_schema

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# =======================================================
# 1. Leitura da Silver Business
# =======================================================

df_silver = spark.read.format("delta").load(silver_business_path)

# =======================================================
# 2. Dimensões (sem necessidade de particionamento)
# =======================================================

# ---------- Dimensão Produto ----------
dim_product = (
    df_silver
    .select(
        col("product_no").alias("product_id"),
        col("product_name")
    )
    .dropDuplicates(["product_id"])
)

dim_product.write.format("delta").mode("overwrite").save(gold_dim_product_path)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS dim_product
    USING DELTA
    LOCATION '{gold_dim_product_path}'
""")

# ---------- Dimensão Cliente ----------
dim_customer = (
    df_silver
    .select(
        col("customer_no").alias("customer_id"),
        col("country"),
        col("is_uk_customer")
    )
    .dropDuplicates(["customer_id"])
)

dim_customer.write.format("delta").mode("overwrite").save(gold_dim_customer_path)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS dim_customer
    USING DELTA
    LOCATION '{gold_dim_customer_path}'
""")

# ---------- Dimensão Data ----------
dim_date = (
    df_silver
    .select(col("sale_date").alias("date"))
    .dropna()
    .dropDuplicates(["date"])
    .withColumn("year", year("date"))
    .withColumn("month", month("date"))
    .withColumn("day", dayofmonth("date"))
    .withColumn("weekofyear", weekofyear("date"))
)

dim_date.write.format("delta").mode("overwrite").save(gold_dim_date_path)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS dim_date
    USING DELTA
    LOCATION '{gold_dim_date_path}'
""")

# =======================================================
# 3. Fato de vendas (grain: TransactionNo + ProductNo)
#    COM PARTICIONAMENTO POR date
# =======================================================

fact_sales = (
    df_silver
    .select(
        col("transaction_no").alias("transaction_id"),
        col("sale_date").alias("date"),
        col("product_no").alias("product_id"),
        col("customer_no").alias("customer_id"),
        col("country"),
        col("quantity").alias("qty"),
        col("price").alias("unit_price"),
        col("line_total").alias("revenue")
    )
)

fact_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("date") \        # <-- particionando por date
    .save(gold_fact_sales_path)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS fact_sales
    USING DELTA
    LOCATION '{gold_fact_sales_path}'
""")

# =======================================================
# 4. Métricas agregadas (ex.: por dia / país)
#    COM PARTICIONAMENTO POR date
# =======================================================

metrics_daily_country = (
    fact_sales
    .groupBy("date", "country")
    .agg(
        sum("revenue").alias("total_revenue"),
        sum("qty").alias("total_quantity"),
        countDistinct("transaction_id").alias("total_orders"),
        countDistinct("customer_id").alias("distinct_customers")
    )
)

metrics_daily_country.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("date") \        # <-- particionando por date
    .save(gold_metrics_daily_country_path)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS metrics_daily_country
    USING DELTA
    LOCATION '{gold_metrics_daily_country_path}'
""")
