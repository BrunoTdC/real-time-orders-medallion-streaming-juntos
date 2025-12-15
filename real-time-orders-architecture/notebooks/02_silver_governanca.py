# Databricks notebook source
# 02_silver_governanca

%run ./00_config_paths_and_schema

from pyspark.sql.functions import *

# =======================================================
# 1. Leitura da Bronze (Delta)
#    Em produção, agendar esse notebook como Job (batch)
# =======================================================

df_bronze = spark.read.format("delta").load(bronze_path)

# =======================================================
# 2. Normalização de tipos / datas / campos
# =======================================================

df_typed = (
    df_bronze
    .withColumn("event_datetime", to_timestamp("event_date_raw", "dd/M/yyyy"))
    .withColumn("event_date", to_date("event_datetime"))  # <- coluna de data para particionamento
    .withColumn("price", col("price").cast("decimal(10,2)"))
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("country", upper(trim(col("country"))))
)

# =======================================================
# 3. Regras de qualidade (camada de governança)
# =======================================================

df_quality = (
    df_typed
    .withColumn(
        "valid_record",
        (
            col("transaction_no").isNotNull() &
            col("product_no").isNotNull() &
            col("customer_no").isNotNull() &
            col("event_datetime").isNotNull() &
            (col("quantity") > 0) &
            (col("price") >= 0)
        )
    )
)

df_trusted    = df_quality.filter(col("valid_record") == True).drop("valid_record")
df_quarantine = df_quality.filter(col("valid_record") == False)

# =======================================================
# 4. Escrita Silver Trusted + Quarantine COM PARTICIONAMENTO
#    Aqui particionamos por event_date (data do evento)
# =======================================================

(
    df_trusted
    .write
    .format("delta")
    .mode("overwrite")               # em prod: ideal usar MERGE incremental
    .option("overwriteSchema", "true")
    .partitionBy("event_date")       # <-- particionando por date
    .save(silver_trusted_path)
)

(
    df_quarantine
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("event_date")       # <-- particionando também a quarentena
    .save(silver_quarantine_path)
)

# Opcional: criar tabelas no metastore
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver_sales_trusted
    USING DELTA
    LOCATION '{silver_trusted_path}'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver_sales_quarantine
    USING DELTA
    LOCATION '{silver_quarantine_path}'
""")
