# Databricks notebook source
# 03_silver_business_rules

%run ./00_config_paths_and_schema

from pyspark.sql.functions import *

# =================================================================
# 1. Leitura da Silver Trusted
# =================================================================

df_trusted = spark.read.format("delta").load(silver_trusted_path)

# =================================================================
# 2. Aplicação de regras de negócio
# =================================================================

test_customers = ["99999", "TESTE", "12345"]  

df_business = (
    df_trusted
    # remove clientes de teste
    .filter(~col("customer_no").isin(test_customers))
    # receita da linha
    .withColumn("line_total", col("price") * col("quantity"))
    # flag por país
    .withColumn("is_uk_customer", col("country") == lit("UNITED KINGDOM"))
    # data da venda (dia) – usando event_datetime já calculado no notebook 02
    .withColumn("sale_date", to_date("event_datetime"))
)

# =================================================================
# 3. Escrita da Silver Business COM PARTICIONAMENTO
#    Particionando por sale_date para facilitar consultas por período
# =================================================================

(
    df_business
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("sale_date")        # <-- particionando por date
    .save(silver_business_path)
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver_sales_business
    USING DELTA
    LOCATION '{silver_business_path}'
""")

