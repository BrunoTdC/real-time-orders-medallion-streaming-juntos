# Databricks notebook source
# 01_bronze_ingest_streaming

# Carrega config comum
%run ./00_config_paths_and_schema

from pyspark.sql.functions import *

# ===========================
# Leitura streaming do Event Hubs (Kafka)
# ===========================

df_raw = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
         .option("subscribe", event_hub_name)
         .option("kafka.security.protocol", "SASL_SSL")
         .option("kafka.sasl.mechanism", "PLAIN")
         .option("kafka.sasl.jaas.config", kafka_sasl)
         .option("startingOffsets", "latest")
         .load()
)

# ===========================
# Parse do JSON + metadados
# ===========================

df_bronze = (
    df_raw
    .selectExpr(
        "CAST(value AS STRING) AS value_str",
        "topic",
        "partition",
        "offset",
        "timestamp AS event_timestamp"
    )
    .withColumn("json", from_json("value_str", sales_schema))
    .select(
        col("json.TransactionNo").alias("transaction_no"),
        col("json.Date").alias("event_date_raw"),
        col("json.ProductNo").alias("product_no"),
        col("json.ProductName").alias("product_name"),
        col("json.Price").alias("price"),
        col("json.Quantity").alias("quantity"),
        col("json.CustomerNo").alias("customer_no"),
        col("json.Country").alias("country"),
        col("topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("event_timestamp"),
        current_timestamp().alias("ingestion_ts"),
        lit("orders_app").alias("source_system")
    )
)

# ===========================
# Escrita streaming em Bronze (Delta)
# ===========================

query_bronze = (
    df_bronze
    .writeStream
    .format("delta")
    .option("checkpointLocation", bronze_checkpoint)
    .option("path", bronze_path)
    .outputMode("append")
    .trigger(processingTime="10 seconds") 
    .start()
)




