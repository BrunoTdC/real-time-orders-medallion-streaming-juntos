# Databricks notebook source
# 00_config_paths_and_schema

from pyspark.sql.types import *
from pyspark.sql.functions import *

# ===========================
# Paths 
# ===========================
bronze_path                     = "/mnt/datalake/bronze/sales_line_items"
bronze_checkpoint               = "/mnt/datalake/_checkpoints/bronze/sales_line_items"

silver_trusted_path             = "/mnt/datalake/silver/trusted/sales_line_items"
silver_quarantine_path          = "/mnt/datalake/silver/quarantine/sales_line_items"
silver_business_path            = "/mnt/datalake/silver/business/sales_line_items"

gold_dim_product_path           = "/mnt/datalake/gold/dim_product"
gold_dim_customer_path          = "/mnt/datalake/gold/dim_customer"
gold_dim_date_path              = "/mnt/datalake/gold/dim_date"
gold_fact_sales_path            = "/mnt/datalake/gold/fact_sales"
gold_metrics_daily_country_path = "/mnt/datalake/gold/metrics_daily_country"

# =======================================
# Config do Event Hubs com protocolo Kafka
# =======================================
kafka_bootstrap_servers = "<NAMESPACE>.servicebus.windows.net:9093"
event_hub_name          = "sales-transactions-raw"

kafka_sasl = (
    "org.apache.kafka.common.security.plain.PlainLoginModule required "
    "username='$ConnectionString' "
    "password='<CONNECTION_STRING_EVENT_HUBS>';"
)

# =======================================
# Schema do payload de vendas (mensagem)
# =======================================
sales_schema = StructType([
    StructField("TransactionNo", StringType(),  True),
    StructField("Date",          StringType(),  True),  # depois convertemos
    StructField("ProductNo",     StringType(),  True),
    StructField("ProductName",   StringType(),  True),
    StructField("Price",         DoubleType(),  True),
    StructField("Quantity",      IntegerType(), True),
    StructField("CustomerNo",    StringType(),  True),
    StructField("Country",       StringType(),  True),
])

