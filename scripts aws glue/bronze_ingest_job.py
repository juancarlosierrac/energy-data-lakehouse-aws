import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import *

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


bucket = "datalake-amaris-energy"
ingest_date = "2025-06-17"
raw_path = f"s3://{bucket}/raw/load_date={ingest_date}/"
bronze_base = f"s3://{bucket}/bronze/ingest_date={ingest_date}/"


df_prov = spark.read.option("header", True).csv(raw_path + "providers.csv") \
  .withColumn("contract_date", to_timestamp(col("contract_date"), "yyyy-MM-dd")) \
  .withColumn("risk_score", col("risk_score").cast("integer")) \
  .dropDuplicates(["provider_id"])
df_prov.write.mode("overwrite").parquet(bronze_base + "providers/")


df_cli = spark.read.option("header", True).csv(raw_path + "clients.csv") \
  .withColumn("signup_date", to_timestamp(col("signup_date"), "yyyy-MM-dd")) \
  .dropDuplicates(["client_id"])
df_cli.write.mode("overwrite").parquet(bronze_base + "clients/")


df_tx = spark.read.option("header", True).csv(raw_path + "transactions.csv") \
  .withColumn("transaction_date", to_timestamp(col("transaction_date"), "yyyy-MM-dd")) \
  .withColumn("total_price", col("total_price").cast("double")) \
  .dropDuplicates(["transaction_id"]) \
  .withColumn("month_of_year", month(col("transaction_date")))
df_tx.write.mode("overwrite").parquet(bronze_base + "transactions/")
