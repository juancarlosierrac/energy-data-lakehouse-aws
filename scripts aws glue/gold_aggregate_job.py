import sys
from awsglue.utils    import getResolvedOptions
from awsglue.context  import GlueContext
from pyspark.context  import SparkContext
from pyspark.sql.functions import col, sum as _sum, count as _count, avg as _avg


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


bucket         = "datalake-amaris-energy"
transform_date = "2025-06-17"


silver_base = f"s3://{bucket}/silver/transform_date={transform_date}/transactions_enriched/"


gold_base = f"s3://{bucket}/gold/publish_date={transform_date}/"


df = spark.read.parquet(silver_base)


df_region = (df.groupBy("region")
               .agg(_sum("total_price").alias("total_revenue")))
df_region.write.mode("overwrite").parquet(gold_base + "revenue_by_region/")


df_risk = (df.groupBy("risk_category")
             .agg(_count("*").alias("num_tx")))
df_risk.write.mode("overwrite").parquet(gold_base + "risk_summary/")


df_cust = (df.groupBy("client_id")
             .agg(
               _count("*").alias("num_tx"),
               _avg("total_price").alias("avg_spend")
             ))
df_cust.write.mode("overwrite").parquet(gold_base + "customer_stats/")
