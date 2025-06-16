import sys
from awsglue.utils    import getResolvedOptions
from awsglue.context  import GlueContext
from pyspark.context  import SparkContext
from pyspark.sql      import SparkSession
from pyspark.sql.functions import col, when


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


bucket           = "datalake-amaris-energy"
ingest_date      = "2025-06-17"
transform_date   = "2025-06-17"


bronze_base = f"s3://{bucket}/bronze/ingest_date={ingest_date}/"
silver_dest = f"s3://{bucket}/silver/transform_date={transform_date}/transactions_enriched/"


df_tx = spark.read.parquet(bronze_base + "transactions/")
df_pr = spark.read.parquet(bronze_base + "providers/")
df_cl = spark.read.parquet(bronze_base + "clients/")


df = df_tx \
    .join(df_pr, "provider_id") \
    .join(df_cl, "client_id")


df = df.filter(
    (col("total_price") > 0) &
    col("region").isin("North","South","East","West")
)


df = df.withColumn("risk_category",
    when(col("risk_score") < 40, "Low")
   .when(col("risk_score") < 70, "Medium")
   .otherwise("High")
)


df.write.mode("overwrite").parquet(silver_dest)
