import dlt
from pyspark.sql.functions import *

# BRONZE LAYER - Single table with IST timezone
@dlt.table(
    comment="Telematics data in bronze layer with IST timezone"
)
def telematics():
    return spark.read.format("parquet") \
                .load("/Volumes/smart_claims_dev/landing/telematics/") \
                .select(
                    col("*"),
                    from_utc_timestamp(current_timestamp(), "Asia/Kolkata").alias("ingestion_timestamp")
                )