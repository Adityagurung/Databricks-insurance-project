import dlt
from pyspark.sql.functions import *

# DLT Pipeline for Claims, Customers, and Policies CSV files

@dlt.table(
    comment="Claims data from CSV file"
)
def claims():
    return spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load("/Volumes/smart_claims_dev/landing/sql_server/claims.csv") \
                .select(
                    col("*"),
                    lit("claims.csv").alias("source_file"),
                    from_utc_timestamp(current_timestamp(), "Asia/Kolkata").alias("ingestion_timestamp")
                )

@dlt.table(
    comment="Customers data from CSV file"
)
def customers():
    return spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load("/Volumes/smart_claims_dev/landing/sql_server/customers.csv") \
                .select(
                    col("*"),
                    lit("customers.csv").alias("source_file"),
                    from_utc_timestamp(current_timestamp(), "Asia/Kolkata").alias("ingestion_timestamp")
                )

@dlt.table(
    comment="Policies data from CSV file"
)
def policies():
    return spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load("/Volumes/smart_claims_dev/landing/sql_server/policies.csv") \
                .select(
                    col("*"),
                    lit("policies.csv").alias("source_file"),
                    from_utc_timestamp(current_timestamp(), "Asia/Kolkata").alias("ingestion_timestamp")
                )