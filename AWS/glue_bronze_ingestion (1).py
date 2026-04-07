"""
Week 1 — AWS Glue PySpark Job
Reads CSV from S3 landing zone → validates → writes Parquet to Bronze
Triggered manually or via EventBridge schedule

Deploy: aws glue create-job --name ecomm-bronze-ingestion --role AWSGlueServiceRole \
    --command '{"Name":"glueetl","ScriptLocation":"s3://your-bucket/scripts/glue_bronze_ingestion.py"}'
"""

import sys
import logging
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, TimestampType
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "S3_LANDING_BUCKET",   # s3://ecomm-landing/raw/
    "S3_BRONZE_BUCKET",    # s3://ecomm-bronze/
    "SNOWFLAKE_WAREHOUSE", # arn or warehouse name — used in next COPY INTO step
    "ENV",                 # staging | prod
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

RUN_DATE = datetime.utcnow().strftime("%Y-%m-%d")
ENV = args["ENV"]
LANDING = args["S3_LANDING_BUCKET"].rstrip("/")
BRONZE = args["S3_BRONZE_BUCKET"].rstrip("/")


# ── Schema definitions (enforce at ingestion, fail fast on drift) ─────────────

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("discount_pct", DoubleType(), True),
    StructField("discount_amount", DoubleType(), True),
    StructField("gmv", DoubleType(), False),
    StructField("net_revenue", DoubleType(), False),
    StructField("order_status", StringType(), False),
    StructField("is_returned", BooleanType(), True),
    StructField("return_reason", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("pincode", StringType(), True),
    StructField("order_date", TimestampType(), False),
    StructField("delivery_date", StringType(), True),
    StructField("created_at", TimestampType(), False),
])

PAYMENTS_SCHEMA = StructType([
    StructField("payment_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("payment_method", StringType(), False),
    StructField("payment_status", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), True),
    StructField("gateway", StringType(), True),
    StructField("gateway_txn_id", StringType(), True),
    StructField("failure_reason", StringType(), True),
    StructField("refund_amount", DoubleType(), True),
    StructField("payment_date", TimestampType(), False),
    StructField("created_at", TimestampType(), False),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("sub_category", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("mrp", DoubleType(), True),
    StructField("selling_price", DoubleType(), False),
    StructField("cost_price", DoubleType(), True),
    StructField("stock_quantity", IntegerType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("created_at", TimestampType(), False),
    StructField("updated_at", TimestampType(), True),
])


def validate_and_tag(df, table_name):
    """Add pipeline metadata columns and assert critical nulls."""
    null_counts = {
        col: df.filter(F.col(col).isNull()).count()
        for col in ["order_id" if table_name == "orders" else
                    "payment_id" if table_name == "payments" else "product_id"]
    }
    for col, cnt in null_counts.items():
        if cnt > 0:
            raise ValueError(
                f"SCHEMA DRIFT ALERT: {table_name}.{col} has {cnt} nulls. "
                "Blocking Bronze load — check upstream source."
            )

    return df.withColumn("_ingested_at", F.lit(datetime.utcnow().isoformat())) \
             .withColumn("_run_date", F.lit(RUN_DATE)) \
             .withColumn("_source_env", F.lit(ENV)) \
             .withColumn("_row_count", F.count("*").over(
                 __import__("pyspark.sql.window", fromlist=["Window"]).Window.partitionBy(F.lit(1))
             ))


def load_table(table_name, schema):
    logger.info(f"Loading {table_name}...")
    src_path = f"{LANDING}/{table_name}/"
    out_path = f"{BRONZE}/{table_name}/run_date={RUN_DATE}/"

    try:
        df = spark.read.csv(src_path, schema=schema, header=True, timestampFormat="yyyy-MM-dd HH:mm:ss")
    except Exception as e:
        raise RuntimeError(f"Failed to read {table_name} from {src_path}: {e}")

    row_count = df.count()
    logger.info(f"  Read {row_count:,} rows from {src_path}")

    # Validate
    df = validate_and_tag(df, table_name)

    # Write partitioned Parquet to Bronze
    df.write.mode("overwrite").partitionBy("_run_date").parquet(out_path)
    logger.info(f"  Wrote {row_count:,} rows → {out_path}")

    # Emit CloudWatch row count metric
    try:
        import boto3
        cw = boto3.client("cloudwatch")
        cw.put_metric_data(
            Namespace="ECommPipeline",
            MetricData=[{
                "MetricName": f"{table_name}_row_count",
                "Value": row_count,
                "Unit": "Count",
                "Dimensions": [{"Name": "env", "Value": ENV}],
            }]
        )
    except Exception as e:
        logger.warning(f"CloudWatch metric push failed (non-fatal): {e}")

    return row_count


# ── Main ───────────────────────────────────────────────────────────────────────

try:
    counts = {
        "orders": load_table("orders", ORDERS_SCHEMA),
        "payments": load_table("payments", PAYMENTS_SCHEMA),
        "products": load_table("products", PRODUCTS_SCHEMA),
    }
    logger.info("Bronze ingestion complete. Row counts: %s", counts)
except Exception as e:
    logger.error("PIPELINE FAILURE: %s", e)
    raise

job.commit()
