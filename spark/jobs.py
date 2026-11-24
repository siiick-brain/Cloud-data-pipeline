"""Bronze, silver and gold Delta jobs. Contract: docs/contracts/delta_tables.md."""

from __future__ import annotations

import os

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

BRONZE_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("payload", StringType()),
    StructField("load_date", DateType()),
    StructField("ingested_at", TimestampType()),
])

SILVER_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("order_id", StringType()),
    StructField("line_number", IntegerType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("store_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DecimalType(10, 2)),
    StructField("currency", StringType()),
    StructField("net_amount", DecimalType(12, 2)),
    StructField("occurred_at", TimestampType()),
    StructField("load_date", DateType()),
])

# Shape of the JSON payload (docs/contracts/order_event.schema.json).
PAYLOAD_SCHEMA = StructType([
    StructField("occurred_at", StringType()),
    StructField("order_id", StringType()),
    StructField("line_number", IntegerType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("store_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DecimalType(10, 2)),
    StructField("currency", StringType()),
])


def get_spark() -> SparkSession:
    """Local-mode session with Delta. getOrCreate reuses the running session."""
    return (
        SparkSession.builder.master("local[*]")
        .appName("spark-jobs")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def table_path(table: str) -> str:
    return f"{os.environ.get('DELTA_ROOT', '/data/delta')}/{table.replace('.', '/')}"


def merge_insert(spark: SparkSession, df: DataFrame, table: str, schema: StructType) -> int:
    """Create the table with a fixed schema if missing, then MERGE on event_id, insert when not matched."""
    path = table_path(table)
    DeltaTable.createIfNotExists(spark).location(path).addColumns(schema).execute()
    target = DeltaTable.forPath(spark, path)
    (
        target.alias("t")
        .merge(df.alias("s"), "t.event_id = s.event_id")
        .whenNotMatchedInsertAll()
        .execute()
    )
    return target.toDF().count()


def read_raw(spark: SparkSession) -> DataFrame:
    return (
        spark.read.format("jdbc")
        .option("url", os.environ.get("JDBC_URL", "jdbc:postgresql://postgres:5432/warehouse"))
        .option("user", os.environ.get("JDBC_USER", "pipeline"))
        .option("password", os.environ.get("JDBC_PASSWORD", ""))
        .option("driver", "org.postgresql.Driver")
        .option("query", "SELECT event_id::text AS event_id, payload::text AS payload, load_date "
                         "FROM raw.order_events")
        .load()
    )


def bronze(spark: SparkSession, source: DataFrame | None = None) -> int:
    df = read_raw(spark) if source is None else source
    df = df.select("event_id", "payload", "load_date").withColumn("ingested_at", F.current_timestamp())
    return merge_insert(spark, df.dropDuplicates(["event_id"]), "bronze.order_events", BRONZE_SCHEMA)


def silver(spark: SparkSession) -> int:
    p = F.from_json("payload", PAYLOAD_SCHEMA)
    df = (
        spark.read.format("delta").load(table_path("bronze.order_events"))
        .select("event_id", "load_date", p.alias("p"))
        .select(
            "event_id", "p.order_id", "p.line_number", "p.customer_id", "p.product_id", "p.store_id",
            "p.quantity", "p.unit_price", "p.currency",
            (F.col("p.quantity") * F.col("p.unit_price")).cast(DecimalType(12, 2)).alias("net_amount"),
            F.to_timestamp("p.occurred_at").alias("occurred_at"),
            "load_date",
        )
    )
    return merge_insert(spark, df, "silver.order_lines", SILVER_SCHEMA)


def gold(spark: SparkSession) -> None:
    lines = (
        spark.read.format("delta").load(table_path("silver.order_lines"))
        .withColumn("sale_date", F.to_date("occurred_at"))
    )
    revenue = F.sum("net_amount").cast(DecimalType(14, 2)).alias("revenue")
    units = F.sum("quantity").cast("long").alias("units")
    daily = lines.groupBy("sale_date", "store_id").agg(F.count("*").alias("order_lines"), units, revenue)
    product = lines.groupBy("sale_date", "product_id").agg(units, revenue)
    daily.write.format("delta").mode("overwrite").save(table_path("gold.daily_sales"))
    product.write.format("delta").mode("overwrite").save(table_path("gold.product_sales"))


JOBS = {"bronze": bronze, "silver": silver, "gold": gold}


def run(job: str, spark: SparkSession) -> None:
    """Run one job, or bronze, silver and gold in order for `all`. Unknown job raises KeyError."""
    for name in (list(JOBS) if job == "all" else [job]):
        JOBS[name](spark)
