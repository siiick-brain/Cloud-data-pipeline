"""Delta demo: a wrong-schema write is rejected, VERSION AS OF reads an older version, RESTORE fixes a
broken table. Runs on its own table under $DELTA_ROOT/demo so the pipeline tables are untouched.

Usage: python3 -m spark.time_travel
"""

from __future__ import annotations

import shutil

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from spark import jobs

SCHEMA = "order_id string, quantity int"


def demo(spark: SparkSession, path: str) -> dict:
    shutil.rmtree(path, ignore_errors=True)
    spark.createDataFrame([("O1", 1), ("O2", 2)], SCHEMA).write.format("delta").save(path)  # v0
    spark.createDataFrame([("O3", 3)], SCHEMA).write.format("delta").mode("append").save(path)  # v1

    try:  # wrong schema: Delta enforces the table schema and refuses the write
        spark.createDataFrame([("O4", "lots")], "order_id string, quantity string") \
            .write.format("delta").mode("append").save(path)
        rejected = False
    except Exception:  # AnalysisException
        rejected = True

    DeltaTable.forPath(spark, path).delete()  # v2: someone breaks the table
    broken = spark.read.format("delta").load(path).count()
    v0 = spark.read.format("delta").option("versionAsOf", 0).load(path).count()
    DeltaTable.forPath(spark, path).restoreToVersion(1)
    restored = spark.read.format("delta").load(path).count()
    return {"bad_write_rejected": rejected, "rows_v0": v0, "rows_broken": broken, "rows_restored": restored}


def main() -> None:
    result = demo(jobs.get_spark(), jobs.table_path("demo.time_travel"))
    print(f"bad write rejected: {result['bad_write_rejected']}")
    print(f"rows at VERSION AS OF 0: {result['rows_v0']}")
    print(f"rows after bad delete: {result['rows_broken']}")
    print(f"rows restored to version 1: {result['rows_restored']}")


if __name__ == "__main__":
    main()
