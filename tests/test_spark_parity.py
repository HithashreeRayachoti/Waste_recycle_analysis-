"""The PySpark silver must match the DuckDB SQL silver row for row."""

import sys

import pytest
import synthetic_dsny

from nyrt.config import REPO_ROOT
from nyrt.transform import warehouse

pyspark = pytest.importorskip("pyspark", reason="pyspark not installed")
pytestmark = pytest.mark.spark
sys.path.insert(0, str(REPO_ROOT / "spark"))


def test_spark_silver_matches_sql(settings):
    from pyspark.sql import SparkSession
    from silver_spark import build_silver
    from test_warehouse import write_bronze

    write_bronze(settings, synthetic_dsny.records(months=14))
    con = warehouse.connect(settings)
    warehouse.load_seeds(con, settings)
    warehouse.run_silver(con, settings, warehouse.available_bronze_months(settings))
    cols = "district_id, month, material, round(tons_collected, 3) as t, quality_flag, n_source_columns"
    expected = con.execute(f"select {cols} from silver.tonnage order by 1, 2, 3").fetchall()

    spark = SparkSession.builder.master("local[1]").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    bronze = spark.read.option("recursiveFileLookup", "true").parquet(
        str(settings.bronze_dir / "source_extract")
    )
    df = build_silver(spark, bronze, str(REPO_ROOT / "seeds"))
    df.createOrReplaceTempView("s")
    got = [tuple(r) for r in spark.sql(f"select {cols} from s order by 1, 2, 3").collect()]
    spark.stop()
    assert len(got) == len(expected)
    assert got == expected
