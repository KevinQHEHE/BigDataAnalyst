import argparse
import logging
import uuid
from datetime import datetime, timezone
from typing import Iterable, List, Tuple

from pyspark.sql import DataFrame, functions as F

from aq_lakehouse.spark_session import build

COMPONENT_TABLE = "hadoop_catalog.aq.silver.aq_components_hourly"
INDEX_TABLE = "hadoop_catalog.aq.silver.aq_index_hourly"

O3_MW = 48.0
NO2_MW = 46.0
SO2_MW = 64.0
CO_MW = 28.0
MOLAR_VOLUME = 24.45  # at 25°C, 1 atm


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="ISO timestamp (UTC) for start of window")
    parser.add_argument("--end", required=True, help="ISO timestamp (UTC) for end of window (inclusive)")
    parser.add_argument("--location-id", dest="location_ids", action="append", default=[], help="optional list of location IDs to process")
    parser.add_argument(
        "--mode",
        choices=["merge", "replace"],
        default="merge",
        help="merge keeps history, replace deletes rows in window before write",
    )
    parser.add_argument(
        "--calc-method",
        default="epa_like_v1",
        help="calc_method value stored alongside AQI results",
    )
    return parser.parse_args()


def parse_iso_ts(value: str) -> datetime:
    ts = datetime.fromisoformat(value)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    else:
        ts = ts.astimezone(timezone.utc)
    return ts


def ensure_table(spark) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS hadoop_catalog.aq")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS hadoop_catalog.aq.silver")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS hadoop_catalog.aq.silver.aq_index_hourly (
          location_id STRING,
          ts_utc TIMESTAMP,
          date_utc DATE,
          aqi INT,
          category STRING,
          dominant_pollutant STRING,
          aqi_pm25 INT,
          aqi_pm10 INT,
          aqi_o3 INT,
          aqi_no2 INT,
          aqi_so2 INT,
          aqi_co INT,
          calc_method STRING,
          run_id STRING,
          computed_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (location_id, days(ts_utc))
        """
    )
    spark.sql(
        """
        ALTER TABLE hadoop_catalog.aq.silver.aq_index_hourly SET TBLPROPERTIES (
          'format-version'='2',
          'write.target-file-size-bytes'='134217728',
          'write.distribution-mode'='hash'
        )
        """
    )


def load_components(spark, start_ts: datetime, end_ts: datetime, location_ids: Iterable[str]) -> DataFrame:
    condition = (F.col("ts_utc") >= F.lit(start_ts)) & (F.col("ts_utc") <= F.lit(end_ts))
    df = spark.table(COMPONENT_TABLE).where(condition)
    if location_ids:
        df = df.where(F.col("location_id").isin(list(location_ids)))
    return df


def aqi_breakpoints() -> dict[str, List[Tuple[float, float, int, int]]]:
    return {
        "pm25": [
            (0.0, 12.0, 0, 50),
            (12.1, 35.4, 51, 100),
            (35.5, 55.4, 101, 150),
            (55.5, 150.4, 151, 200),
            (150.5, 250.4, 201, 300),
            (250.5, 350.4, 301, 400),
            (350.5, 500.4, 401, 500),
        ],
        "pm10": [
            (0.0, 54.0, 0, 50),
            (55.0, 154.0, 51, 100),
            (155.0, 254.0, 101, 150),
            (255.0, 354.0, 151, 200),
            (355.0, 424.0, 201, 300),
            (425.0, 504.0, 301, 400),
            (505.0, 604.0, 401, 500),
        ],
        "o3": [  # 8h, ppm
            (0.000, 0.054, 0, 50),
            (0.055, 0.070, 51, 100),
            (0.071, 0.085, 101, 150),
            (0.086, 0.105, 151, 200),
            (0.106, 0.200, 201, 300),
        ],
        "no2": [  # 1h, ppb
            (0.0, 53.0, 0, 50),
            (54.0, 100.0, 51, 100),
            (101.0, 360.0, 101, 150),
            (361.0, 649.0, 151, 200),
            (650.0, 1249.0, 201, 300),
            (1250.0, 1649.0, 301, 400),
            (1650.0, 2049.0, 401, 500),
        ],
        "so2": [  # 1h, ppb
            (0.0, 35.0, 0, 50),
            (36.0, 75.0, 51, 100),
            (76.0, 185.0, 101, 150),
            (186.0, 304.0, 151, 200),
            (305.0, 604.0, 201, 300),
            (605.0, 804.0, 301, 400),
            (805.0, 1004.0, 401, 500),
        ],
        "co": [  # 8h, ppm
            (0.0, 4.4, 0, 50),
            (4.5, 9.4, 51, 100),
            (9.5, 12.4, 101, 150),
            (12.5, 15.4, 151, 200),
            (15.5, 30.4, 201, 300),
            (30.5, 40.4, 301, 400),
            (40.5, 50.4, 401, 500),
        ],
    }


def linear_aqi(column: F.Column, breakpoints: List[Tuple[float, float, int, int]]) -> F.Column:
    expr = F.lit(None).cast("double")
    for c_low, c_high, a_low, a_high in breakpoints:
        slope = (a_high - a_low) / (c_high - c_low)
        piece = (column - F.lit(c_low)) * F.lit(slope) + F.lit(a_low)
        expr = F.when((column >= F.lit(c_low)) & (column <= F.lit(c_high)), piece).otherwise(expr)

    # handle values above the highest breakpoint
    # NOTE: we intentionally DO NOT extrapolate beyond the highest AQI breakpoint to avoid
    # producing implausible AQI values. Instead clamp to the top AQI (typically 500).
    max_c_low, max_c_high, max_a_low, max_a_high = breakpoints[-1]
    expr = F.when(column > F.lit(max_c_high), F.lit(max_a_high)).otherwise(expr)

    expr = F.when(column.isNull(), None).otherwise(expr)
    return F.round(expr).cast("int")


def compute_indices(df: DataFrame, calc_method: str, run_id: str) -> DataFrame:
    bps = aqi_breakpoints()

    pm25_aqi = linear_aqi(F.col("pm25_24h_avg"), bps["pm25"])
    pm10_aqi = linear_aqi(F.col("pm10_24h_avg"), bps["pm10"])

    # Unit conversions: input component units are expected as follows:
    # - o3_8h_max: µg/m³  -> convert to ppm (µg/m³ * (MOLAR_VOLUME / (MW * 1000)))
    # - co_8h_max:  µg/m³  -> convert to ppm (µg/m³ * (MOLAR_VOLUME / (MW * 1000)))
    # - no2_1h_max: µg/m³  -> convert to ppb (µg/m³ * (MOLAR_VOLUME / (MW * 1000)) * 1000)
    # - so2_1h_max: µg/m³  -> convert to ppb (µg/m³ * (MOLAR_VOLUME / (MW * 1000)) * 1000)
    # The factor 1000 adjustments map between µg/m³ and mg/m³ or ppm/ppb as needed.

    o3_ppm = F.col("o3_8h_max") * (MOLAR_VOLUME / (O3_MW * 1000.0))
    # CO molecular weight conversion: convert µg/m³ -> ppm (use same *1/1000 factor)
    co_ppm = F.col("co_8h_max") * (MOLAR_VOLUME / (CO_MW * 1000.0))
    # NO2 and SO2 breakpoints are in ppb: convert µg/m³ -> ppb by scaling to ppm then *1000
    no2_ppb = F.col("no2_1h_max") * (MOLAR_VOLUME / (NO2_MW * 1000.0)) * F.lit(1000.0)
    so2_ppb = F.col("so2_1h_max") * (MOLAR_VOLUME / (SO2_MW * 1000.0)) * F.lit(1000.0)

    o3_aqi = linear_aqi(o3_ppm, bps["o3"])
    co_aqi = linear_aqi(co_ppm, bps["co"])
    no2_aqi = linear_aqi(no2_ppb, bps["no2"])
    so2_aqi = linear_aqi(so2_ppb, bps["so2"])

    df = (
        df
        .withColumn("aqi_pm25", pm25_aqi)
        .withColumn("aqi_pm10", pm10_aqi)
        .withColumn("aqi_o3", o3_aqi)
        .withColumn("aqi_no2", no2_aqi)
        .withColumn("aqi_so2", so2_aqi)
        .withColumn("aqi_co", co_aqi)
    )

    # Compute overall AQI as the max of sub-indices, then defensively cap to 500
    aqi_overall = F.greatest("aqi_pm25", "aqi_pm10", "aqi_o3", "aqi_no2", "aqi_so2", "aqi_co")
    aqi_overall = F.when(aqi_overall.isNull(), None).otherwise(F.least(aqi_overall, F.lit(500)))

    categories = (
        F.when(aqi_overall.isNull(), None)
        .when(aqi_overall <= 50, "Good")
        .when(aqi_overall <= 100, "Moderate")
        .when(aqi_overall <= 150, "Unhealthy for Sensitive Groups")
        .when(aqi_overall <= 200, "Unhealthy")
        .when(aqi_overall <= 300, "Very Unhealthy")
        .otherwise("Hazardous")
    )

    pollutant_array = F.array(
        F.struct(F.col("aqi_pm25").cast("int").alias("aqi"), F.lit("pm25").alias("pollutant")),
        F.struct(F.col("aqi_pm10").cast("int").alias("aqi"), F.lit("pm10").alias("pollutant")),
        F.struct(F.col("aqi_o3").cast("int").alias("aqi"), F.lit("o3").alias("pollutant")),
        F.struct(F.col("aqi_no2").cast("int").alias("aqi"), F.lit("no2").alias("pollutant")),
        F.struct(F.col("aqi_so2").cast("int").alias("aqi"), F.lit("so2").alias("pollutant")),
        F.struct(F.col("aqi_co").cast("int").alias("aqi"), F.lit("co").alias("pollutant")),
    )

    df = df.withColumn("pollutant_scores", pollutant_array)
    df = df.withColumn("valid_scores", F.expr("filter(pollutant_scores, x -> x.aqi IS NOT NULL)"))
    df = df.withColumn(
        "dominant_pollutant",
        F.expr(
            "CASE WHEN size(valid_scores) = 0 THEN NULL "
            "ELSE array_max(valid_scores).pollutant END"
        ),
    )

    df = (
        df
        .withColumn("aqi", aqi_overall.cast("int"))
        .withColumn("category", categories)
        .withColumn("calc_method", F.lit(calc_method))
        .withColumn("run_id", F.lit(run_id))
        .withColumn("computed_at", F.current_timestamp())
        .withColumn("date_utc", F.to_date("ts_utc"))
    )

    select_cols = [
        "location_id",
        "ts_utc",
        "date_utc",
        "aqi",
        "category",
        "dominant_pollutant",
        "aqi_pm25",
        "aqi_pm10",
        "aqi_o3",
        "aqi_no2",
        "aqi_so2",
        "aqi_co",
        "calc_method",
        "run_id",
        "computed_at",
    ]

    return df.select(*select_cols)


def delete_existing(spark, start_ts: datetime, end_ts: datetime, location_ids: Iterable[str]) -> None:
    predicates = [
        f"ts_utc >= TIMESTAMP '{start_ts.strftime('%Y-%m-%d %H:%M:%S')}'",
        f"ts_utc <= TIMESTAMP '{end_ts.strftime('%Y-%m-%d %H:%M:%S')}'",
    ]
    if location_ids:
        ids = ",".join([f"'{lid}'" for lid in location_ids])
        predicates.append(f"location_id IN ({ids})")
    condition = " AND ".join(predicates)
    logging.info("Deleting existing index rows where %s", condition)
    spark.sql(f"DELETE FROM {INDEX_TABLE} WHERE {condition}")


def write_to_table(spark, df: DataFrame) -> None:
    temp_view = f"staging_index_{uuid.uuid4().hex}"
    df.createOrReplaceTempView(temp_view)
    try:
        spark.sql(
            f"""
            MERGE INTO {INDEX_TABLE} t
            USING {temp_view} s
            ON t.location_id = s.location_id AND t.ts_utc = s.ts_utc
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )
    finally:
        spark.catalog.dropTempView(temp_view)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    args = parse_args()

    start_ts = parse_iso_ts(args.start)
    end_ts = parse_iso_ts(args.end)
    if start_ts > end_ts:
        raise SystemExit("--start must be <= --end")

    spark = build("silver_index_hourly")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        ensure_table(spark)
        df_components = load_components(spark, start_ts, end_ts, args.location_ids)
        if df_components.rdd.isEmpty():
            logging.info("No component rows found in requested window; nothing to compute")
            return

        run_id = str(uuid.uuid4())
        df_index = compute_indices(df_components, args.calc_method, run_id)

        if df_index.rdd.isEmpty():
            logging.info("No AQI rows computed; nothing to write")
            return

        if args.mode == "replace":
            delete_existing(spark, start_ts, end_ts, args.location_ids)

        write_to_table(spark, df_index)

        spark.sql(
            f"""
            SELECT location_id,
                   MIN(ts_utc) AS min_ts,
                   MAX(ts_utc) AS max_ts,
                   COUNT(*)    AS rows
            FROM {INDEX_TABLE}
            WHERE ts_utc BETWEEN TIMESTAMP '{start_ts.strftime('%Y-%m-%d %H:%M:%S')}'
                             AND TIMESTAMP '{end_ts.strftime('%Y-%m-%d %H:%M:%S')}'
            GROUP BY location_id
            ORDER BY location_id
            """
        ).show(truncate=False)

        print(f"RUN_ID={run_id}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
