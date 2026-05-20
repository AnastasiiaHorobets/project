from pyspark.sql.functions import (
    col,
    hour,
    when,
    round,
    count,
    avg,
    percentile_approx,
    sum,
    desc,
    to_date,
    dense_rank,
)
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# Task 3.1 — time_of_day category
def add_time_of_day(df: DataFrame) -> DataFrame:
    hour_col = hour(col("tpep_pickup_datetime"))

    time_of_day_expr = (
        when(hour_col.between(0, 5), "night")
        .when(hour_col.between(6, 11), "morning")
        .when(hour_col.between(12, 17), "afternoon")
        .otherwise("evening")
    )
    return df.withColumn("time_of_day", time_of_day_expr)


# Task 3.2 — speed_mph and suspicious flag
def add_speed_mph(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "speed_mph",
        when(
            col("trip_duration_minutes") > 0,
            (col("trip_distance") * 60) / col("trip_duration_minutes"),
        ),
    )

    df = df.withColumn(
        "is_suspicious_speed", when((col("speed_mph") > 80), True).otherwise(False)
    )
    return df


# Task 3.3 — tip_pct
def add_tip_pct(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "tip_pct",
        when(
            (col("fare_amount") > 0),
            round(((col("tip_amount") / col("fare_amount")) * 100), 2),
        ).otherwise(None),
    )
    return df


# Task 3.4 — payment_type label
def add_payment_type_label(df: DataFrame) -> DataFrame:
    code_label = (
        when(col("payment_type") == 1, "Credit Card")
        .when(col("payment_type") == 2, "Cash")
        .when(col("payment_type") == 3, "No Charge")
        .when(col("payment_type") == 4, "Dispute")
        .when(col("payment_type") == 5, "Unknown")
        .when(col("payment_type") == 6, "Voided Trip")
        .otherwise("Unknown")
    )
    df = df.withColumn("payment_type_label", code_label)
    return df


def add_pickup_date(df: DataFrame) -> DataFrame:
    df = df.withColumn("pickup_date", (to_date(col("tpep_pickup_datetime"))))
    return df


def add_pickup_hour(df: DataFrame) -> DataFrame:
    df = df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
    return df


# Part 4 — Aggregations & Joins
def hourly_trip_analysis(df: DataFrame) -> DataFrame:
    df = (
        df.groupBy("pickup_hour")
        .agg(
            count("*").alias("total_trips"),
            round(avg(col("fare_amount")), 2).alias("avg_fare"),
            round(avg(col("tip_pct")), 2).alias("avg_tip_pct"),
            round(avg("trip_duration_minutes"), 2).alias("avg_duration_minutes"),
        )
        .orderBy("pickup_hour")
    )
    return df


# Task 4.2 — Tip behavior by payment type
def tip_by_payment_type(df: DataFrame) -> DataFrame:
    df = df.groupBy("payment_type_label").agg(
        count("*").alias("total_trips"),
        round(avg(col("tip_pct")), 2).alias("avg_tip_pct"),
        percentile_approx(col("tip_pct"), 0.5).alias("median_tip_pct"),
    )
    return df


# Task 4.3 — Top 10 pickup locations by revenue
def top_pickup_locations_by_revenue(df: DataFrame) -> DataFrame:
    # df must contain 'pickup_hour' column
    df = (
        df.groupBy("PULocationID")
        .agg(sum("fare_amount").alias("total_revenue"))
        .orderBy(col("total_revenue").desc())
        .limit(10)
    )
    return df


def create_zone_lookup(spark):
    data = [
        (132, "JFK Airport"),
        (138, "LaGuardia Airport"),
        (161, "Midtown Center"),
        (237, "Upper East Side South"),
        (236, "Upper East Side North"),
        (186, "Penn Station / Madison Sq West"),
    ]
    schema = StructType(
        [
            StructField("PULocationID", IntegerType(), True),
            StructField("zone_name", StringType(), True),
        ]
    )
    zone_lookup_df = spark.createDataFrame(data, schema=schema)
    return zone_lookup_df


# Task 5.1 — Running cumulative trip count by day
def cumulative_trips_by_day(df: DataFrame) -> DataFrame:
    df = df.groupBy("pickup_date").agg(count("*").alias("daily_trip_count"))

    window_spec = Window.orderBy("pickup_date").rowsBetween(
        Window.unboundedPreceding, 0
    )

    df = df.withColumn(
        "cumulative_trip_count", sum("daily_trip_count").over(window_spec)
    ).orderBy("pickup_date")
    return df


# Task 5.2 — Rank hours within each day
def rank_hours_by_day(df: DataFrame) -> DataFrame:
    df = df.groupBy("pickup_date", "pickup_hour").agg(count("*").alias("trip_count"))
    window_part = Window.partitionBy("pickup_date").orderBy(desc("trip_count"))
    df = df.withColumn("daily_hour_rank", dense_rank().over(window_part))
    return df
