import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from cleaning import clean_data
from analysis import (
    add_time_of_day,
    add_speed_mph,
    add_tip_pct,
    add_payment_type_label,
    add_pickup_date,
    add_pickup_hour,
    hourly_trip_analysis,
    tip_by_payment_type,
    create_zone_lookup,
    top_pickup_locations_by_revenue,
    cumulative_trips_by_day,
    rank_hours_by_day
)

spark = (
    SparkSession.builder
    .appName("NYC Yellow Taxi Trip Records (2023)")
    .master("local[*]")
    .getOrCreate()
)

if os.path.exists("data/cleaned_trips"):
    df = spark.read.parquet("data/cleaned_trips")
else:
    df_raw = spark.read.csv("data/trips", header=True, inferSchema=True)
    before_rows = df_raw.count()
    df = clean_data(df_raw)
    after_rows = df.count()
    removed = before_rows - after_rows
    percentage = (removed / before_rows) * 100
    print(f"{removed} rows were removed.")
    print(f"representing {round(percentage, 2)}% of the dataset")
    df.coalesce(3).write.mode("overwrite").parquet("data/cleaned_trips")


# # 3760088 rows were removed.                                                      
# # representing 3.71% of the dataset


# Task 3.1 — time_of_day category
df_time_of_day = add_time_of_day(df)
df_time_of_day.groupBy("time_of_day").count().show()


# # # Task 3.2 — speed_mph and suspicious flag
df = add_speed_mph(df)
df.groupBy("is_suspicious_speed").count().show()
# # # High speeds are usually caused by bad timestamps or incorrect distance measurements.(True=77698 > 80, false = 97409011 < 80)


# # Task 3.3 — tip_pct
df = add_tip_pct(df)
df.select("tip_pct").show()


# # Task 3.4 — payment_type label
df = add_payment_type_label(df)
df.groupBy("payment_type_label").count().show()


# # # Part 4 — Aggregations & Joins
df = add_pickup_hour(df)
hourly_df = hourly_trip_analysis(df)
hourly_df.show()
# # # Hour with the highest average fare: Hour 5 has the highest average fare (16.44).
# # # At 5 AM, trips are usually longer and there is less traffic, so the average fare is higher.


# # Task 4.2 — Tip behavior by payment type
payment_df = tip_by_payment_type(df)
payment_df.show(5)
# # # Cash trips show 0% tip because tips are usually given in cash and are not recorded in the dataset. 
# # # This does not mean that cash passengers tip less than card passengers. 
# # # The dataset is missing information about cash tips, so the results are incomplete.


# # Task 4.3 — Top 10 pickup locations by revenue
df = add_pickup_date(df)
top_df = top_pickup_locations_by_revenue(df)
zone_lookup_df = create_zone_lookup(spark)
top_with_zone_df = top_df.join(zone_lookup_df, "PULocationID", "left")
top_with_zone_df.show()
# # # Named zones in top 10:  JFK Airport, LaGuardia Airport, Midtown Center, Penn Station / Madison Sq West, Upper East Side South, Upper East Side North
# # # Locations with no label: 230, 162, 48, 170


# # Task 5.1 — Running cumulative trip count by day
cumulative_trips_by_day_df = cumulative_trips_by_day(df)
cumulative_trips_by_day_df.show(10)


# # Task 5.2 — Rank hours within each day
# # df = add_pickup_date(df)
# # df = add_pickup_hour(df)
rank_df = rank_hours_by_day(df)
rank_df.filter(F.col("daily_hour_rank") == 1).show(10)









# # # ===== Part 1 — Data Exploration =====
# df_raw = spark.read.csv("data/trips", header=True, inferSchema=True)
# # df_raw.printSchema()
# # print(len(df_raw.columns))
# # print(df_raw.count())
# # df_raw.select("trip_distance", "fare_amount", "tip_amount").describe().show()

# # Count NULLs in each column
# null_counts = df_raw.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df_raw.schema.names])
# # # columns_names = df_raw.withColumn("columns_names", F.lit(df_raw.schema.names))
# # # # print(columns_names)
# # # columns_names.select("columns_names").show()

# # # print(df_raw.schema.names)
# null_counts.show(truncate=False)






