from pyspark.sql.functions import col, unix_timestamp
from pyspark.sql import DataFrame


def clean_data(df: DataFrame) -> DataFrame:

    df_clean = (
        df
        .filter(col("trip_distance") > 0)
        .filter((col("fare_amount") > 0) & (col("fare_amount") < 500))
        .filter((col("passenger_count") >= 1) & (col("passenger_count") <= 6))
        .filter(col("tip_amount") >= 0)
        .filter(col("tpep_pickup_datetime").isNotNull())
        .filter(col("tpep_dropoff_datetime").isNotNull())
        .filter(col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))
        .withColumn(
            "trip_duration_minutes",
            (unix_timestamp(col("tpep_dropoff_datetime")) -
            unix_timestamp(col("tpep_pickup_datetime"))) / 60
        )
        .filter((col("trip_duration_minutes") > 0) & (col("trip_duration_minutes") < 180))
    )
    return df_clean





# # ====== Part 2 — Filtering & Cleaning ======
# # Task 2.1 — Remove invalid records
# df_clean = (
#     df
#     .filter(col("trip_distance") > 0)
#     .filter((col("fare_amount") > 0) & (col("fare_amount") < 500))
#     .filter((col("passenger_count") >= 1) & (col("passenger_count") <= 6))
#     .filter(col("tip_amount") >= 0)
#     .filter(col("tpep_pickup_datetime").isNotNull())
#     .filter(col("tpep_dropoff_datetime").isNotNull())
#     .filter(col("tpep_dropoff_datetime") > col("tpep_pickup_datetime")))

# # before_rows = df.count()
# # after_rows = df_clean.count()
# # removed = before_rows - after_rows
# # percentage = (removed / before_rows) * 100
# # print(f"{removed} rows were removed.\nrepresenting {round(percentage, 2)}% of the dataset")
# # After applying the filters, 3,504,320 rows were removed, representing 3.46% of the dataset.

# # Task 2.2 — Compute trip_duration_minutes
# df_clean = df_clean.withColumn(
#     "trip_duration_minutes",(
#         unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60
#         )
# df_clean = df_clean.filter((col("trip_duration_minutes") > 0) & (col("trip_duration_minutes") < 180))
# df_clean.coalesce(3).write.mode("overwrite").parquet("data/cleaned_trips")

# df = spark.read.parquet("data/cleaned_trips")
# df.select("trip_distance").count()
# # df_clean.show()
# # // We do it this way because first we create a new column, then compute the duration in minutes, and only after that we can apply filtering. 
# # //Column does not exist → AnalysisException. 