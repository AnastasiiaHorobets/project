<!-- Step 1 -->
<!-- Exploration steps -->

The dataset contains 18 columns, including two timestamp fields for trip start and end times. 
Several numeric columns (e.g., trip_distance, fare_amount, tip_amount) can be used for aggregations. 
Some integer fields such as VendorID and payment_type represent categorical values.

<!-- Step 2 -->
<!-- • Count total rows in the raw file. -->
The dataset contains about 101 milion rows, indicating a large dataset

<!-- Step 3 -->
<!-- • Run a descriptive statistics summary on: trip_distance, fare_amount, tip_amount. Note min/max values. -->
The min values for key columns are negative, fare_amount (-37264.53), trip_distance (-1856.0), and tip_amount (-493.22), which are not realistic
The max values are also extremely high fare_amount = (297004.51), trip_distance (943274.8), tip_amount (141492.02)

<!-- Step 4 -->
<!-- • Count null values per column. Which columns have the most nulls? -->
The column congestion_surcharge has the highest number of null values.
Other columns have some nulls, but several columns have no missing values.





<!-- -- VendorID: integer (nullable = true)
 |-- tpep_pickup_datetime: timestamp (nullable = true)
 |-- tpep_dropoff_datetime: timestamp (nullable = true)
 |-- passenger_count: integer (nullable = true)
 |-- trip_distance: double (nullable = true)
 |-- RatecodeID: integer (nullable = true)
 |-- store_and_fwd_flag: string (nullable = true)
 |-- PULocationID: integer (nullable = true)
 |-- DOLocationID: integer (nullable = true)
 |-- payment_type: integer (nullable = true)
 |-- fare_amount: double (nullable = true)
 |-- extra: double (nullable = true)
 |-- mta_tax: double (nullable = true)
 |-- tip_amount: double (nullable = true)
 |-- tolls_amount: double (nullable = true)
 |-- improvement_surcharge: double (nullable = true)
 |-- total_amount: double (nullable = true)
 |-- congestion_surcharge: double (nullable = true) -->