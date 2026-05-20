# Dataset Exploration

## Dataset Overview

The NYC Yellow Taxi dataset contains **18 columns**, including:

### Timestamp columns
- `tpep_pickup_datetime`
- `tpep_dropoff_datetime`

### Numerical columns
- `trip_distance`
- `fare_amount`
- `tip_amount`
- `total_amount`

These columns can be used for aggregations and statistical analysis.

### Categorical columns
- `VendorID`
- `payment_type`
- `RatecodeID`

---

## Dataset Size

The raw dataset contains approximately:

```text
101 million rows
```

This indicates a large-scale dataset suitable for distributed processing with **PySpark**.

---

## Descriptive Statistics

### Minimum Values

Several numerical columns contain unrealistic negative values.

| Column | Minimum Value |
|---------|---------------|
| fare_amount | -37264.53 |
| trip_distance | -1856.0 |
| tip_amount | -493.22 |

These values likely represent data quality issues or corrupted records.

### Maximum Values

Several numerical columns contain extremely large outliers.

| Column | Maximum Value |
|---------|---------------|
| fare_amount | 297004.51 |
| trip_distance | 943274.8 |
| tip_amount | 141492.02 |

These observations indicate the need for cleaning and filtering rules.

---

## Missing Values Analysis

The column:

```text
congestion_surcharge
```

contains the highest number of null values.

Several other columns contain some missing data, while some columns have no null values.

---

## Dataset Schema

```text
VendorID: integer
tpep_pickup_datetime: timestamp
tpep_dropoff_datetime: timestamp
passenger_count: integer
trip_distance: double
RatecodeID: integer
store_and_fwd_flag: string
PULocationID: integer
DOLocationID: integer
payment_type: integer
fare_amount: double
extra: double
mta_tax: double
tip_amount: double
tolls_amount: double
improvement_surcharge: double
total_amount: double
congestion_surcharge: double
```