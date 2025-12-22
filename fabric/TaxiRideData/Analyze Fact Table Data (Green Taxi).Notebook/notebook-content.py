# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6ba1b9bd-a790-4853-b698-83020b2054a1",
# META       "default_lakehouse_name": "LH_NYC_TAXI_FOR_AI",
# META       "default_lakehouse_workspace_id": "e6e5cd6b-89e0-47a2-a424-00fe0a5be641",
# META       "known_lakehouses": [
# META         {
# META           "id": "6ba1b9bd-a790-4853-b698-83020b2054a1"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Setup DataFrames

from pyspark.sql.functions import col, sum, mean, stddev, min, max, count, dayofweek, hour
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pandas as pd
import matplotlib.pyplot as plt

fact_lakehouse = 'LH_NYC_TAXI_FOR_AI'

df              = spark.sql(f"SELECT * FROM {fact_lakehouse}.fact_green_tripdata")
dim_weekday     = spark.sql(f"SELECT * FROM {fact_lakehouse}.dim_weekday")
dim_paymenttype = spark.sql(f"SELECT * FROM {fact_lakehouse}.dim_paymenttype")
dim_location    = spark.sql(f"SELECT * FROM {fact_lakehouse}.dim_location")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": false
# META }

# CELL ********************

#display(df.describe())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": false
# META }

# CELL ********************

# Analysis - Monthly Passengers Analysis

monthly_passenger_count = df \
    .groupBy("pickup_year", "pickup_month") \
    .agg(sum("passenger_count").alias("total_passenger_count")) \
    .orderBy("pickup_year", "pickup_month")

monthly_passenger_count_pd = monthly_passenger_count.toPandas()

monthly_passenger_count_pd['year_month'] = monthly_passenger_count_pd['pickup_year'].astype(str) + '-' + monthly_passenger_count_pd['pickup_month'].astype(str).str.zfill(2)
monthly_passenger_count_pd = monthly_passenger_count_pd.sort_values('year_month')
#display(monthly_passenger_count_pd)

plt.figure(figsize=(24, 12))
plt.plot(monthly_passenger_count_pd['year_month'], monthly_passenger_count_pd['total_passenger_count'], marker='o')
plt.xticks(rotation=90)
plt.xlabel('Year-Month')
plt.ylabel('Total Passenger Count')
plt.title('Monthly Passenger Count')
plt.grid(True)
plt.tight_layout()
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# Analysis - Hourly Passengers Analysis

hourly_passenger_count = df \
    .groupBy("pickup_hour") \
    .agg(sum("passenger_count").alias("total_passenger_count")) \
    .orderBy("pickup_hour")
hourly_passenger_count_pd = hourly_passenger_count.toPandas()

plt.figure(figsize=(10, 6))
plt.bar(hourly_passenger_count_pd['pickup_hour'], hourly_passenger_count_pd['total_passenger_count'])
# params: color=plt.cm.rainbow(hourly_passenger_count_pd['pickup_hour'] / hourly_passenger_count_pd['pickup_hour'].max()
plt.xticks(hourly_passenger_count_pd['pickup_hour'])
plt.xlabel('Hour of Day')
plt.ylabel('Total Passenger Count')
plt.title('Hourly Passenger Count')
plt.grid(True)
plt.tight_layout()
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Analysis - Trip Duration Analysis

trip_duration = df.select("trip_duration") #.filter((col("trip_duration").between(0, 200)))
trip_duration_pd = trip_duration.toPandas()

trip_duration_stats = df.select(mean("trip_duration").alias("mean")).collect()
mean_duration = trip_duration_stats[0]['mean']

plt.figure(figsize=(10, 6))
plt.hist(trip_duration_pd['trip_duration'], bins=50, color='blue', edgecolor='black', alpha=0.7)

plt.axvline(mean_duration, color='r', linestyle='dashed', linewidth=1, label=f'Mean: {mean_duration:.2f} minutes')
plt.text(mean_duration, plt.ylim()[1]*0.9, f'Mean: {mean_duration:.2f} minutes', color='r', ha='center', fontsize=12)

plt.xlabel('Trip Duration (minutes)', fontsize=14)
plt.ylabel('Frequency', fontsize=14)
plt.title('Distribution of Trip Duration', fontsize=16)
plt.legend(fontsize=12)
plt.grid(True)
plt.tight_layout()
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Analysis - Trip Distance Analysis

df_m = df.withColumn("trip_distance_meters", col("trip_distance") * 1609.34)

trip_distance_df = df_m.select("trip_distance_meters") #.filter((col("trip_distance_meters").between(0, 20000)))
trip_distance_pd = trip_distance_df.toPandas()

trip_distance_stats = df_m.select(mean("trip_distance_meters").alias("mean")).collect()
mean_value = trip_distance_stats[0]['mean']

plt.figure(figsize=(10, 6))
plt.hist(trip_distance_pd['trip_distance_meters'], bins=50, color='blue', edgecolor='black', alpha=0.7)

plt.axvline(mean_value, color='r', linestyle='dashed', linewidth=1, label=f'Mean: {mean_value:.2f} meters')
plt.text(mean_value, plt.ylim()[1]*0.9, f'Mean: {mean_value:.2f} meters', color='r', ha='center', fontsize=12)

plt.xlabel('Trip Distance (meters)', fontsize=14)
plt.ylabel('Frequency', fontsize=14)
plt.title('Distribution of Trip Distance (meters)', fontsize=16)
plt.legend(fontsize=12)
plt.grid(True)
plt.tight_layout()
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
