# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b2ac112b-1878-4c11-9f2c-fc3a6ecc00cb",
# META       "default_lakehouse_name": "LH_NYC_TAXI_RAW",
# META       "default_lakehouse_workspace_id": "e6e5cd6b-89e0-47a2-a424-00fe0a5be641",
# META       "known_lakehouses": [
# META         {
# META           "id": "b2ac112b-1878-4c11-9f2c-fc3a6ecc00cb"
# META         },
# META         {
# META           "id": "26bb2637-27a1-45e4-9fa9-8b2e53935e88"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Cleansing Green Taxi Data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": false
# META }

# CELL ********************

# get_raw_data_files function

def get_raw_data_files():
    raw_data_prefix = 'Files/raw'
    raw_data_files = []
    try:
        for year in range(2015, 2025):
            for mon in range(0, 12):
                month = mon + 1
                month_str = f"{month:02d}"
                raw_data_files.append((f"{year}", month_str, f"{raw_data_prefix}/{year}/{month_str}/green_tripdata_{year}-{month_str}.parquet"))
        print("Raw Data File paths created successfully.")
    except Exception as e:
        print(f"Error creating file paths: {e}")
    
    return raw_data_files


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": false
# META }

# CELL ********************

# clearn_dataframe function

from pyspark.sql.functions import col, expr, sum, month, count, year, dayofmonth, hour, minute, dayofweek, unix_timestamp, avg, stddev  
import pyspark.sql.functions as F

def clean_dataframe(df, year, month):
    try:
        columns_to_drop = [
            "VendorID",
            "store_and_fwd_flag",
            "RateCodeID",
            "extra",
            "mta_tax",
            "ehail_fee",
            "improvement_surcharge",
            "trip_type", 
            "congestion_surcharge"
        ]
        df = df.drop(*columns_to_drop).dropna()

        # Renaming & Retyping fields
        df = df.select(
            col("lpep_pickup_datetime").cast("timestamp").alias("pickup_datetime"),
            col("lpep_dropoff_datetime").cast("timestamp").alias("dropoff_datetime"),
            col("PULocationID").cast("int").alias("pickup_LocationID"),
            col("DOLocationID").cast("int").alias("dropoff_LocationID"),
            col("payment_type").cast("int").alias("payment_type"),
            col("passenger_count").cast("int").alias("passenger_count"),
            col("trip_distance").cast("double").alias("trip_distance"),
            col("total_amount").cast("double").alias("total_amount"),
            col("fare_amount").cast("double").alias("fare_amount"),
            col("tip_amount").cast("double").alias("tip_amount"),
            col("tolls_amount").cast("double").alias("tolls_amount")
        )

        # Durations
        df = df.withColumn(
            "trip_duration", 
            (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60
        )
        
        # Speed
        df = df.withColumn(
            "speed_mph", 
            col("trip_distance") / (col("trip_duration") / 60)
        )

        # Cleaning data
        df = df.filter(
            # Data validations
            (col("pickup_LocationID").between(1, 265)) &
            (col("dropoff_LocationID").between(1, 265)) &
            (col("payment_type").isin([1, 2])) &
            (col("passenger_count").between(1, 5)) &
            (col("total_amount") > 0) &
            (col("fare_amount") > 0) &
            (col("tip_amount") >= 0) &
            (col("tolls_amount") >= 0) &
            (col("trip_distance") > 0) &
            (col("trip_duration") > 0) &
            ((col("dropoff_datetime")) > (col("pickup_datetime"))) &
            # Best practices
            (col("trip_distance") < 50) &       # within 50 miles
            (col("trip_duration") < 3 * 60) &   # within 3 hours
            (col("fare_amount") < 300) &        # within 300 dollars
            (col("speed_mph") > 1) &
            (col("speed_mph") < 80)
        )

        df = df.withColumn("pickup_year", F.year(col("pickup_datetime")))
        df = df.withColumn("pickup_month", F.month(col("pickup_datetime")))
        df = df.withColumn("pickup_day", F.dayofmonth(col("pickup_datetime")))
        df = df.withColumn("pickup_hour", F.hour(col("pickup_datetime")))
        df = df.withColumn("pickup_minute", F.minute(col("pickup_datetime")))
        df = df.withColumn("pickup_weekday", F.dayofweek(col("pickup_datetime")))
        df = df.filter(
            (col("pickup_year") == year) &
            (col("pickup_month") == month)
        )
        #df = df.drop("dropoff_datetime")

        df = df.selectExpr(
            "cast(pickup_datetime as timestamp)",
            "cast(dropoff_datetime as timestamp)",
            "cast(pickup_LocationID as int)",
            "cast(dropoff_LocationID as int)",
            "cast(payment_type as int)",
            "cast(passenger_count as int)",
            "cast(trip_distance as double)",
            "cast(trip_duration as double)",
            "cast(total_amount as double)",
            "cast(pickup_year as int)",
            "cast(pickup_month as int)",
            "cast(pickup_day as int)",
            "cast(pickup_hour as int)",
            "cast(pickup_minute as int)",
            "cast(pickup_weekday as int)",
            "cast(speed_mph as double)"
        )

        return df
    except Exception as e:
        print(f"Error processing dataframe: {e}")
        return None


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": false
# META }

# CELL ********************

# Populate Green Taxi Trip Record Data

cleansing_lakehouse = 'LH_NYC_TAXI_CLEANSING'

for year, month, raw_data_file in get_raw_data_files():
    try:
        df = spark.read.parquet(raw_data_file)
        clean_df = clean_dataframe(df, int(year), int(month))

        name = ((raw_data_file.split('/'))[-1].split('.'))[0].replace('-', '_')
        if clean_df is not None:
            #display(clean_df.describe())
            table_name = f"{cleansing_lakehouse}.cleaned_{name}"
            clean_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(table_name)
            print(f"Processed and saved data for cleaned_{name}")
        else:
            print(f"Skipping save for cleaned_green_{year}_{month} due to processing error.")

    except Exception as e:
        print(f"Error processing file {raw_data_file}: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": false
# META }

# CELL ********************

# Drop created cleansing tables

spark.sql("USE LH_NYC_TAXI_CLEANSING")
tables = spark.sql("SHOW TABLES").select("tableName").rdd.flatMap(lambda x: x).collect()
for table_name in tables:
    print(f"Dropping {table_name}")
    #spark.sql(f"DROP TABLE IF EXISTS {table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }
