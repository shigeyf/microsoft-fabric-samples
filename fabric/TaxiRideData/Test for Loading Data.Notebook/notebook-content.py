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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Load Data

# Yellow Taxi Trip Record Data
# https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

# Gree Taxi Trip Record Data
# https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf


from pyspark.sql.functions import col, expr, sum, month, count, year, dayofmonth, hour, minute, dayofweek, unix_timestamp, avg, stddev

folder_prefix = 'Files/raw'

df = spark.read.parquet(f"{folder_prefix}/2024/01/green_tripdata_2024-01.parquet")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# |-- VendorID: long (nullable = true):
#   A code indicating the LPEP provider that provided the record.
#     1 = Creative Mobile Technologies, LLC 
#     2 = Curb Mobility, LLC 
#     6 = Myle Technologies Inc 

# |-- lpep_pickup_datetime: timestamp_ntz (nullable = true)
#   The date and time when the meter was engaged. 

# |-- lpep_dropoff_datetime: timestamp_ntz (nullable = true)
#   lpep_dropoff_datetime The date and time when the meter was disengaged.  

# |-- store_and_fwd_flag: string (nullable = true)
#   This flag indicates whether the trip record was held in vehicle memory
#    before sending to the vendor, aka “store and forward,”
#    because the vehicle did not have a connection to the server. 

# |-- RatecodeID: double (nullable = true)
#   The final rate code in effect at the end of the trip. 
#    1 = Standard rate 
#    2 = JFK 
#    3 = Newark 
#    4 = Nassau or Westchester 
#    5 = Negotiated fare 
#    6 = Group ride 
#    99 = Null/unknown 

# |-- PULocationID: long (nullable = true)
#   TLC Taxi Zone in which the taximeter was engaged. 

# |-- DOLocationID: long (nullable = true)
#   TLC Taxi Zone in which the taximeter was disengaged. 

# |-- passenger_count: double (nullable = true)
#   The number of passengers in the vehicle.  

# |-- trip_distance: double (nullable = true)
#   The elapsed trip distance in miles reported by the taximeter. 

# |-- fare_amount: double (nullable = true)
#   The time-and-distance fare calculated by the meter.
#   For additional information on the following columns, see https://www.nyc.gov/site/tlc/passengers/taxi-fare.page 

# |-- extra: double (nullable = true)
#   Miscellaneous extras and surcharges. 

# |-- mta_tax: double (nullable = true)
#   Tax that is automatically triggered based on the metered rate in use. 

# |-- tip_amount: double (nullable = true)
#   Tip amount – This field is automatically populated for credit card tips. Cash tips are not included. 

# |-- tolls_amount: double (nullable = true)
#   Total amount of all tolls paid in trip. 

# |-- ehail_fee: double (nullable = true)
# |-- improvement_surcharge: double (nullable = true)
#   Improvement surcharge assessed trips at the flag drop. The improvement surcharge began being levied in 2015. 

# |-- total_amount: double (nullable = true)
#   The total amount charged to passengers. Does not include cash tips. 

# |-- payment_type: double (nullable = true)
#   A numeric code signifying how the passenger paid for the trip. 
#    0 = Flex Fare trip 
#    1 = Credit card 
#    2 = Cash 
#    3 = No charge 
#    4 = Dispute 
#    5 = Unknown 
#    6 = Voided trip 

# |-- trip_type: double (nullable = true)
#   A code indicating whether the trip was a street-hail or a dispatch that is automatically assigned based on the metered rate in use but can be altered by the driver.  
#    1 = Street-hail 
#    2 = Dispatch

# |-- congestion_surcharge: double (nullable = true)
#   Total amount collected in trip for NYS congestion surcharge. 

df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Drop columns

columns_to_drop = [
    "VendorID",
    "store_and_fwd_flag",
    "RateCodeID",
    "extra",
    "mta_tax",
    "tolls_amount", 
    "ehail_fee",
    "improvement_surcharge",
    "fare_amount",
    "tip_amount",
    "trip_type", 
    "congestion_surcharge"
]

df = df.drop(*columns_to_drop).dropna()
print(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
