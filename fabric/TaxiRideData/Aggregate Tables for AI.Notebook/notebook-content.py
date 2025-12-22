# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "26bb2637-27a1-45e4-9fa9-8b2e53935e88",
# META       "default_lakehouse_name": "LH_NYC_TAXI_CLEANSING",
# META       "default_lakehouse_workspace_id": "e6e5cd6b-89e0-47a2-a424-00fe0a5be641",
# META       "known_lakehouses": [
# META         {
# META           "id": "26bb2637-27a1-45e4-9fa9-8b2e53935e88"
# META         },
# META         {
# META           "id": "6ba1b9bd-a790-4853-b698-83020b2054a1"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# List Cleansing tables

cleansing_lakehouse = 'LH_NYC_TAXI_CLEANSING'
fact_lakehouse = 'LH_NYC_TAXI_FOR_AI'

spark.sql(f"USE {cleansing_lakehouse}")
tables = spark.sql("SHOW TABLES").select("tableName").rdd.flatMap(lambda x: x).collect()
print("List of tables in LH_NYC_TAXI_CLEANSING:")
for table_name in tables:
    print(table_name)
    #spark.sql(f"DROP TABLE IF EXISTS {table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": false
# META }

# CELL ********************

import requests
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

csv_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
response = requests.get(csv_url)

location_csv = 'Files/taxi_zone_lookup.csv'
with open(f"/lakehouse/default/{location_csv}", 'wb') as file:
    file.write(response.content)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": false
# META }

# CELL ********************

# Create Dimension Table - Weekday

spark.sql(f"DROP TABLE IF EXISTS {fact_lakehouse}.dim_weekday")

data = [
    (1, "Sunday"),
    (2, "Monday"),
    (3, "Tuesday"),
    (4, "Wednesday"),
    (5, "Thursday"),
    (6, "Friday"),
    (7, "Saturday")
]

schema = StructType([
    StructField("pickup_weekday", IntegerType(), True),
    StructField("description", StringType(), True)
])

dim_weekday_df = spark.createDataFrame(data, schema)
dim_weekday_df.write.mode("overwrite").saveAsTable(f"{fact_lakehouse}.dim_weekday")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": false
# META }

# CELL ********************

# Create Dimension Table - PaymentType

spark.sql(f"DROP TABLE IF EXISTS {fact_lakehouse}.dim_paymenttype")

data = [
    (1, "Credit card"),
    (2, "Cash"),
    #(3, "No charge"),
    #(4, "Dispute"),
    #(5, "Unknown"),
    #(6, "Voided trip")
]

schema = StructType([
    StructField("payment_type", IntegerType(), True),
    StructField("description", StringType(), True)
])

dim_paymenttypedf = spark.createDataFrame(data, schema)
dim_paymenttypedf.write.mode("overwrite").saveAsTable(f"{fact_lakehouse}.dim_paymenttype")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": false
# META }

# CELL ********************

# Create Dimention Table - Location

schema = StructType([
    StructField("LocationID", IntegerType(), True),
    StructField("Borough", StringType(), True),
    StructField("Zone", StringType(), True),
    StructField("service_zone", StringType(), True)
])

dim_location = spark.read.format("csv").schema(schema).option("header", "true").load(f"{location_csv}")
dim_location.write.format("delta").mode("overwrite").saveAsTable(f"{fact_lakehouse}.dim_location")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": false
# META }

# CELL ********************

# Show Dimention data

display(spark.sql(f"SELECT * FROM {fact_lakehouse}.dim_weekday ORDER BY pickup_weekday ASC"))
display(spark.sql(f"SELECT * FROM {fact_lakehouse}.dim_paymenttype ORDER BY payment_type ASC"))
display(spark.sql(f"SELECT * FROM {fact_lakehouse}.dim_location ORDER BY LocationID ASC"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": false
# META }

# CELL ********************

# Aggregate tables into Dataframe of Fact table 

spark.sql("USE LH_NYC_TAXI_CLEANSING")
tables = spark.sql("SHOW TABLES").select("tableName").rdd.flatMap(lambda x: x).collect()

fact_tripdata_df = None

for table_name in tables:
    df = spark.table(table_name)
    if fact_tripdata_df is None:
        fact_tripdata_df = df
    else:
        fact_tripdata_df = fact_tripdata_df.union(df)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": false
# META }

# CELL ********************

# Write Dataframe to Fact table 

fact_tripdata_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fact_lakehouse}.fact_green_tripdata")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": false
# META }

# CELL ********************

# Display Fact Table describe()

display(fact_tripdata_df.describe())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": false
# META }

# CELL ********************

# Verify Fact Table - Data count

fact_lakehouse = 'LH_NYC_TAXI_FOR_AI'
df = spark.sql(f"SELECT * FROM {fact_lakehouse}.fact_green_tripdata")
print(f"Fact Table: count = {df.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": false
# META }

# CELL ********************

# Verify Cleansing Tables - Data count

cleansing_lakehouse = 'LH_NYC_TAXI_CLEANSING'

spark.sql(f"USE {cleansing_lakehouse}")
tables = spark.sql("SHOW TABLES").select("tableName").rdd.flatMap(lambda x: x).collect()

total = 0
for table_name in tables:
    df = spark.sql(f"SELECT * FROM {cleansing_lakehouse}.{table_name}")
    total = total + df.count()
    #print(f"Cleansing Table [{table_name}]: count = {df.count()}")

print(f"Cleansing Table Total: count = {total}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": false
# META }
