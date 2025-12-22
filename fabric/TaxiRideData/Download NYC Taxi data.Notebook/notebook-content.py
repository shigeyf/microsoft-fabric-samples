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

# Create a folder for raw data

folder_prefix = 'Files/raw'
mssparkutils.fs.mkdirs(folder_prefix)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Download NYC Taxi Trip Record Data
# https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

import requests

#years = [2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,2023,2024,2025]
#kinds = ['yellow', 'green', 'fhv', 'flvhv']

years = [2015,2016,2017,2018,2019,2020,2021,2022,2023,2024,2025]
kinds = ['green']

for year in years:
    for m in range(0, 12):
        month = m + 1
        month_str = f"{month:02d}"
        folder_path = f"{folder_prefix}/{year}/{month_str}"
        mssparkutils.fs.mkdirs(folder_path)
        
        for kind in kinds:        
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{kind}_tripdata_{year}-{month_str}.parquet"
            response = requests.get(url)

            if response.status_code == 200:
                data_path = f"/lakehouse/default/{folder_path}"
                with open(data_path + f"/{kind}_tripdata_{year}-{month_str}.parquet", 'wb') as file:
                    file.write(response.content)
                    print(f"Downloaded and saved: {data_path}/{kind}_tripdata_{year}-{month_str}.parquet")
            else:
                print(f"Failed to download: {url}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Clear raw data
#mssparkutils.fs.rm(folder_prefix, True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
