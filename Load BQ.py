# Databricks notebook source

#countries = ['GB', 'NL', 'FR']

#for country in countries:
#    spark.sql(f"DROP TABLE IF EXISTS `{country}_rolling_avg`")
#    dbutils.fs.rm(f"dbfs:/rolling_avg_FR/{country}", True)

# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql.types import DoubleType

# Create a Spark session
spark = SparkSession.builder.getOrCreate()

# Read data from JSON files
json_files = ['/FileStore/tables/airquality/1633531868.ndjson', '/FileStore/tables/airquality/1633527666.ndjson', '/FileStore/tables/airquality/1633534868.ndjson', '/FileStore/tables/airquality/1633539068.ndjson', '/FileStore/tables/airquality/1633542067.ndjson', '/FileStore/tables/airquality/1633546872.ndjson' ]
df = spark.read.json(json_files)

# Filter data to only include the measures of interest and the specified sources
df_filtered = df.filter(
  col('parameter').isin(['o3', 'no2', 'co', 'pm10', 'pm25'])
)

# Extract the timestamp field from 'date.local' struct
df_filtered = df_filtered.withColumn('timestamp_field', col('date.utc'))

# Convert the extracted timestamp field to Unix timestamp
df_filtered = df_filtered.withColumn('date_unix', unix_timestamp('timestamp_field', 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\''))


# Define AQI calculation functions for PM2.5 and PM10 using a lookup table
def calculate_aqi(PMobs, PM_breakpoints, AQI_values):
    for i, (low, high) in enumerate(PM_breakpoints):
        if low <= PMobs <= high:
            AQILow, AQIHigh = AQI_values[i]
            BreakpointLow, BreakpointHigh = low, high
            AQI = ((AQIHigh - AQILow) / (BreakpointHigh - BreakpointLow)) * (PMobs - BreakpointLow) + AQILow
            return AQI
    return None

# Define AQI breakpoints lookup tables for PM2.5 and PM10
pm25_breakpoints = [
    (0, 12.0), (12.1, 35.4), (35.5, 55.4), (55.5, 150.4), (150.5, 250.4), (250.5, 350.4), (350.5, 500.4), (500.5, 999999.9)
]

pm10_breakpoints = [
    (0, 54.0), (55.0, 154.0), (155.0, 254), (255.0, 354.0), (355.0, 424.0), (425.0, 504.0), (505.0, 604.0), (604.1, 99999.9)
]

aqi_value = [
    (0, 50), (51,100), (101,150), (151,200), (201,300), (301,400), (401,500), (500,999) 
]


# Define the window
partition = Window.partitionBy('city', 'parameter').orderBy('date_unix').rangeBetween(-86400, 0)

# Calculate the 24-hour rolling average for each city and parameter
df_filtered = df_filtered.withColumn('rolling_avg', avg('value').over(partition))

# Define the UDFs
calculate_pm25_aqi_udf = udf(lambda value: calculate_aqi(value, pm25_breakpoints, aqi_value), DoubleType())
calculate_pm10_aqi_udf = udf(lambda value: calculate_aqi(value, pm10_breakpoints, aqi_value), DoubleType())

# Calculate AQI for PM2.5 and PM10 and create new columns 'pm25_aqi' and 'pm10_aqi'
df_filtered = df_filtered.withColumn(
    'pm25_aqi',
    when(df_filtered['parameter'] == 'pm25', calculate_pm25_aqi_udf(df_filtered['rolling_avg'])).otherwise(None)
)

df_filtered = df_filtered.withColumn(
    'pm10_aqi',
    when(df_filtered['parameter'] == 'pm10', calculate_pm10_aqi_udf(df_filtered['rolling_avg'])).otherwise(None)
)

# List of countries to save as separate tables
countries = ['GB', 'NL', 'FR']

# Save the filtered data as a separate Delta table for each country
for country in countries:
    df_country = df_filtered.filter(col('country') == country)
    
    # Define the Delta table path
    table_path = f"dbfs:/rolling_avg_{country}"
    
    # Write the filtered data to a Delta table and overwrite if it exists
    df_country.write \
        .format("delta") \
        .option("path", table_path) \
        .option("mergeSchema", "true") \
        .mode("overwrite") \
        .saveAsTable(f"{country}_rolling_avg")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.default.fr_rolling_avg

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql.types import DoubleType

# Create a Spark session
spark = SparkSession.builder.getOrCreate()

# Read data from JSON files
json_files = ['/FileStore/tables/airquality/1633531868.ndjson', '/FileStore/tables/airquality/1633527666.ndjson', '/FileStore/tables/airquality/1633534868.ndjson', '/FileStore/tables/airquality/1633539068.ndjson', '/FileStore/tables/airquality/1633542067.ndjson', '/FileStore/tables/airquality/1633546872.ndjson' ]
df = spark.read.json(json_files)

# Filter data to only include the measures of interest and the specified sources
df_filtered = df.filter(
  col('parameter').isin(['o3', 'no2', 'co', 'pm10', 'pm25'])
)

# Extract the timestamp field from 'date.local' struct
df_filtered = df_filtered.withColumn('timestamp_field', col('date.utc'))

# Convert the extracted timestamp field to Unix timestamp
df_filtered = df_filtered.withColumn('date_unix', unix_timestamp('timestamp_field', 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\''))


# Define AQI calculation functions for PM2.5 and PM10 using a lookup table
def calculate_aqi(PMobs, PM_breakpoints, AQI_values):
    for i, (low, high) in enumerate(PM_breakpoints):
        if low <= PMobs <= high:
            AQILow, AQIHigh = AQI_values[i]
            BreakpointLow, BreakpointHigh = low, high
            AQI = ((AQIHigh - AQILow) / (BreakpointHigh - BreakpointLow)) * (PMobs - BreakpointLow) + AQILow
            return AQI
    return None

# Define AQI breakpoints lookup tables for PM2.5 and PM10
pm25_breakpoints = [
    (0, 12.0), (12.1, 35.4), (35.5, 55.4), (55.5, 150.4), (150.5, 250.4), (250.5, 350.4), (350.5, 500.4), (500.5, 999999.9)
]

pm10_breakpoints = [
    (0, 54.0), (55.0, 154.0), (155.0, 254), (255.0, 354.0), (355.0, 424.0), (425.0, 504.0), (505.0, 604.0), (604.1, 99999.9)
]

aqi_value = [
    (0, 50), (51,100), (101,150), (151,200), (201,300), (301,400), (401,500), (500,999) 
]


# Define the window
partition = Window.partitionBy('city', 'parameter').orderBy('date_unix').rangeBetween(-86400, 0)

# Calculate the 24-hour rolling average for each city and parameter
df_filtered = df_filtered.withColumn('rolling_avg', avg('value').over(partition))

# Define the UDFs
calculate_pm25_aqi_udf = udf(lambda value: calculate_aqi(value, pm25_breakpoints, aqi_value), DoubleType())
calculate_pm10_aqi_udf = udf(lambda value: calculate_aqi(value, pm10_breakpoints, aqi_value), DoubleType())

# Calculate AQI for PM2.5 and PM10 and create new columns 'pm25_aqi' and 'pm10_aqi'
df_filtered = df_filtered.withColumn(
    'pm25_aqi',
    when(df_filtered['parameter'] == 'pm25', calculate_pm25_aqi_udf(df_filtered['rolling_avg'])).otherwise(None)
)

df_filtered = df_filtered.withColumn(
    'pm10_aqi',
    when(df_filtered['parameter'] == 'pm10', calculate_pm10_aqi_udf(df_filtered['rolling_avg'])).otherwise(None)
)

# List of countries to save as separate tables
countries = ['GB', 'NL', 'FR']

for country in countries:
    df_country = df_filtered.filter(col('country') == country)
    
    bucket = 'ppbucket01'
    table = "airquality.aqiTable"

    df_country.write.format("bigquery").option("temporaryGcsBucket", bucket).option("table", table).mode("append").save() 

