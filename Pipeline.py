# Databricks notebook source
dbutils.widgets.text(
  name='json_path',
  defaultValue='/',
  label='json path'
)

print(dbutils.widgets.get("json_path"))

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql.types import DoubleType
import sys

file_path = dbutils.widgets.get("json_path")

# Read data from JSON file
df = spark.read.json(file_path)

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
calculate
