#!/usr/bin/env python
# coding: utf-8

import argparse
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Define the path to our good Java
java21_path = "/usr/lib/jvm/java-21-openjdk-amd64"

# 2. Set JAVA_HOME
os.environ["JAVA_HOME"] = java21_path

# 3. The Magic: Shove Java 21 to the absolute front of the PATH
os.environ["PATH"] = f"{java21_path}/bin:" + os.environ.get("PATH", "")

parser = argparse.ArgumentParser()

parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output

spark = SparkSession.builder \
    #.master("spark://codespaces-472bba:7077") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .appName('test') \
    .getOrCreate()

# Green Taxi Dataframe

df_green = spark.read \
    .option("recursiveFileLookup", "true") \
    .parquet(input_green)

df_green.show()
df_green.count()

# Yello Taxi dataframe

df_yellow = spark.read \
    .option("recursiveFileLookup", "true") \
    .parquet(input_yellow)

df_yellow.show()
df_yellow.count()

# Building Common Columns

print(df_green.columns)
print(df_yellow.columns)

print (set(df_green.columns) & set(df_yellow.columns))

df_green = df_green \
   .withColumnRenamed('lpep_pickup_datetime', "pickup_datetime") \
   .withColumnRenamed('lpep_dropoff_datetime', "dropoff_datetime")

df_yellow = df_yellow \
   .withColumnRenamed('tpep_pickup_datetime', "pickup_datetime") \
   .withColumnRenamed('tpep_dropoff_datetime', "dropoff_datetime")

print(set(df_green.columns) & set(df_yellow.columns))

common_columns = []

yellow_columns = set(df_yellow.columns)

for col in df_green.columns:
    if col in yellow_columns:
        common_columns.append(col)

print(common_columns)

# Staging Green & Yellow dataframes with common column selects

df_green_sel = df_green \
    .select(common_columns) \
    .withColumn('service_type', F.lit('green'))

df_yellow_sel = df_yellow \
    .select(common_columns) \
    .withColumn('service_type', F.lit('yellow'))

# Combine (Union) Green and Yellow into Trips DataFrame

df_trips_data = df_green_sel.unionAll(df_yellow_sel)
df_trips_data.groupBy('service_type').count().show()
df_trips_data.createOrReplaceTempView('trips_data')

spark.sql("""
SELECT
    service_type,
    COUNT(1)
FROM
    trips_data
GROUP BY
    service_type
""") \
    .show()

df_result = spark.sql("""
SELECT 
    -- Revenue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_monthly_passenger_count,
    AVG(trip_distance) AS avg_monthly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")

df_result.show()

# spark.conf.set("spark.sql.adaptive.enabled", "false")

print(f"Writing results into Parquet files @ {output}")
df_result.write.parquet(output, mode = "overwrite")
# df_result.coalesce(1).write.parquet('data/report/revenue')

# spark.stop()