#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from src.models import Ride1, ride_serializer, ride_from_row1
from kafka import KafkaProducer
import time

# Load parquet data into Pandas dataframe
url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet'

columns = ['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount', 'total_amount']
df = pd.read_parquet(url, columns=columns)

print(f'Rows: {len(df)}, Columns: {len(df.columns)}')

# --- NEW DATA CLEANING LOGIC ---
# 1. Filter out wild outliers: keep only year 2025, month 10
start_date = '2025-10-01'
end_date = '2025-10-31'
df = df[(df['lpep_pickup_datetime'] >= start_date) & (df['lpep_pickup_datetime'] < end_date)]

# 2. Sort the data chronologically so Flink's watermark moves smoothly
df = df.sort_values('lpep_pickup_datetime')

print(f'Cleaned Rows: {len(df)}')


# start Kafka producer, connect to Kafka broker
server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=ride_serializer
)

# send all dataframe rows by Kafka producer to new topic
# calculate and print time of total send job
topic_name = 'green-trips'

t0 = time.time()

for _, row in df.iterrows():
    ride = ride_from_row1(row)
    producer.send(topic_name, value=ride)
    # print(f'Sent: {ride}')
    time.sleep(0.001)

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')
