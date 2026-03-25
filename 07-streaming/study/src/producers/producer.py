#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from models import Ride, ride_serializer, ride_from_row
from kafka import KafkaProducer
import time

# Load parquet data into Pandas dataframe
url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet'

columns = ['PULocationID', 'DOLocationID', 'trip_distance', 'total_amount', 'tpep_pickup_datetime']
df = pd.read_parquet(url, columns=columns).head(1000)

# start Kafka producer, connect to Kafka broker
server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=ride_serializer
)

# send all dataframe rows by Kafka producer to new topic
# calculate and print time of total send job
topic_name = 'rides'

t0 = time.time()

for _, row in df.iterrows():
    ride = ride_from_row(row)
    producer.send(topic_name, value=ride)
    print(f'Sent: {ride}')
    time.sleep(0.01)

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')
