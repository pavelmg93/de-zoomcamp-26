#!/usr/bin/env python
# coding: utf-8

from kafka import KafkaConsumer
from src.models import Ride, ride_deserializer
from datetime import datetime
import psycopg2

# define Kafka broker connection and topic
server = 'localhost:9092'
topic_name = 'green-trips'

# start and connect Kafka consumer to Kafka broker, subscribe to topic
# use our model for data conversion
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    enable_auto_commit=False, # for reruns
    group_id='rides-console',
    value_deserializer=ride_deserializer
)

# consume all messages from topic

# for record in consumer:
#     print(record.value)

print(f"Listening to {topic_name}...")

# connect to db

conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='postgres',
    user='postgres',
    password='postgres'
)
conn.autocommit = True
cur = conn.cursor()

# write all consumed events to db

count = 0
for message in consumer:
    ride = message.value
    pickup_dt = datetime.fromtimestamp(ride.lpep_pickup_datetime / 1000)
    dropoff_dt = datetime.fromtimestamp(ride.lpep_dropoff_datetime / 1000)
    cur.execute(
        """INSERT INTO processed_events
        'lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount', 'total_amount'
           (PULocationID, DOLocationID, trip_distance, total_amount, pickup_datetime)
           VALUES (%s, %s, %s, %s, %s)""",
        (ride.PULocationID, ride.DOLocationID,
         ride.trip_distance, ride.total_amount, pickup_dt)
    )
    count += 1
    if count % 100 == 0:
        print(f"Inserted {count} rows...")

consumer.close()
cur.close()
conn.close()