#!/usr/bin/env python
# coding: utf-8

from kafka import KafkaConsumer
from models import Ride, ride_deserializer
from datetime import datetime

# define Kafka broker connection and topic
server = 'localhost:9092'
topic_name = 'rides'

# start and connect Kafka consumer to Kafka broker, subscribe to topic
# use our model for data conversion
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='rides-console',
    value_deserializer=ride_deserializer
)

# consume all messages from topic

# for record in consumer:
#     print(record.value)

print(f"Listening to {topic_name}...")

count = 0
for message in consumer:
    ride = message.value
    pickup_dt = datetime.fromtimestamp(ride.tpep_pickup_datetime / 1000)
    print(f"Received: PU={ride.PULocationID}, DO={ride.DOLocationID}, "
          f"distance={ride.trip_distance}, amount=${ride.total_amount:.2f}, "
          f"pickup={pickup_dt}")
    count += 1
    if count >= 10:
        print(f"\n... received {count} messages so far (stopping after 10 for demo)")
        break

# stop Kafka consumer client
consumer.close()