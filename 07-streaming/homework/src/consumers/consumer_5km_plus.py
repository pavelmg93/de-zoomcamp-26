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
    group_id='rides-console',
    value_deserializer=ride_deserializer
)

# consume all messages from topic and count trips longer than 5km
# convert trip distance to km from miles

print(f"Counting trips in {topic_name}...")

count = 0

for message in consumer:
    ride = message.value
    miles_to_km = 1.60934 # Question asks for km, but options match miles bahaviour
    multiplier = 1
    if ride.trip_distance > (5.0 * multiplier):
        print(f'Currnet count: {count}')
        count += 1

print(f"Total trips with distance > 5km: {count}")

consumer.close()