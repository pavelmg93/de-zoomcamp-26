import json
import dataclasses

from dataclasses import dataclass
import pandas as pd


@dataclass
class Ride:
    lpep_pickup_datetime: int # epoch milliseconds
    lpep_dropoff_datetime: int # epoch milliseconds
    PULocationID: int
    DOLocationID: int
    passenger_count: int
    trip_distance: float
    tip_amount: float
    total_amount: float

@dataclass
class Ride1:
    lpep_pickup_datetime: str # string time
    lpep_dropoff_datetime: str # string time
    PULocationID: int
    DOLocationID: int
    passenger_count: int
    trip_distance: float
    tip_amount: float
    total_amount: float

def ride_from_row(row):
    return Ride(
        lpep_pickup_datetime=int(row['lpep_pickup_datetime'].timestamp() * 1000),
        lpep_dropoff_datetime=int(row['lpep_dropoff_datetime'].timestamp() * 1000),        
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        passenger_count=int(row['passenger_count']) if pd.notna(row['passenger_count']) else 0,
        trip_distance=float(row['trip_distance']) if pd.notna(row['trip_distance']) else -1,
        tip_amount=float(row['tip_amount']) if pd.notna(row['tip_amount']) else 0,
        total_amount=float(row['total_amount']) if pd.notna(row['total_amount']) else 0,
    )

def ride_from_row1(row):
    return Ride1(
        # Convert the pandas timestamp to the exact string format Flink expects
        lpep_pickup_datetime=row['lpep_pickup_datetime'].strftime('%Y-%m-%d %H:%M:%S'),
        lpep_dropoff_datetime=row['lpep_dropoff_datetime'].strftime('%Y-%m-%d %H:%M:%S'),      
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        passenger_count=int(row['passenger_count']) if pd.notna(row['passenger_count']) else 0,
        trip_distance=float(row['trip_distance']) if pd.notna(row['trip_distance']) else -1,
        tip_amount=float(row['tip_amount']) if pd.notna(row['tip_amount']) else 0,
        total_amount=float(row['total_amount']) if pd.notna(row['total_amount']) else 0,
    )

def ride_serializer(ride):
    ride_dict = dataclasses.asdict(ride)
    ride_json = json.dumps(ride_dict).encode('utf-8')
    return ride_json


def ride_deserializer(data):
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return Ride(**ride_dict)

def ride_deserializer1(data):
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return Ride1(**ride_dict)

