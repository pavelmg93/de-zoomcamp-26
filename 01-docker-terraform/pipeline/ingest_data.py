import math
import pandas as pd
from tqdm.auto import tqdm
from sqlalchemy import create_engine

def set_parameters():
    global pg_host, pg_user, pg_pass, pg_db, pg_port
    global year, month, chunk_size, table_name, table_size
    global url, dtype, parse_dates

    # Define Postgres connection parameters
    pg_host = "localhost"
    pg_user = "root"
    pg_pass = "root"
    pg_db = "ny_taxi"
    pg_port = 5432

    # Define data parameters
    year = 2021
    month = 1
    chunk_size = 100000
    table_name = "yellow_taxi_data"
    table_size = 0  # will be calculated on first read

    # Define the URL for the dataset
    prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
    url = f"{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz"

    # Define the data types for each column
    dtype = {
        "VendorID": "Int64",
        "passenger_count": "Int64",
        "trip_distance": "float64",
        "RatecodeID": "Int64",
        "store_and_fwd_flag": "string",
        "PULocationID": "Int64",
        "DOLocationID": "Int64",
        "payment_type": "Int64",
        "fare_amount": "float64",
        "extra": "float64",
        "mta_tax": "float64",
        "tip_amount": "float64",
        "tolls_amount": "float64",
        "improvement_surcharge": "float64",
        "total_amount": "float64",
        "congestion_surcharge": "float64"
    }

    # Define the columns to parse as dates
    parse_dates = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime"
    ]

def run():
    # Set parameters
    set_parameters()

    # Create the database engine
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    # Read to get size and schema
    df = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates
    )
    table_size = len(df)

    # Create the table with the correct schema
    df.head(0).to_sql(name = table_name, con=engine, if_exists = 'replace')

    # Create iterable object that will read data in chunks
    df_iter = pd.read_csv(
        url,
        dtype = dtype,
        parse_dates = parse_dates,
        iterator = True,
        chunksize = chunk_size
    )

    # Ingest data to Postgres chunk by chunk
    for df_chunk in tqdm(df_iter, total = math.ceil(table_size / chunk_size)):
        df_chunk.to_sql(name = table_name, con = engine, if_exists = 'append')

if __name__ == "__main__":
    run()