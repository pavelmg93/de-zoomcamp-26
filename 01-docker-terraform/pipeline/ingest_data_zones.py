import math
import pandas as pd
from tqdm.auto import tqdm
from sqlalchemy import create_engine
import click

dtype = {
    "LocationID": "Int64",
    "Borough": "string",
    "Zone": "string",
    "service_zone": "string",
}

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for ingestion')
@click.option('--target-table', 'target_table', default='zones', help='Target table name')

def main(pg_user, pg_pass, pg_host, pg_port, pg_db, chunksize, target_table):

    # Build the URL for the specified year/month
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"

    # Create the database engine using CLI-provided credentials
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    # Read to get size and schema
    df = pd.read_csv(
        url,
        dtype=dtype,
    )
    table_size = len(df)
    print(f"Total rows to ingest: {table_size}")

    # Create the table with the correct schema
    df.head(0).to_sql(name=target_table, con=engine, if_exists='replace')
    print(f"Table {target_table} created in the database.")
    print(df.head(0))

    # Create iterable object that will read data in chunks
    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        iterator=True,
        chunksize=chunksize
    )

    # Ingest data to Postgres chunk by chunk with visual progress bar
    for df_chunk in tqdm(df_iter, total=math.ceil(table_size / chunksize)):
        df_chunk.to_sql(name=target_table, con=engine, if_exists='append')

if __name__ == "__main__":
    main()