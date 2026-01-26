import pandas as pd
from tqdm.auto import tqdm
from sqlalchemy import create_engine
import click

@click.command()
@click.option('--pg-user', default='root')
@click.option('--pg-pass', default='root')
@click.option('--pg-host', default='localhost')
@click.option('--pg-port', default=5432, type=int)
@click.option('--pg-db', default='ny_taxi')
@click.option('--year', default=2021, type=int)
@click.option('--month', default=1, type=int)
@click.option('--chunksize', default=100000, type=int)
@click.option('--target-table', default='yellow_taxi_data')

def main(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, chunksize, target_table):

    # 1. URL construction (Parquet files don't need dtype/parse_dates, it's built-in)
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet"
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    print(f"Downloading {url}...")
    df = pd.read_parquet(url)
    
    table_size = len(df)
    print(f"Total rows to ingest: {table_size}")

    # 2. Create schema
    df.head(0).to_sql(name=target_table, con=engine, if_exists='replace')

    # 3. Manual chunking since read_parquet doesn't have an iterator
    for i in tqdm(range(0, table_size, chunksize)):
        df_chunk = df.iloc[i : i + chunksize]
        df_chunk.to_sql(name=target_table, con=engine, if_exists='append', method='multi', chunksize=1000)

    print("Ingestion complete!")

if __name__ == "__main__":
    main()