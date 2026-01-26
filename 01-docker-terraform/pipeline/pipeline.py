import sys
import pandas as pd

print('Arguments: ', sys.argv)
month = sys.argv[1]
print(f'Hello, pipeline! The month is {month}.')

df = pd.DataFrame({'day': [1, 2], 'num_assengers': [3, 4]})
df['month'] = month
print(df.head())

df.to_parquet(f'data/nyc_tlc_yellow_tripdata_sample_2020-{month}.parquet')