#!/usr/bin/env python
# coding: utf-8


from time import time
import argparse
import os
import gzip

import pandas as pd
from sqlalchemy import create_engine


def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table = params.table
    url = params.url
    csv_name = 'output.csv.gz'


    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    df = pd.read_csv(csv_name, iterator=True, chunksize=100000, compression='gzip')

    try:
        while True:
            t_start = time()
            
            df_iter = next(df)
            
            df_iter.tpep_pickup_datetime = df_iter.tpep_pickup_datetime.apply(pd.to_datetime)
            df_iter.tpep_dropoff_datetime = df_iter.tpep_dropoff_datetime.apply(pd.to_datetime)
            
            df_iter.to_sql(name=table, con=engine, if_exists='append')
            
            t_end = time()
            t_final = t_end - t_start
            
            print(f'inserted another chunk, took {t_final:.2f} seconds.')
            
    except StopIteration:
        print("Data ingestion finished.")



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres.')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table', help='name of the table where we will write results to')
    parser.add_argument('--url', help='url of the CSV file')

    args = parser.parse_args()

    main(args)
