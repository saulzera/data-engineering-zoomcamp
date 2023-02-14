#!/usr/bin/env python
# coding: utf-8


from time import time
from datetime import timedelta
import os

import pandas as pd
from sqlalchemy import create_engine

from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url):
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    df = pd.read_csv(csv_name)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    return df


def get_chunks(df: pd.DataFrame, n: int):
    chunk_list = [df[i:i+n] for i in range(0, df.shape[0], n)] # splits the DF in a list with chunks of size n
    return chunk_list


@task(log_prints=True)
def transform_data(df):
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df


@task(log_prints=True, retries=3) # will transform the ingest_data function into a prefect task
def ingest_data(table, df):

    connection_block = SqlAlchemyConnector.load("pg-connector")
    with connection_block.get_connection(begin=False) as engine:
        df.head(n=0).to_sql(name=table, con=engine, if_exists='append')
        df.to_sql(name=table, con=engine, if_exists='append')


@flow(name='Subflow', log_prints=True)
def log_subflow(table:str):
    print(f'Logging Subflow for {table}')


@flow(name="Ingest Flow")
def main(table: str):

    url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
   
    log_subflow(table)
    raw_data = extract_data(url) # download data from url and build a dataframe
    data = transform_data(raw_data) # clean trips with 0 passengers
    chunk_list = get_chunks(data, 100000) # split the data into smaller chunks to ingest

    count = 0
    max_chunk_size = len(chunk_list)

    for chunk in chunk_list:
        t_start = time()

        count += 1
        ingest_data(table, chunk)

        t_end = time()
        total_time = t_end - t_start
        print(f'Inserted chunk {count}/{max_chunk_size} in {total_time:.3f} seconds.')


if __name__ == '__main__':
    main('yellow_taxi_trips')
