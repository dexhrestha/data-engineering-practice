import csv
import pandas as pd
from sqlalchemy import create_engine

from time import time
import requests
import argparse
import os
# www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
# !wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv
#www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    csv_name = 'output.csv'

    #download
    if not os.path.exists(csv_name):
        with requests.get(url,stream=True) as r:
            r.raise_for_status()
            with open(csv_name,'wb')  as f:
                for chunk in r.iter_content(chunk_size=8000):
                    f.write(chunk)

    df  = pd.read_csv(csv_name,nrows=100) 

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    #Create postgresql connection engine
    # print(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')


    #Create schema
    print(pd.io.sql.get_schema(df,name=table_name,con=engine))

    #Read csv data in chunks using interator
    df_iter = pd.read_csv(csv_name,iterator=True,chunksize=100000)


    start = time()
    c = 0
    while True:
        try:
            df = next(df_iter)
        except:
            break
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name=table_name,con=engine,if_exists="append")
        total = time() - start
        print(f"Successfully inserted {c+1} chunks in {total:.2f} seconds")
        c+=1
    
    zones  = pd.read_csv('../datasets/taxi+_zone_lookup.csv',nrows=100) 
    print(pd.io.sql.get_schema(zones,name='zones',con=engine))
    zones.to_sql(name='zones',con=engine,if_exists="replace")

if __name__ == "__main__":
    

    parser = argparse.ArgumentParser(description='Ingest CSV Data to Postgres')

    # user 
    # password 
    # host
    # port 
    # database name
    # table name 
    # url of the csv

    parser.add_argument('--user',help='user name for postgres')
    parser.add_argument('--password',help='password for postgres')
    parser.add_argument('--host',help='host for postgres')
    parser.add_argument('--port',help='port for postgres')
    parser.add_argument('--db',help='database name for postgres')
    parser.add_argument('--table_name',help='name of table where we will write the results')
    parser.add_argument('--url',help='url of csv file')

    main(parser.parse_args())


