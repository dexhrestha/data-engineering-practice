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

def download(url,output):
    if not os.path.exists(output):
        with requests.get(url,stream=True) as r:
            r.raise_for_status()
            with open(output,'wb')  as f:
                for chunk in r.iter_content(chunk_size=8000):
                    f.write(chunk)
        
        return output

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    taxi_table_name = params.taxi_table_name
    zones_table_name = params.zones_table_name
    source_url = params.source_url
    zones_url = params.zones_url
    #problem while downloading file so copied locally
    taxi_csv = 'taxi.csv'
    zones_csv = 'zones.csv'

    # download(source_url,taxi_csv)
    # download(zones_url,zones_csv)

    df  = pd.read_csv(taxi_csv,nrows=100) 

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    #Create postgresql connection engine
    # print(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')


    #Create schema
    print(pd.io.sql.get_schema(df,name=taxi_table_name,con=engine))

    #Read csv data in chunks using interator
    df_iter = pd.read_csv(taxi_csv,iterator=True,chunksize=100000)


 
    c = 0
    while True:
        start = time()
        try:
            df = next(df_iter)
        except:
            break
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name=taxi_table_name,con=engine,if_exists="append")
        total = time() - start
        print(f"Successfully inserted {c+1} chunks in {total:.2f} seconds")
        c+=1
    
    zones  = pd.read_csv(zones_csv) 
    print(pd.io.sql.get_schema(zones,name=zones_table_name,con=engine))
    zones.to_sql(name=zones_table_name,con=engine,if_exists="replace")

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
    parser.add_argument('--taxi_table_name',help='name of table where we will write the taxi results')
    parser.add_argument('--zones_table_name',help='name of table where we will write the zones results')
    parser.add_argument('--source_url',help='url of taxi data csv file')
    parser.add_argument('--zones_url',help='urk of zones data csv file')

    main(parser.parse_args())


