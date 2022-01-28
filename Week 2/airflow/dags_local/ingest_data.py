import pandas as pd
from sqlalchemy import create_engine

from time import time
# www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
# !wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv
#www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records


def ingest_data(user,password,host,port,db,table_name,csv_file):
    print(table_name,csv_file)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    print("Connection established succesfully")
    print("Started inserting.. ")
    # download(source_url,taxi_csv)
    # download(zones_url,zones_csv)

    df  = pd.read_csv(csv_file,nrows=100) 

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # #Create postgresql connection engine
    # # print(f'postgresql://{user}:{password}@{host}:{port}/{db}')


    # #Create schema
    print(pd.io.sql.get_schema(df,name=table_name,con=engine))

    # #Read csv data in chunks using interator
    df_iter = pd.read_csv(csv_file,iterator=True,chunksize=100000)


 
    c = 0
    while True:
        start = time()
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
    
    # zones  = pd.read_csv(zones_csv) 
    # print(pd.io.sql.get_schema(zones,name=zones_table_name,con=engine))
    # zones.to_sql(name=zones_table_name,con=engine,if_exists="replace")

if __name__ == "__main__":
    ingest_data()

    