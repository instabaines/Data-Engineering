#!/usr/bin/env python
# coding: utf-8


import os
from  sqlalchemy import create_engine
from time import time
import pandas as pd
import argparse

def main(params):
    user =params.user
    pwd = params.password
    host = params.host
    port =params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    #download the data
    pqt_name = 'output.parquet'
    os.system(f"wget {url} -O {pqt_name}")
    

    engine  = create_engine(f'postgresql://{user}:{pwd}@{host}:{port}/{db}')
    engine.connect()
    data = pd.read_parquet(pqt_name)



    # print(pd.io.sql.get_schema(data,name=table_name,con=engine))
    data.iloc[:10000].to_sql(name=table_name,con=engine,if_exists='replace')



    initial =10000
    count = 20000
    while count<len(data):
        t_start = time()
        data.iloc[initial:count].to_sql(name=table_name,con=engine,if_exists='append')
        t_end = time()
        print(f"Inserted {count-initial} entries in {round(t_end-t_start,3)}")
        initial = count
        count =count+10000


    data.iloc[initial:].to_sql(name=table_name,con=engine,if_exists='append')



parser = argparse.ArgumentParser(description='Ingest parquet data to Postgres')
# user 
# password
# host
# port
# database name
# table name
# url of the csv





if __name__=='__main__':
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='table name for postgres')
    parser.add_argument('--url', help='url of the file for postgres')

    args = parser.parse_args()
    main(args)


