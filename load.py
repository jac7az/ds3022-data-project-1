import duckdb
import os
import logging
import pandas as pd
import time

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

def load_parquet_files():

    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")
        
        #Initialize the table 
        con.execute("""CREATE OR REPLACE TABLE yellow_tripdata (tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP,passenger_count BIGINT, trip_distance DOUBLE);""")
        con.execute("""CREATE OR REPLACE TABLE green_tripdata (lpep_pickup_datetime TIMESTAMP, lpep_dropoff_datetime TIMESTAMP,passenger_count BIGINT, trip_distance DOUBLE);""")
        logger.info("Created green and yellow tables")

         #Get a list of all 10-years worth of files for yellow taxi trips and green taxi trips and put them into lists so sql insert into table. Delete entries from each year that aren't from that year (eg. 2008 entry in 2023 file)
        for year in range(2014,2025):
            for month in range(1,13):
                con.execute(f"""INSERT INTO yellow_tripdata SELECT tpep_pickup_datetime, tpep_dropoff_datetime,passenger_count, trip_distance FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02}.parquet')
                            WHERE YEAR(tpep_pickup_datetime)={year};
                            INSERT INTO green_tripdata SELECT lpep_pickup_datetime, lpep_dropoff_datetime,passenger_count, trip_distance FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02}.parquet')
                            WHERE YEAR(lpep_pickup_datetime)={year};""")
                logging.info(f"Input data from {year} {month:02}")
                time.sleep(60)
        logging.info("Successfully added 10 years of files into each table")

        #Create separate table for emissions called nyc_taxi_data
        con.execute(f"""CREATE OR REPLACE TABLE nyc_taxi_data AS SELECT * FROM read_csv('data/vehicle_emissions.csv')""")
        logging.info("created nyc_taxi_data table")

        #Output basic descriptive stats to screen and to log related to column names and their datatypes.
        columns=['column_name','column_type','null','key','default','extra']
        yellow_stats=con.execute(f"""DESCRIBE SELECT * FROM yellow_tripdata;""")
        yellow_data = pd.DataFrame(yellow_stats.fetchall(),columns=columns)
        logging.info(yellow_data['column_name'])

        green_stats=con.execute(f"""DESCRIBE SELECT * FROM green_tripdata;""") 
        green_data=pd.DataFrame(green_stats.fetchall(),columns=columns)
        logging.info(green_data['column_name'])
        
        emission_stats=con.execute(f"""DESCRIBE SELECT * FROM nyc_taxi_data;""") 
        emission_data=pd.DataFrame(emission_stats.fetchall(),columns=columns)
        logging.info(emission_data['column_name'])
        
        print(yellow_data,'\n')
        print(green_data,'\n')
        print(emission_data)

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    load_parquet_files()