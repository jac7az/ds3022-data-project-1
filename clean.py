import duckdb
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

def clean_parquet_files():

    con = duckdb.connect(database='emissions.duckdb', read_only=False)
    logger.info("Connected to emissions.duckdb for cleaning")

    #Eliminate duplicate entries and entries with null variables
    try:  
        con.execute(f"""CREATE TABLE clean_yellow_tripdata AS SELECT DISTINCT * FROM yellow_tripdata;
                    "DROP TABLE yellow_tripdata;
                    ALTER TABLE clean_yellow_tripdata RENAME TO yellow_tripdata;""")
        con.execute(f"""DELETE FROM yellow_tripdata WHERE VendorID IS NULL
                    OR tpep_pickup_datetime IS NULL
                    OR tpep_dropoff_datetime IS NULL
                    OR tpep_dropoff_datetime - tpep_pickup_datetime > INTERVAL '24 hours'
                    OR passenger_count IS NULL
                    OR passenger_count=0
                    OR trip_distance IS NULL
                    OR trip_distance>100;
                    """)
        logging.info("Created clean version of yellow tripdata")

        con.execute(f"""CREATE TABLE clean_green_tripdata AS SELECT DISTINCT * FROM green_tripdata;
                    DROP TABLE green_tripdata;
                    ALTER TABLE clean_green_tripdata RENAME TO green_tripdata;""")
        con.execute(f"""DELETE FROM green_tripdata WHERE VendorID IS NULL
                    OR lpep_pickup_datetime IS NULL
                    OR lpep_dropoff_datetime IS NULL
                    OR lpep_dropoff_datetime - lpep_pickup_datetime > INTERVAL '24 hours'
                    OR passenger_count IS NULL
                    OR passenger_count=0
                    OR trip_distance IS NULL
                    OR trip_distance>100;
                    """)
        logging.info("Created clean version of green tripdata")

        con.execute(f"""CREATE TABLE clean_emissions AS SELECT DISTINCT * FROM emissions;
                    DROP TABLE emissions;
                    ALTER TABLE clean_emissions RENAME TO emissions;""")
        logging.info("Create clean table for emissions")
    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

def clean_test():
    con = duckdb.connect(database='emissions.duckdb', read_only=False)
    logger.info("Connected to emissions.duckdb for clean testing")
    try:
        #test to see if there are any remaining entries that break the conditions in yellow_tripdata
        print(con.execute(f"""SELECT COUNT(*) FROM yellow_tripdata WHERE VendorID IS NULL
                                 OR tpep_pickup_datetime IS NULL OR tpep_dropoff_datetime IS NULL 
                                 OR tpep_dropoff_datetime - tpep_pickup_datetime > INTERVAL '24 hours'
                                 OR passenger_count IS NULL OR passenger_count=0
                                 OR trip_distance IS NULL OR trip_distance>100;""").fetchone()[0])
        if con.execute("SELECT COUNT(*) FROM yellow_tripdata;").fetchone()[0]-con.execute("SELECT COUNT(DISTINCT *) FROM yellow_tripdata;").fetchone()[0]==0:
            print("No duplicates found.")
            logging.info("No duplicate or incorrect entries in yellow_tripdata")
        else:
            logging.error("Duplicates found")
        
        #test to see if there are any remaining entries that break the conditions in green_tripdata   
        print(con.execute(f"""SELECT COUNT(*) FROM green_tripdata WHERE VendorID IS NULL
                                 OR lpep_pickup_datetime IS NULL OR lpep_dropoff_datetime IS NULL 
                                 OR lpep_dropoff_datetime - lpep_pickup_datetime > INTERVAL '24 hours'
                                 OR passenger_count IS NULL OR passenger_count=0
                                 OR trip_distance IS NULL OR trip_distance>100;""").fetchone()[0]) 
        if con.execute("SELECT COUNT(*) FROM green_tripdata;").fetchone()[0]-con.execute("SELECT COUNT(DISTINCT *) FROM green_tripdata;").fetchone()[0]==0:
            print("No duplicates found.")
            logging.info("No duplicate or incorrect entries in green_tripdata")
        else:
            logging.error("Duplicates found")
    except Exception as e:
        print(f"Clean testing error has occured:{e}")

if __name__ == "__main__":
    clean_parquet_files()
    clean_test()