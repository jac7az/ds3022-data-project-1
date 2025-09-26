import duckdb
import logging
import pandas as pd

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

def analysis():

    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")
        co2_dic={}
        #Largest carbon producing trip each year for green and yellow
        for i in range(2014,2025):
            co2_dic[i]=float(con.execute(f"""SELECT MAX(trip_co2_kgs) FROM yellow_tripdata WHERE YEAR(tpep_pickup_datetime)={i};""").fetchall()[0][0])

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    analysis()