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

        #Largest carbon producing trip for green and yellow in all 10 years
        max_y=con.execute(f"""SELECT * FROM yellow_tripdata WHERE trip_co2_kgs=(SELECT MAX(trip_co2_kgs) FROM yellow_tripdata);""").fetchall()
        min_y=con.execute(f"""SELECT * FROM yellow_tripdata WHERE trip_co2_kgs=(SELECT MIN(trip_co2_kgs) FROM yellow_tripdata);""").fetchall()
        max_g=con.execute(f"""SELECT * FROM green_tripdata WHERE trip_co2_kgs=(SELECT MAX(trip_co2_kgs) FROM green_tripdata);""").fetchall()
        min_g=con.execute(f"""SELECT * FROM green_tripdata WHERE trip_co2_kgs=(SELECT MIN(trip_co2_kgs) FROM green_tripdata);""").fetchall()

        print(f"Highest CO2 trip for yellow taxi: {max_y}")
        print(f"Highest CO2 trip for green taxi: {max_g}")
        print(f"Lowest CO2 trip for yellow taxi: {min_y}")
        print(f"Lowest CO2 trip for green taxi: {min_g}")
        logging.info("Min and max CO2 output for yellow and green taxi trips")

        #average most carbon heavy and carbon light hours of the day for YELLOW and for GREEN trips

        
        
    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    analysis()