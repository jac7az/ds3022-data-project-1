import duckdb
import logging
import pandas as pd
import time
import matplotlib.pyplot as plt
import pandas as pd

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='analysis.log'
)
logger = logging.getLogger(__name__)

def analysis():

    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        #Largest and lowest producing trips for green and yellow in all 10 years, with a maximum display of the first 10 largest and lowest for each table.
        max_y=con.execute(f"""SELECT * FROM yellow_emissions WHERE trip_co2_kgs=(SELECT MAX(trip_co2_kgs) FROM yellow_emissions);""").fetchmany(size=10)
        min_y=con.execute(f"""SELECT * FROM yellow_emissions WHERE trip_co2_kgs=(SELECT MIN(trip_co2_kgs) FROM yellow_emissions);""").fetchmany(size=10)
        max_g=con.execute(f"""SELECT * FROM green_emissions WHERE trip_co2_kgs=(SELECT MAX(trip_co2_kgs) FROM green_emissions);""").fetchmany(size=10)
        min_g=con.execute(f"""SELECT * FROM green_emissions WHERE trip_co2_kgs=(SELECT MIN(trip_co2_kgs) FROM green_emissions);""").fetchmany(size=10)

        print(f"Highest CO2 trip for yellow taxi: {max_y}",'\n')
        time.sleep(3) #Prevents all of the entries appearing at once.
        print(f"Highest CO2 trip for green taxi: {max_g}",'\n')
        time.sleep(3)
        print(f"Lowest CO2 trip for yellow taxi: {min_y}",'\n')
        time.sleep(3)
        print(f"Lowest CO2 trip for green taxi: {min_g}",'\n')
        logging.info("Min and max CO2 outputs for yellow and green taxi trips")

        # average most carbon heavy and carbon light hours of the day for YELLOW and for GREEN trips
        heavy_y=con.execute(f"""SELECT AVG(hour_of_day) FROM yellow_emissions WHERE trip_co2_kgs=(SELECT MAX(trip_co2_kgs) FROM yellow_emissions);""").fetchall()[0][0]
        light_y=con.execute(f"""SELECT ROUND(AVG(hour_of_day),2) FROM yellow_emissions WHERE trip_co2_kgs=(SELECT MIN(trip_co2_kgs) FROM yellow_emissions);""").fetchall()[0][0]
        heavy_g=con.execute(f"""SELECT AVG(hour_of_day) FROM green_emissions WHERE trip_co2_kgs=(SELECT MAX(trip_co2_kgs) FROM green_emissions);""").fetchall()[0][0]
        light_g=con.execute(f"""SELECT ROUND(AVG(hour_of_day),2) FROM green_emissions WHERE trip_co2_kgs=(SELECT MIN(trip_co2_kgs) FROM green_emissions);""").fetchall()[0][0]
        
        print(f"Average heaviest hour for yellow trip: {heavy_y}")
        print(f"Average heaviest hour for yellow trip: {light_y}")
        print(f"Average heaviest hour for green trip: {heavy_g}")
        print(f"Average heaviest hour for green trip: {light_g}")
        logging.info(f"Average heaviest hour for yellow trip: {heavy_y}")
        logging.info(f"Average heaviest hour for yellow trip: {light_y}")
        logging.info(f"Average heaviest hour for green trip: {heavy_g}")
        logging.info(f"Average heaviest hour for green trip: {light_g}")

        #Average are the most carbon heavy and carbon light days of the week for YELLOW and for GREEN trips?
        heavy_day_y=con.execute("""SELECT day_of_week, ROUND(AVG(trip_co2_kgs),3) FROM yellow_emissions 
                                GROUP BY day_of_week
                                ORDER BY AVG(trip_co2_kgs) DESC LIMIT 1
                                ;""").fetchall()
        light_day_y=con.execute("""SELECT day_of_week, ROUND(AVG(trip_co2_kgs),3) FROM yellow_emissions 
                                GROUP BY day_of_week
                                ORDER BY AVG(trip_co2_kgs) LIMIT 1
                                ;""").fetchall()
        heavy_day_g=con.execute("""SELECT day_of_week, ROUND(AVG(trip_co2_kgs),3) FROM green_emissions 
                                GROUP BY day_of_week
                                ORDER BY AVG(trip_co2_kgs) DESC LIMIT 1
                                ;""").fetchall()
        light_day_g=con.execute("""SELECT day_of_week, ROUND(AVG(trip_co2_kgs),3) FROM green_emissions 
                                GROUP BY day_of_week
                                ORDER BY AVG(trip_co2_kgs) LIMIT 1
                                ;""").fetchall()
        print(f"Average heaviest hour for yellow trip: {heavy_day_y[0][0]} at {heavy_day_y[0][1]} kg")
        print(f"Average lightest hour for yellow trip: {light_day_y[0][0]} at {light_day_y[0][1]} kg")
        print(f"Average heaviest hour for green trip: {heavy_day_g[0][0]} at {heavy_day_g[0][1]} kg")
        print(f"Average lightest hour for green trip: {light_day_g[0][0]} at {light_day_g[0][1]} kg")
        logging.info(f"Average heaviest hour for yellow trip: {heavy_day_y[0][0]} at {heavy_day_y[0][1]} kg")
        logging.info(f"Average lightest hour for yellow trip: {light_day_y[0][0]} at {light_day_y[0][1]} kg")
        logging.info(f"Average heaviest hour for green trip: {heavy_day_g[0][0]} at {heavy_day_g[0][1]} kg")
        logging.info(f"Average lightest hour for green trip: {light_day_g[0][0]} at {light_day_g[0][1]} kg")

        #average are the most carbon heavy and carbon light weeks of the year for YELLOW and for GREEN trips
        heavy_week_y=con.execute("""SELECT week_of_year, ROUND(AVG(trip_co2_kgs),3) FROM yellow_emissions 
                                GROUP BY week_of_year
                                ORDER BY AVG(trip_co2_kgs) DESC LIMIT 1
                                ;""").fetchall()
        light_week_y=con.execute("""SELECT week_of_year, ROUND(AVG(trip_co2_kgs),3) FROM yellow_emissions 
                                GROUP BY week_of_year
                                ORDER BY AVG(trip_co2_kgs) LIMIT 1
                                ;""").fetchall()
        heavy_week_g=con.execute("""SELECT week_of_year, ROUND(AVG(trip_co2_kgs),3) FROM green_emissions 
                                GROUP BY week_of_year
                                ORDER BY AVG(trip_co2_kgs) DESC LIMIT 1
                                ;""").fetchall()
        light_week_g=con.execute("""SELECT week_of_year, ROUND(AVG(trip_co2_kgs),3) FROM green_emissions 
                                GROUP BY week_of_year
                                ORDER BY AVG(trip_co2_kgs) LIMIT 1
                                ;""").fetchall()
        print(f"Average heaviest week for yellow trip: {heavy_week_y[0][0]} at {heavy_week_y[0][1]} kg")
        print(f"Average lightest week for yellow trip: {light_week_y[0][0]} at {light_week_y[0][1]} kg")
        print(f"Average heaviest week for green trip: {heavy_week_g[0][0]} at {heavy_week_g[0][1]} kg")
        logging.info(f"Average lightest week for green trip: {light_week_g[0][0]} at {light_week_g[0][1]} kg")
        logging.info(f"Average heaviest week for yellow trip: {heavy_week_y[0][0]} at {heavy_week_y[0][1]} kg")
        logging.info(f"Average lightest week for yellow trip: {light_week_y[0][0]} at {light_week_y[0][1]} kg")
        logging.info(f"Average heaviest week for green trip: {heavy_week_g[0][0]} at {heavy_week_g[0][1]} kg")
        logging.info(f"Average lightest week for green trip: {light_week_g[0][0]} at {light_week_g[0][1]} kg")
        
        #most carbon heavy and carbon light months of the year for YELLOW and for GREEN trips
        heavy_month_y=con.execute("""SELECT month_of_year, ROUND(AVG(trip_co2_kgs),3) FROM yellow_emissions 
                                GROUP BY month_of_year
                                ORDER BY AVG(trip_co2_kgs) DESC LIMIT 1
                                 ;""").fetchall()
        light_month_y=con.execute("""SELECT month_of_year, ROUND(AVG(trip_co2_kgs),3) FROM yellow_emissions 
                                GROUP BY month_of_year
                                ORDER BY AVG(trip_co2_kgs) LIMIT 1
                                 ;""").fetchall()
        heavy_month_g=con.execute("""SELECT month_of_year, ROUND(AVG(trip_co2_kgs),3) FROM green_emissions 
                                GROUP BY month_of_year
                                ORDER BY AVG(trip_co2_kgs) DESC LIMIT 1
                                 ;""").fetchall()
        light_month_g=con.execute("""SELECT month_of_year, ROUND(AVG(trip_co2_kgs),3) FROM green_emissions 
                                GROUP BY month_of_year
                                ORDER BY AVG(trip_co2_kgs) LIMIT 1
                                 ;""").fetchall()
        print(f"Average heaviest month for yellow trip: {heavy_month_y[0][0]} at {heavy_month_y[0][1]} kg")
        print(f"Average lightest month for yellow trip: {light_month_y[0][0]} at {light_month_y[0][1]} kg")
        print(f"Average heaviest month for green trip: {heavy_month_g[0][0]} at {heavy_month_g[0][1]} kg")
        print(f"Average lightest month for green trip: {light_month_g[0][0]} at {light_month_g[0][1]} kg")
        logging.info(f"Average heaviest month for yellow trip: {heavy_month_y[0][0]} at {heavy_month_y[0][1]} kg")
        logging.info(f"Average lightest month for yellow trip: {light_month_y[0][0]} at {light_month_y[0][1]} kg")
        logging.info(f"Average heaviest month for green trip: {heavy_month_g[0][0]} at {heavy_month_g[0][1]} kg")
        logging.info(f"Average lightest month for green trip: {light_month_g[0][0]} at {light_month_g[0][1]} kg")

        #time-series plots for each year along the X-axis and CO2 totals along the Y-axis
            #Render two lines/bars/plots of data, one each for YELLOW and GREEN taxi trip CO2 totals  
        yellow_df=pd.DataFrame(columns=['year','co2'])
        green_df=pd.DataFrame(columns=['year','co2'])
        y=con.execute("""SELECT DISTINCT YEAR(tpep_pickup_datetime) AS year,ROUND(SUM(trip_co2_kgs),3) as co2 FROM yellow_emissions 
                         GROUP BY year""").fetchall()
        g=con.execute("""SELECT DISTINCT YEAR(lpep_pickup_datetime) AS year,ROUND(SUM(trip_co2_kgs),3) as co2 FROM green_emissions 
                         GROUP BY year""").fetchall()
        logging.info("Dataframes created. Plotting now.")
        
        for i in range(len(y)):
            yellow_df.loc[i]=[y[i][0],y[i][1]]
            green_df.loc[i]=[g[i][0],g[i][1]]
        plt.plot(yellow_df['year'],yellow_df['co2'],label='Yellow Taxi')
        plt.plot(green_df['year'],green_df['co2'],label="Green Taxi")
        plt.legend()
        plt.xlabel("Year")
        plt.xticks(yellow_df['year'])
        plt.ylabel("Total CO2 Emission in KG")
        plt.savefig('yearly_emissions.png')

        logging.info("PNG created. Check workspace sidebar.")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    analysis()