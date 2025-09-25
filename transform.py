import duckdb
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

def transform_parquet_files():
    con = duckdb.connect(database='emissions.duckdb', read_only=False)
    logger.info("Connected to emissions.duckdb for cleaning")

    try:
        
    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    transform_parquet_files()