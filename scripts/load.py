import pandas as pd
from common.base import engine
from sqlalchemy import text
import logging

def main(processed_df:pd.DataFrame) -> int:
    logging.info('Starting data loading', extra={'step': 'Load'})
    try:
        with engine.connect() as conn:
            conn.execute(text('DELETE FROM sales'))
            conn.execute(text('DELETE FROM car_models'))
        inserted_rows = processed_df.to_sql('sales', con=engine, if_exists='append', index = False)
        logging.info('Data loading finished', extra={'step': 'Load'})
    except Exception as e:
        logging.error(f'Error: {e}', extra={'step': 'Load'})
        raise

    return inserted_rows