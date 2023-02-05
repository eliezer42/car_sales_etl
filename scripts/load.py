import pandas as pd
from common.base import engine

def main(processed_df:pd.DataFrame) -> int:
    inserted_rows = processed_df.to_sql('sales', con=engine, if_exists='append', index = False)
    return inserted_rows