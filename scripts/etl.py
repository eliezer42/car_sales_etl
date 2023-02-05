import os
from sqlalchemy import create_engine, text
import pandas as pd

base_path = os.path.abspath(__file__ + "/../../")
source_path = os.path.join(base_path,'data','input','car_sales.csv')

# Column name mapping
cols = {'Purchase Date':'purchase_date',
        'Make':'manufacturer',
        'Model':'model',
        'Color':'color',
        'New Car':'is_new_car',
        'Top Speed':'top_speed',
        'Buyer Gender':'buyer_gender',
        'Country':'country',
        'City':'city',
        'Sale Price':'sale_price',
        'Discount':'discount'}


def extract(path_to_file:str) -> pd.DataFrame:
    # Reads the csv file and extracts a subsets of the columns
    raw_df = pd.read_csv(path_to_file, encoding='utf-8', usecols=cols.keys())
    return raw_df
    

def transform(raw_df:pd.DataFrame) -> pd.DataFrame:
    #Gets rid of missing values
    clean_df = raw_df.dropna()
    # Ranames the columns according to mapping
    df = clean_df.rename(columns=cols)
    #Converts the date into ISO Format
    df['purchase_date'] = pd.to_datetime(df['purchase_date'], dayfirst=True)
    df['purchase_date'] = df['purchase_date'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    #TODO additional transformations
    return df

def load(processed_df:pd.DataFrame) -> int:
    # Creates in memory database
    engine = create_engine('sqlite://', echo=False)
    
    inserted_rows = processed_df.to_sql('car_sales', con=engine, if_exists='replace', index = False)
    
    sql = text('SELECT * FROM car_sales')
    with engine.connect() as conn:
        r = conn.execute(sql)
        print(r.fetchone())    
    return inserted_rows

def car_sales_etl(path_to_file:str) -> int:
    uploaded_rows = 0
    car_sales_raw = extract(path_to_file)
    car_sales_processed = transform(car_sales_raw)
    uploaded_rows = load(car_sales_processed)
    return uploaded_rows

if __name__ == '__main__':
    parsed_rows = car_sales_etl(source_path)
    print(f"{parsed_rows} were parsed from {source_path} and inserted into database.")