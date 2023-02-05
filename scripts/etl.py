import pandas as pd
import extract
import load
import json

# Column name mapping
with open('column_mapping.json') as f:
    cols = json.load(f)

def transform(raw_df:pd.DataFrame) -> pd.DataFrame:
    #Gets rid of missing values
    clean_df = raw_df.dropna()
    # Ranames the columns according to mapping
    df = clean_df.rename(columns=cols)
    #Converts the date into ISO Format
    df['purchase_date'] = pd.to_datetime(df['purchase_date'], dayfirst=True)
    df['purchase_date'] = df['purchase_date'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    #TODO additional transformations
    return df[['manufacturer','model_name','purchase_date','color','is_new_car','top_speed','buyer_gender', 'country', 'city', 'sale_price', 'discount']]

if __name__ == '__main__':
    uploaded_rows = 0
    car_sales_raw = extract.main()
    car_sales_processed = transform(car_sales_raw)
    uploaded_rows = load.main(car_sales_processed)
    print(f"{uploaded_rows} were parsed from {extract.source_url} and inserted into database.")