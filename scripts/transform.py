import pandas as pd
import json
import common.base as base
from common.tables import CarModel, CarSale
from sqlalchemy import select
import logging

with open('column_mapping.json') as f:
    cols = json.load(f)

def shorten_gender(gender:str) -> str:
    if gender == 'Male':
        return 'M'
    elif gender == 'Female':
        return 'F'
    elif gender == 'Non-binary':
        return 'N'
    else:
        # Other (Prefer not to say)
        return 'X'

def get_models_lookup_table() -> dict:
    with base.session as session:
        with session.begin():
            car_models = session.scalars(select(CarModel)).all()
        # Builds a dict with model_name as key and id as value
        car_models_dict = {model.model_name:model.id for model in car_models}
    return car_models_dict

def replace_model_by_its_id(model:str, manufacturer:str, models_lookup:dict) -> int:
    try:
        # If the model is already in the lookup table, it returns its id
        model_id = models_lookup[model]
    except KeyError:
        # if not, creates a new entry in the car models table and returns the newly created id
        with base.session as session:
            with session.begin():
                new_model = CarModel(model_name=model, manufacturer=manufacturer)
                session.add(new_model)
            session.refresh(new_model)
        models_lookup[model] = new_model.id
        model_id = new_model.id
    return model_id

def main(raw_df:pd.DataFrame) -> pd.DataFrame:
    logging.info('Starting data transformation', extra={'step': 'Transform'})
    
    try:
        #loads models lookup table
        models_lookup = get_models_lookup_table()
        #Gets rid of missing values
        clean_df = raw_df.dropna()

        # Ranames the columns according to mapping
        df = clean_df.rename(columns=cols)

        # Converts the date into datetime object
        df['purchase_date'] = pd.to_datetime(df['purchase_date'], dayfirst=True)

        # Gets the purchase year
        df['purchase_year'] = df['purchase_date'].dt.year

        # Creates datestring with ISO Format
        df['purchase_date_iso'] = df['purchase_date'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        # Calculates final price
        df['final_price'] = df['sale_price']*(1 - df['discount'])

        # Applied for disk space efficiency
        df['buyer_gender'] = df['buyer_gender'].apply(lambda x: shorten_gender(x))

        # Replaces the model and manufacturer for a key that points to car_models table
        df['model_id'] = df.apply(lambda x: replace_model_by_its_id(x['model_name'],x['manufacturer'],models_lookup), axis=1)
        logging.info('Data transformation finished', extra={'step': 'Transform'})
    except Exception as e:
        logging.error(f'Error: {e}', extra={'step': 'Transform'})
        raise

    return df[['model_id','purchase_date','purchase_year','purchase_date_iso','color','is_new_car','top_speed','buyer_gender', 'country', 'city', 'final_price']]