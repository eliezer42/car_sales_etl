from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.orm import declarative_base
import os

db_user = os.environ.get('DB_USER','postgres') #defaults to postgres
db_password = os.environ.get('DB_PASS')

engine = create_engine(f"postgresql://{db_user}:{db_password}@localhost:5432/car_sales")
session = Session(engine)
Base = declarative_base()