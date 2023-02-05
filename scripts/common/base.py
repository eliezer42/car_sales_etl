from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.orm import declarative_base
import os

db_user = os.environ.get('DB_USER','postgres') #defaults to postgres
db_password = os.environ.get('DB_PASS')
db_host = os.environ.get('DB_HOST','localhost') #defaults to localhost
db_port = os.environ.get('DB_PORT','5432')

engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/car_sales")
session = Session(engine)
Base = declarative_base()