from base import Base, engine
from sqlalchemy import Column, Integer, String, Float, Boolean

class CarSale(Base):
    __tablename__ = "sales"

    id = Column(Integer, primary_key=True)
    manufacturer = Column(String(32))
    model_name = Column(String(32))
    purchase_date = Column(String(32))
    color = Column(String(32))
    is_new_car = Column(Boolean)
    top_speed = Column(Float)
    buyer_gender = Column(String(32))
    country = Column(String(128))
    city = Column(String(128))
    sale_price = Column(Float)
    discount = Column(Float)


# Create the table in the database
if __name__ == "__main__":
    Base.metadata.create_all(engine)