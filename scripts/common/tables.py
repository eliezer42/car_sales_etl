from common.base import Base, engine
from sqlalchemy import Column, Integer, String, Float, Boolean, ForeignKey, Date
from sqlalchemy.orm import relationship

class CarModel(Base):
    __tablename__ = "car_models"

    id = Column(Integer, primary_key=True)
    model_name = Column(String(32))
    manufacturer = Column(String(64))

class CarSale(Base):
    __tablename__ = "sales"

    id = Column(Integer, primary_key=True)
    model_id = Column(Integer, ForeignKey("car_models.id"))
    purchase_date = Column(Date)
    purchase_year = Column(Integer)
    purchase_date_iso = Column(String(24))
    color = Column(String(32))
    is_new_car = Column(Boolean)
    top_speed = Column(Float)
    buyer_gender = Column(String(1))
    country = Column(String(128))
    city = Column(String(128))
    final_price = Column(Float)

    model = relationship("CarModel", foreign_keys=[model_id])

Base.metadata.create_all(engine)