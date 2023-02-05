from base import Base, engine
# Create the table in the database
if __name__ == "__main__":
    Base.metadata.create_all(engine)