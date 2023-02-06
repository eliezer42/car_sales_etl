# Car Sales ETL
## Description
ETL written in python for a car sales dataset to be loaded into a postgresql database.

## Pre-requisites
To run this project you will have to:
1. set up a postgresql server
2. have python3.9 or higher intalled
3. clone or download this repo
4. set database connection parameters as environment variables
5. (optional) set up a python virtual environment

## Usage

These ETL project is enabled for handling csv files and excel files either stored locally or remotely. The first 2 examples are for csv files. The chunksize parameter set the amount of rows to be parsed at a time by the ETL, which makes the project able to handle large csv files. The last 2 examples are for excel files that has the data table in their first sheet.

In order to test online data retrieving functionality, I provide links to a couple of publicly available files stored in AWS S3.

```
python ./scripts/etl.py --source ./data/input/car_sales.csv --source-type csv --chunksize 1000
python ./scripts/etl.py --source https://em-datatsets.s3.amazonaws.com/car_sales.csv --source-type csv --chunksize 1000 --online
python ./scripts/etl.py --source ./data/input/car_sales.xlsx --source-type excel
python ./scripts/etl.py --source https://em-datatsets.s3.amazonaws.com/car_sales.xlsx --source-type excel --online
```
Depending on your operating system you may need to use `python3` instead of `python`.

Assuming you have the project folder in your local disk and a postgresql server was properly set up either in your localhost or in a remote host, go to the root of the project and open a terminal in that location.

Set the following environment variables as shown.

Windows:

````
$env:DB_USER='YOUR_USERNAME_HERE'
$env:DB_PASS='YOUR_PASSWORD_HERE'
$env:DB_HOST='YOUR_HOSTNAME_HERE'
$env:DB_PORT='YOUR_TCP_PORT_HERE'
````

Linux:

````
export DB_USER='YOUR_USERNAME_HERE'
export DB_PASS='YOUR_PASSWORD_HERE'
export DB_HOST='YOUR_HOSTNAME_HERE'
export DB_PORT='YOUR_TCP_PORT_HERE'
````

Take into account that the projects defines default values for DB_USER, DB_HOST and DB_PORT as 'postgres', 'localhost' and 5432 respectively, so if these values matches your configuration, you only require to set up DB_PASS. Last but not least, you have to create a database called `car_sales`.

Finally, make sure you have the dependencies installed in your environment. You can do so by runnning the following line `pip install -r requirements.txt` in Windows. Alternatively, use `python3 -m pip install -r requirements_linux.txt` (or just `pip install -r requirements_linux.txt` if you are using a virtual environment, which I highly encourage when using linux).


## Project Structure

    .
    ├── data                    # Here goes all the data
    │   └── input               # All the input files are stored here
    │       └── car_sales.csv   # Sample dataset
    ├── scripts                 # Source files for ETL
    │   ├── common              # Common functionality folder
    │   │   ├── base.py         # Contains database connection definitions
    │   │   └── tables.py       # Defines the data model
    │   ├── extract.py          # Retrieves the data into a proper dataframe
    │   ├── transform.py        # Holds the tranformations logic
    │   ├── load.py             # Stores the processed dataframe into the database
    │   └── etl.py              # Main script
    ├── column_mappings.json    # JSON files that helps to map original field names to the final ones
    └── ...

## Approach

This ETL project follows a straightforward file structure, where the functions of Extract, Transform and Load are defined in their corresponding source files. Every one of these python modules has a `main` function, that is responsible for executing the internal logic. In the same way, there is one script that orchestrates the three phases called `etl.py`. This source file handles cli interaction as well, so it has to be executed with proper arguments and options from a shell in order to run this ETL pipeline.

The main libraries of the project are `pandas` and `sqlalchemy`. The first was choosen due to its user-friendliness and efficiency since it is built on top of `numpy`. The later makes it easy to upload data to the databases without sacrificing performance.

### Data Modeling

Since the source dataset is not normalized, it was necessary to tranform it into a fact table (sales) and a dimension table (car_models), following the star model widely used in BI applications. If you want to check the dataset out more closely, open the Jupyter Notebook that contains a basic EDA.

![alt text](https://em-datatsets.s3.amazonaws.com/Screenshot_3.png)

### Flexibility
This project is thought to be extendable so it can handle new data sources and any change in its schema. If more data types were needed it's just matter of modifying the conditional statement in `extract.py` apropriately.

```python
        if source_type == 'excel':
            raw_df = pd.read_excel(source_file, usecols=cols.keys())
        elif source_type == 'csv':
            raw_df = pd.read_csv(source_file, encoding='utf-8', usecols=cols.keys(), chunksize=chunksize, on_bad_lines='skip')
        else:
            raise ValueError("Unsupported data source type")
```
In the same way, if there was as a change in the naming of the source fields, adjusting 'column_mappings.json' should be enough. This file serves a double purpose as well, since the keys are used to filter the columns we are interested in.
```js
{
    "Purchase Date":"purchase_date",
    "Make":"manufacturer",
    "Model":"model_name",
    "Color":"color",
    "New Car":"is_new_car",
    "Top Speed":"top_speed",
    "Buyer Gender":"buyer_gender",
    "Country":"country",
    "City":"city",
    "Sale Price":"sale_price",
    "Discount":"discount"
}
```

### Scalability

This ETL is able to handle large csv files since it leverages pandas ability to load data in chunks. As a result the script could process csv files of several GBs.
```python
            with extract.main(source=args.source, source_type=args.source_type, online=is_online, chunksize=1000) as reader:
                for chunk in reader:
                    car_sales_processed = transform.main(chunk)
                    uploaded_rows = uploaded_rows + load.main(car_sales_processed)
```
It is also possible to enable this ETL to run on a cluster by using pandas-API for PySpark, which requires little code refactoring.
