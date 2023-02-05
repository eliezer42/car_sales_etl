import extract
import transform
import load

if __name__ == '__main__':
    uploaded_rows = 0
    car_sales_raw = extract.main()
    car_sales_processed = transform.main(car_sales_raw)
    uploaded_rows = load.main(car_sales_processed)
    print(f"{uploaded_rows} rows were parsed from {extract.source_url} and inserted into database.")