import extract
import transform
import load

if __name__ == '__main__':
    uploaded_rows = 0
    # chunksize set to 1000 for testing purposes. IRL it would be a much larger number.
    with extract.main(chunksize=1000) as reader:
        for chunk in reader:
            print(chunk.shape[0])
            car_sales_processed = transform.main(chunk)
            uploaded_rows += load.main(car_sales_processed)
    print(f"{uploaded_rows} rows were parsed from {extract.source_url} and inserted into database.")
