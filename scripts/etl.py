import extract
import transform
import load
import argparse
from validators.url import url

if __name__ == '__main__':
    uploaded_rows = 0
    parser = argparse.ArgumentParser(description='Process different data sources')
    parser.add_argument('--source', type=str, required=True, help='Path to the source file')
    parser.add_argument('--source-type', type=str, required=True, choices=['csv', 'excel'], help='Type of the data source')
    parser.add_argument('--chunksize', type=int, default=0, help='Chunksize to read csv file with')
    parser.add_argument('--online', dest='online', action='store_true', help='Weather the source file is online or local')
    args = parser.parse_args()

    is_online = True if args.online else False

    if args.source_type == 'excel':
        raw_df = extract.main(source=args.source, source_type=args.source_type, online=is_online)
        car_sales_processed = transform.main(raw_df)
        uploaded_rows = load.main(car_sales_processed)
    else:
        # chunksize set to 1000 for testing purposes. IRL it would be a much larger number.
        with extract.main(source=args.source, source_type=args.source_type, online=is_online, chunksize=1000) as reader:
            for chunk in reader:
                car_sales_processed = transform.main(chunk)
                uploaded_rows = uploaded_rows + load.main(car_sales_processed)
    print(f"{uploaded_rows} rows were parsed from {args.source} and inserted into database.")

