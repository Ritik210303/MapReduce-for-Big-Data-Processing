import happybase
import csv

# Create connection
connection = happybase.Connection('localhost', port=9090, autoconnect=False)

# Open connection to perform operations
def open_connection():
    connection.open()

# Close the opened connection
def close_connection():
    connection.close()

# Get the pointer to a table
def get_table(name):
    open_connection()
    table = connection.table(name)
    return table

# Batch insert data from CSV into HBase
def batch_insert_data(file_path):
    print(f"Starting batch insert for {file_path}")
    try:
        with open(file_path, 'r') as file:
            csv_reader = csv.DictReader(file)
            table = get_table('yellow_taxi_trips')  # Replace with your actual table name

            with table.batch(batch_size=100) as b:  # Adjust batch size as needed
                for row in csv_reader:
                    # Ensure all required fields exist
                    if all(field in row for field in ['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee']):
                        row_key = f"{row['VendorID']}:{row['tpep_pickup_datetime']}"  # Unique key based on VendorID and pickup time
                        # Prepare the data to be inserted
                        row_data = {
                            'info:VendorID': row['VendorID'],
                            'info:tpep_pickup_datetime': row['tpep_pickup_datetime'],
                            'info:tpep_dropoff_datetime': row['tpep_dropoff_datetime'],
                            'info:passenger_count': row['passenger_count'],
                            'info:trip_distance': row['trip_distance'],
                            'info:RatecodeID': row['RatecodeID'],
                            'info:store_and_fwd_flag': row['store_and_fwd_flag'],
                            'info:PULocationID': row['PULocationID'],
                            'info:DOLocationID': row['DOLocationID'],
                            'info:payment_type': row['payment_type'],
                            'info:fare_amount': row['fare_amount'],
                            'info:extra': row['extra'],
                            'info:mta_tax': row['mta_tax'],
                            'info:tip_amount': row['tip_amount'],
                            'info:tolls_amount': row['tolls_amount'],
                            'info:improvement_surcharge': row['improvement_surcharge'],
                            'info:total_amount': row['total_amount'],
                            'info:congestion_surcharge': row['congestion_surcharge'],
                            'info:airport_fee': row['airport_fee']
                        }
                        # Insert data into batch
                        b.put(row_key, row_data)
                    else:
                        print(f"Missing data for row: {row}")

        print(f"Batch insert done for {file_path}")
    except Exception as e:
        print(f"Error inserting data from {file_path}: {e}")

# Insert data for both CSV files
batch_insert_data('yellow_tripdata_2017-03.csv')
batch_insert_data('yellow_tripdata_2017-04.csv')

# Close the connection
close_connection()
