from mrjob.job import MRJob
import csv

class MRRevenueByPickupLocation(MRJob):

    def mapper(self, _, line):
        """Mapper function to emit pickup location and total revenue."""
        try:
            row = dict(zip(
                ['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count',
                 'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID',
                 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
                 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee'],
                next(csv.reader([line]))
            ))
            
            # Check if the row is valid
            if self.is_valid_row(row):
                # Emit pickup location and total revenue
                yield row['PULocationID'], float(row['total_amount'])
        except Exception as e:
            pass

    def reducer(self, location, revenues):
        """Reducer function to sum the total revenue for each pickup location."""
        total_revenue = sum(revenues)
        yield location, total_revenue

    def is_valid_row(self, row):
        """Validate if the row has required fields."""
        try:
            float(row['total_amount'])  # Ensure total_amount is a valid float
            int(row['PULocationID'])    # Ensure PULocationID is a valid integer
            return True
        except:
            return False

if __name__ == '__main__':
    MRRevenueByPickupLocation.run()
