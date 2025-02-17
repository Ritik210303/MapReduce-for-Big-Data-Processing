from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime

class RevenueAnalysis(MRJob):

    def mapper(self, _, line):
        # Skip header line if present
        if "tpep_pickup_datetime" in line:
            return
        
        # Split the line into columns
        fields = line.split(',')
        
        try:
            # Extract the necessary fields
            pickup_datetime = fields[1]  # tpep_pickup_datetime
            fare_amount = float(fields[7])  # fare_amount
            
            # Convert pickup_datetime to datetime object
            pickup_datetime = datetime.strptime(pickup_datetime, '%Y-%m-%d %H:%M:%S')

            # Extract month, hour, and day of the week
            month = pickup_datetime.month
            hour = pickup_datetime.hour
            weekday = pickup_datetime.weekday()  # Monday is 0, Sunday is 6
            
            # Classify hour into 'day' or 'night'
            time_period = 'day' if 6 <= hour < 18 else 'night'
            
            # Classify day into 'weekday' or 'weekend'
            day_type = 'weekend' if weekday in [5, 6] else 'weekday'
            
            # Emit key as (month, time_period, day_type) and value as the fare amount
            yield (month, time_period, day_type), fare_amount
        
        except Exception as e:
            # Skip any invalid records
            pass

    def reducer(self, key, values):
        # Calculate the average fare amount for each key (month, time_period, day_type)
        total_fare = 0
        count = 0
        for fare in values:
            total_fare += fare
            count += 1
        
        # Emit the key (month, time_period, day_type) and the average revenue
        if count > 0:
            avg_fare = total_fare / count
            yield key, avg_fare

if __name__ == '__main__':
    RevenueAnalysis.run()
