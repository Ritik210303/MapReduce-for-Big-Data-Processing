from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime


class AverageTripTimeByPickup(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_get_trip_times,
                reducer=self.reducer_sum_trip_times
            ),
            MRStep(
                reducer=self.reducer_calculate_average
            )
        ]

    def mapper_get_trip_times(self, _, line):
        """Mapper: Extract pickup location ID and calculate trip duration."""
        try:
            # Split the line into fields
            fields = line.split(',')

            # Skip header row by checking if pickup_location_id is numeric
            if fields[7].isdigit():
                pickup_location_id = fields[7]
                pickup_time = fields[1]
                dropoff_time = fields[2]

                # Parse timestamps to calculate trip time in minutes
                pickup_dt = datetime.strptime(pickup_time, '%Y-%m-%d %H:%M:%S')
                dropoff_dt = datetime.strptime(dropoff_time, '%Y-%m-%d %H:%M:%S')
                trip_duration = (dropoff_dt - pickup_dt).total_seconds() / 60  # Convert to minutes

                # Emit pickup location ID and trip duration
                yield pickup_location_id, trip_duration

        except (IndexError, ValueError):
            # Skip lines with invalid data
            pass

    def reducer_sum_trip_times(self, pickup_location_id, durations):
        """Reducer: Calculate total trip time and count for each pickup location."""
        total_time = 0
        trip_count = 0

        for duration in durations:
            total_time += duration
            trip_count += 1

        # Emit pickup location ID, total time, and trip count
        yield pickup_location_id, (total_time, trip_count)

    def reducer_calculate_average(self, pickup_location_id, values):
        """Final Reducer: Calculate average trip time."""
        total_time = 0
        trip_count = 0

        for value in values:
            total_time += value[0]
            trip_count += value[1]

        # Calculate average trip time
        if trip_count > 0:
            average_time = total_time / trip_count
            yield pickup_location_id, average_time


if __name__ == '__main__':
    AverageTripTimeByPickup.run()
