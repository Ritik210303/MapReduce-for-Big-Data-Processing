from mrjob.job import MRJob
from mrjob.step import MRStep


class VendorTripsRevenue(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_get_trips_and_revenue,
                reducer=self.reducer_aggregate_trips_and_revenue
            ),
            MRStep(
                reducer=self.reducer_find_top_vendor
            )
        ]

    def mapper_get_trips_and_revenue(self, _, line):
        """Mapper: Extract VendorID and compute revenue."""
        try:
            # Parse the line into a list of fields
            fields = line.split(',')
            vendor_id = fields[0]
            fare_amount = float(fields[10])
            tip_amount = float(fields[13])

            # Calculate revenue
            revenue = fare_amount + tip_amount

            # Emit vendor_id, trips (1), and revenue
            yield vendor_id, (1, revenue)

        except (IndexError, ValueError):
            # Skip rows with invalid data
            pass

    def reducer_aggregate_trips_and_revenue(self, vendor_id, values):
        """Reducer: Aggregate trips and revenue for each VendorID."""
        total_trips = 0
        total_revenue = 0.0

        for trips, revenue in values:
            total_trips += trips
            total_revenue += revenue

        # Emit vendor_id, total_trips, and total_revenue
        yield None, (vendor_id, total_trips, total_revenue)

    def reducer_find_top_vendor(self, _, vendor_data):
        """Reducer: Identify the vendor with the most trips and their total revenue."""
        top_vendor = None
        max_trips = 0
        total_revenue = 0.0

        for vendor_id, trips, revenue in vendor_data:
            if trips > max_trips:
                top_vendor = vendor_id
                max_trips = trips
                total_revenue = revenue

        # Emit the top vendor, their total trips, and total revenue
        yield top_vendor, (max_trips, total_revenue)


if __name__ == '__main__':
    VendorTripsRevenue.run()
