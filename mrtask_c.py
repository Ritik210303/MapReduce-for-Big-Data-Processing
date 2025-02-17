from mrjob.job import MRJob
from mrjob.step import MRStep


class PaymentTypeCount(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_get_payment_types,
                reducer=self.reducer_count_payment_types
            ),
            MRStep(
                reducer=self.reducer_sort_payment_types
            )
        ]

    def mapper_get_payment_types(self, _, line):
        """Mapper: Extract Payment Type from each line."""
        try:
            # Split the line into fields
            fields = line.split(',')
            # Skip the header row by checking if it contains non-numeric payment_type
            if fields[9].isdigit():
                payment_type = fields[9]

                # Emit payment_type with a count of 1
                yield payment_type, 1

        except (IndexError, ValueError):
            # Skip lines with invalid data
            pass

    def reducer_count_payment_types(self, payment_type, counts):
        """Reducer: Aggregate counts for each payment type."""
        # Sum up all counts for the payment type
        total_count = sum(counts)
        # Emit payment_type and its total count
        yield None, (payment_type, total_count)

    def reducer_sort_payment_types(self, _, payment_data):
        """Reducer: Sort payment types by count."""
        # Sort by count in descending order
        sorted_payment_data = sorted(payment_data, key=lambda x: x[1], reverse=True)

        # Emit sorted payment types and their counts
        for payment_type, count in sorted_payment_data:
            yield payment_type, count


if __name__ == '__main__':
    PaymentTypeCount.run()
