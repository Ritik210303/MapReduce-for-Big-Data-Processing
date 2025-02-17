from mrjob.job import MRJob
from mrjob.step import MRStep

class TipsToRevenueRatio(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratios,
                   reducer=self.reducer_calculate_average),
            MRStep(reducer=self.reducer_sort_by_ratio)
        ]

    def mapper_get_ratios(self, _, line):
        # Skip the header
        if line.startswith("VendorID"):
            return
        fields = line.split(",")
        try:
            PULocationID = fields[7]  # Pickup location ID
            tip_amount = float(fields[13])
            total_amount = float(fields[16])

            # Avoid division by zero
            if total_amount > 0:
                yield PULocationID, (tip_amount / total_amount)
        except (ValueError, IndexError):
            pass

    def reducer_calculate_average(self, PULocationID, ratios):
        # Calculate the average tips-to-revenue ratio for each location
        ratios_list = list(ratios)
        avg_ratio = sum(ratios_list) / len(ratios_list)
        yield None, (avg_ratio, PULocationID)

    def reducer_sort_by_ratio(self, _, location_ratios):
        # Sort by ratio in descending order
        for avg_ratio, PULocationID in sorted(location_ratios, reverse=True):
            yield PULocationID, avg_ratio

if __name__ == "__main__":
    TipsToRevenueRatio.run()

