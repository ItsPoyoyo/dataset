%%writefile routine2_total_volume.py
from mrjob.job import MRJob
import csv

class VolumePerCompanyMonth(MRJob):

    def mapper(self, _, line):
        if line.startswith("Date"):
            return

        parts = next(csv.reader([line]))

        try:
            date = parts[0]          # Date column
            volume = parts[5]        # Volume column
            company = parts[-1]      # Company name (last column)
        except IndexError:
            return

        year_month = date[:7]  # "YYYY-MM"

        # remove virgula de volume como "1,234,567"
        try:
            volume_int = int(volume.replace(",", ""))
        except ValueError:
            return # Skip lines with invalid volume
        yield (company, year_month), volume_int

    def combiner(self, key, values):
        # Combiner is the same as reducer for sum
        yield key, sum(values)


    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == "__main__":
    VolumePerCompanyMonth.run()


#!python routine2_total_volume.py stock_details_5_years.csv > routine2_total_volume_output.txt
