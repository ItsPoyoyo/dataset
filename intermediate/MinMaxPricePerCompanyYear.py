%%writefile routine3_min_max_price.py
from mrjob.job import MRJob
import csv

class MinMaxPricePerCompanyYear(MRJob):

    def mapper(self, _, line):
        if line.startswith("Date"):
            return

        parts = next(csv.reader([line]))

        try:
            date = parts[0]          # Date column
            close_price = parts[4]   # Close price column
            company = parts[-1]      # Company name (last column)
        except IndexError:
            return

        year = date[:4]  # "YYYY"

        try:
            close_price_float = float(close_price)
        except ValueError:
            return # Skip lines with invalid price

        yield (company, year), (close_price_float, close_price_float) # Yield (min_price, max_price)

    def combiner(self, key, values):
        min_price = float('inf')
        max_price = float('-inf')
        for min_val, max_val in values:
            min_price = min(min_price, min_val)
            max_price = max(max_price, max_val)
        yield key, (min_price, max_price)


    def reducer(self, key, values):
        min_price = float('inf')
        max_price = float('-inf')
        for min_val, max_val in values:
            min_price = min(min_price, min_val)
            max_price = max(max_price, max_val)
        yield key, (min_price, max_price)

if __name__ == "__main__":
    MinMaxPricePerCompanyYear.run()

#!python routine3_min_max_price.py stock_details_5_years.csv > routine3_min_max_price_output.txt
