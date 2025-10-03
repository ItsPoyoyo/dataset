from mrjob.job import MRJob
from mrjob.step import MRStep

class Avg(MRJob):
	def mapper(self, indice, block):
		if block.startswith("Date"):
			return
		else:
			columns = block.split(",")
			high = float(columns[2])
			low = float(columns[3])
			close = float(columns[4])
			open = float(columns[1])
			company = columns[8]
			totalPrice = high + low + open + close
			yield company, (totalPrice, 4)

	def combiner(self, key, values):
		sumPrice = 0.0
		count = 0
		for p, i in values:
			sumPrice += p
			count += i
		yield key, (sumPrice, count)

	def reducer(self, key, values):
		sumPrice = 0.0
		count = 0
		for p, i in values:
			sumPrice += p
			count += i
		result = f"Average Price = $: {sumPrice/count:.2f}"
		yield key, result
	
if __name__ == "__main__":
	Avg.run()
