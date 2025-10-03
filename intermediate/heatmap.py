from mrjob.job import MRJob
import numpy as np

class Heatmap(MRJob):
	def mapper(self, _, line):
		if line.startswith("Date"):
			return
		else:
			columns = line.split(",")
			key = (columns[10]
	def combiner(self, key, values):
		pass
		
	def reducer(self, key, values):
		pass

if __name__ == '__main__':
	Heatmap.run()
