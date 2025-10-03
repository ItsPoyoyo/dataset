from mrjob.job import MRJob
import re
from datetime import datetime

class Heatmap(MRJob):
	def mapper(self, _, line):
		if line.startswith("Date"):
			return
		else:
			columns = line.split(",")
			date = datetime.strptime(columns[0][:10], "%Y-%m-%d") 
			month = date.strftime("%B")
			day = date.strftime("%A")
			keyMonth = ("Month", columns[8], month) #Company, Month
			keyDay = ("Day", columns[8], day) # Companym, Day
			value = int(columns[5]) # Volume
			yield keyMonth, value
			yield keyDay, value

	def combiner(self, key, values):
		volumeSum :int = 0
		for volume in values:
			volumeSum += volume

		yield key, volumeSum
		
	def reducer(self, key, values):
		totalVolume : int = 0
		counterVolume : int = 0
		for volume in values:
			totalVolume += volume
			counterVolume += 1
		averageVolume : int = totalVolume / float(counterVolume)
		analysis_type, company, period = key

		yield f"{analysis_type}: {period} | Company: {company} |", f" Average: {averageVolume:.2f}" 
	
if __name__ == '__main__':
	Heatmap.run()
