from mrjob.job import MRJob

class MinMaxClose(MRJob):
  def mapper(self, _, line):
    if "CLOSE" in line.upper():
        return
    
    values = line.upper().split(",")

    if len(values) <= 8:
        return
    data = values[0]
    close = float(values[4])
    organization = values[8]

    yield organization, (float(close), data)

  def reducer(self, key, values):

    low = float('inf')
    high = float('-inf')
    date_low = ""
    date_high = ""
    for close, date in values:
        if close < low:
          low = close
          date_low = date
        elif close > high:
          high = close
          date_high = date
           

    result = f"Min close: {low} on date {date_low}; High close: {high} on date {date_high}" 
    yield key, result 

if __name__ == '__main__':
  MinMaxClose.run()