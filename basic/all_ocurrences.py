from mrjob.job import MRJob

class AllOcurrences(MRJob):
   def mapper(self, _, line):
      if "CLOSE" in line.upper():
         return
      values = line.upper().split(",")
      if values[8] == "VALE":
        organization = "VALE"
        close = float(values[4])
        yield organization, close
      
   def reducer(self, key, values):
      for v in values:
        yield key, v

if __name__ == '__main__':
  AllOcurrences.run()