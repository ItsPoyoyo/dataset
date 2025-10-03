from mrjob.job import MRJob

class CountRepeat(MRJob):
   def mapper(self, _, line):
    if "CLOSE" in line.upper():
       return
    
    values = line.upper().split(",")
    organization = values[8]

    yield organization, 1

   def reducer(self, key, values):
      yield key, sum(values)

if __name__ == '__main__':
    CountRepeat.run()