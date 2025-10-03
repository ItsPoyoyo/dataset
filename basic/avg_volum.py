from mrjob.job import MRJob

class AvgVolum(MRJob):
  def mapper(self, _, line):
    if "CLOSE" in line.upper():
        return
    
    columns = line.upper().split(",")
    volum = columns[5]
    company = columns[8]
    yield company, (float(volum), 1)

  def reducer(self, key, values):
    sum_volum = 0
    count = 0
    for value, c in values:
      sum_volum += value
      count += c

    yield key, f"Media de acoes negociadas {sum_volum / count :.2f}"

if __name__ == '__main__':
  AvgVolum.run()