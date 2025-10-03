from mrjob.job import MRJob

class posNegClose(MRJob):
  def mapper(self, _, line):
    try:
      columns = line.split(',')
      company = columns[8]
      open = float(columns[1])
      close = float(columns[4])
      year = int(columns[0].split('-')[0])
      
      yield (company, year), (open, close)
    except Exception as e:
      pass
  
  def combiner(self, key, values):
    neg = 0
    pos = 0
    neutral = 0
    for o, c in values:
      if c > o:
        pos += 1
      elif c == o:
        neutral += 1
      else:
        neg += 1
    yield key, (pos, neg, neutral)

  def reducer(self, key, values):
    neg = 0
    pos = 0
    neutral = 0
    for p, n, neu in values:
      neg += n
      pos += p
      neutral += neu

    yield key, f"Positive days: {pos}, Negative days: {neg}, Neutral days: {neutral}"

if __name__ == '__main__':
  posNegClose.run()