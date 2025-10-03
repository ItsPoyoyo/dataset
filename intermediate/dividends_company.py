from mrjob.job import MRJob

class dividendsByCompany(MRJob):
  def mapper(self, _, line):
    try:
      columns = line.split(',')
      company = columns[8]
      dividends = float(columns[6]) if columns[6] else 0.0
      year = int(columns[0].split('-')[0])
      
      yield (company, year), dividends
    except:
      pass
  
  def combiner(self, key, values):
    values_list = list(values)
    total_dividends = sum(values_list)
    count = len(values_list)
    yield key, (total_dividends, count)

  def reducer(self, key, values):
    total_dividends = 0
    total_count = 0
    
    for div, count in values:
      total_dividends += div
      total_count += count
    
    avg_dividends = total_dividends / total_count
    
    yield key, f"Total Dividends: ${total_dividends:.2f}, Avg per Day: ${avg_dividends:.4f}"

if __name__ == '__main__':
  dividendsByCompany.run()