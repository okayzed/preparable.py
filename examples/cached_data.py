import sys
sys.path.append('..')

import random

from preparable import Preparer, Preparable, PrepFetcher, PrepResult

# {{{ Fetchers
class SimpleFetcher(PrepFetcher):
  def init(self, data):
    self.data = data

  def fetch(self):
    return self.data

class CacheFetcher(PrepFetcher):
  def init(self, data, key):
    self.data = data
    self.cache_key = key

  def fetch(self):
    return self.data

  def get_cache_key(self):
    return self.cache_key
# }}}


# {{{ Preparables
class CachePreparable(object):
  def work(self):
    fetcher = CacheFetcher('bar', 'foo')
    data = yield fetcher

    yield PrepResult('baz' + data)

class MultiCachePreparable(object):
  def work(self):
    order = [10, 20, 30, 40]
    random.shuffle(order)
    a = CacheFetcher(order[0], 'a')
    b = CacheFetcher(order[1], 'b')
    c = CacheFetcher(order[2], 'c')
    d = CacheFetcher(order[3], 'd')

    results = yield [a, b, c, d]

    yield PrepResult((order, results))

class MultiCacheStepPreparable(object):
  def work(self):
    order = [10, 20, 30, 40]
    random.shuffle(order)
    a = CacheFetcher(order[0], 'a')
    b = CacheFetcher(order[1], 'b')
    c = CacheFetcher(order[2], 'c')
    d = CacheFetcher(order[3], 'd')

    results_one = yield [a, b]
    results_two = yield [c, d]

    yield PrepResult((results_one, results_two))

class MultiDispatchPreparable(object):
  def work(self):
    order = [10, 20, 30, 40]
    random.shuffle(order)
    a = SimpleFetcher(order[0])
    b = SimpleFetcher(order[1])
    c = SimpleFetcher(order[2])
    d = SimpleFetcher(order[3])

    results = yield [a, b, c, d]

    yield PrepResult((order, results))
# }}}

if __name__ == "__main__":
  print("PREPARING")
  p = Preparer()
  able = MultiCachePreparable()
  bable = MultiCacheStepPreparable()
  cable = MultiDispatchPreparable()
  r = p.add(able.work)
  r = p.add(bable.work)
  r = p.add(cable.work)

  p.run()

  p.print_summary()
  print(p.cache)
