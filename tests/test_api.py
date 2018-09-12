import sys
sys.path.append('..')

import unittest
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
class SimplePreparable(object):
  def work(self):
    fetcher = SimpleFetcher(10)
    data = yield fetcher

    yield PrepResult(data)

class CachePreparable(object):
  def work(self):
    fetcher = CacheFetcher('bar', 'foo')
    one = yield fetcher

    second_fetcher = CacheFetcher('baz', 'a')
    two = yield second_fetcher

    yield PrepResult(str(two) + str(one))

class MultiStepPreparable(object):
  def work(self):
    fetcher = SimpleFetcher(10)
    data = yield fetcher
    data_two = yield SimpleFetcher(data * data)

    yield PrepResult(data_two)

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

# {{{ TESTS
class TestPreparerAPI(unittest.TestCase):
  def setUp(self):
    self.prep = Preparer()

  def test_success(self):

    able = SimplePreparable()
    ret = self.prep.add(able.work)
    self.prep.run()

    self.assertEqual(ret.get_result(), 10)
    self.assertEqual(self.prep.success, True)
    self.assertEqual(len(self.prep.exceptions), 0)
    self.assertEqual(len(self.prep.finished), 1)

  def test_multi_step(self):
    able = MultiStepPreparable()
    ret = self.prep.add(able.work)
    self.prep.run()

    self.assertEqual(ret.get_result(), 100)
    self.assertEqual(self.prep.success, True)
    self.assertEqual(len(self.prep.exceptions), 0)
    self.assertEqual(len(self.prep.finished), 1)

  def test_multi_dispatch(self):
    able = MultiDispatchPreparable()
    ret = self.prep.add(able.work)
    self.prep.run()

    results, order = ret.get_result()
    self.assertEqual(results, order)
    self.assertEqual(self.prep.success, True)
    self.assertEqual(len(self.prep.exceptions), 0)
    self.assertEqual(len(self.prep.finished), 1)

  def test_several_preps(self):
    results = []
    iters = 40
    for i in range(iters):
      able = MultiDispatchPreparable()
      ret = self.prep.add(able.work)
      results.append(ret)

    self.prep.run()
    found_results = [r.get_result() for r in results]
    self.assertEqual(len(found_results), iters)
    for result in results:
      results, order = result.get_result()
      self.assertEqual(results, order)

    self.assertEqual(self.prep.success, True)
    self.assertEqual(len(self.prep.exceptions), 0)
    self.assertEqual(len(self.prep.finished), iters)

  def test_cache(self):
    able = CachePreparable()
    ret = self.prep.add(able.work)
    self.prep.run()

    self.assertEqual(ret.get_result(), 'bazbar')
    self.assertEqual(self.prep.cache['foo'], 'bar')
    self.assertEqual(self.prep.success, True)
    self.assertEqual(len(self.prep.exceptions), 0)
    self.assertEqual(len(self.prep.finished), 1)

  def test_multi_cache(self):
    able = MultiCachePreparable()
    ret = self.prep.add(able.work)
    self.prep.run()

    results, order = ret.get_result()

    self.assertEqual(results, order)
    self.assertTrue('a' in self.prep.cache)
    self.assertTrue('b' in self.prep.cache)
    self.assertTrue('c' in self.prep.cache)
    self.assertTrue('d' in self.prep.cache)

    self.assertEqual(self.prep.success, True)
    self.assertEqual(len(self.prep.exceptions), 0)
    self.assertEqual(len(self.prep.finished), 1)

  def test_multi_cache_step(self):
    able = MultiCacheStepPreparable()
    ret = self.prep.add(able.work)
    self.prep.run()

    res1, res2 = ret.get_result()

    self.assertNotEqual(res1, res2)
    self.assertTrue('a' in self.prep.cache)
    self.assertTrue('b' in self.prep.cache)
    self.assertTrue('c' in self.prep.cache)
    self.assertTrue('d' in self.prep.cache)

    self.assertEqual(self.prep.cache['a'], res1[0])
    self.assertEqual(self.prep.cache['b'], res1[1])
    self.assertEqual(self.prep.cache['c'], res2[0])
    self.assertEqual(self.prep.cache['d'], res2[1])

    self.assertEqual(self.prep.success, True)
    self.assertEqual(len(self.prep.exceptions), 0)
    self.assertEqual(len(self.prep.finished), 1)

  def test_multi_cache_many(self):
    iters = 30
    results = []
    for i in range(iters):
      able = MultiCachePreparable()
      results.append(self.prep.add(able.work))

    self.prep.run()

    self.assertTrue('a' in self.prep.cache)
    self.assertTrue('b' in self.prep.cache)
    self.assertTrue('c' in self.prep.cache)
    self.assertTrue('d' in self.prep.cache)

    res0 = results[0].get_result()[1]
    for result in results:
        self.assertEqual(res0, result.get_result()[1])

    self.assertEqual(self.prep.success, True)
    self.assertEqual(len(self.prep.exceptions), 0)
    self.assertEqual(len(self.prep.finished), iters)

  def test_single_then_multi_cache(self):
    bable = CachePreparable()
    bable_ret = self.prep.add(bable.work)
    able = MultiCachePreparable()
    able_ret = self.prep.add(able.work)

    self.prep.run()

    a_val = self.prep.cache['a']
    self.assertEqual(bable_ret.get_result(), str(a_val) + 'bar')

  def test_multi_then_single(self):
    able = MultiCachePreparable()
    able_ret = self.prep.add(able.work)
    bable = CachePreparable()
    bable_ret = self.prep.add(bable.work)

    self.prep.run()

    a_val = self.prep.cache['a']
    self.assertEqual(bable_ret.get_result(), str(a_val) + 'bar')

  def test_bad_prep(self):
    able = SimplePreparable()
    error = False
    try:
      ret = self.prep.add(able)
    except:
      error = True

    self.assertEqual(error, True)

# }}}
if __name__ == "__main__":
  unittest.main()
