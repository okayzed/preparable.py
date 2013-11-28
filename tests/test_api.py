import unittest
import sys
sys.path.append('..')

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
    data = yield fetcher

    yield PrepResult('baz' + data)

class MultiStepPreparable(object):
  def work(self):
    fetcher = SimpleFetcher(10)
    data = yield fetcher
    data_two = yield SimpleFetcher(data * data)

    yield PrepResult(data_two)

class MultiDispatchPreparable(object):
  def work(self):
    a = SimpleFetcher(10)
    b = SimpleFetcher(20)
    c = SimpleFetcher(30)
    d = SimpleFetcher(40)

    results = yield [a, b, c, d]

    yield PrepResult(results)
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

    self.assertEqual(ret.get_result(), [10, 20, 30, 40])
    self.assertEqual(self.prep.success, True)
    self.assertEqual(len(self.prep.exceptions), 0)
    self.assertEqual(len(self.prep.finished), 1)

  def test_several_preps(self):
    results = []
    for i in xrange(30):
      able = MultiDispatchPreparable()
      ret = self.prep.add(able.work)
      results.append(ret)

    self.prep.run()
    found_results = map(lambda r: r.get_result(), results)
    self.assertEqual(len(found_results), 30)
    self.assertEqual(self.prep.success, True)
    self.assertEqual(len(self.prep.exceptions), 0)
    self.assertEqual(len(self.prep.finished), 30)

  def test_cache(self):
    able = CachePreparable()
    ret = self.prep.add(able.work)
    self.prep.run()

    self.assertEqual(ret.get_result(), 'bazbar')
    self.assertEqual(self.prep.cache['foo'], 'bar')
    self.assertEqual(self.prep.success, True)
    self.assertEqual(len(self.prep.exceptions), 0)
    self.assertEqual(len(self.prep.finished), 1)



  def test_bad_prep(self):
    able = SimplePreparable()
    error = False
    try:
      ret = self.prep.add(able)
    except:
      error = True

    self.assertEqual(error, True)

class TestFetcherAPI(unittest.TestCase):
  def setUp(self):
    pass

class TestPreparableAPI(unittest.TestCase):
  def setUp(self):
    pass

  def test_shuffle(self):
    pass


# }}}
if __name__ == "__main__":
  unittest.main()
