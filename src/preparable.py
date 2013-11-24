import time
import pickle_methods
import multiprocessing
from collections import defaultdict

from logging_pool import LoggingPool
from debuggable_class import Debuggable

# The Preparable class is an abstraction around
# parallelizing data fetches. The function
# passed to the Preparable can fetch data in a 
# way that looks linear but is parallelized and
# asychronous under the hood.
class Preparable(Debuggable):
  def __init__(self, multi_func, cache_key=None):
    self.func = multi_func
    self.cache_key = cache_key
    self.generator = None

  def __repr__(self):
    return "Preparable: 0x%0xf %s" % (abs(hash(self)), self.func.__name__)

  def start(self, *args, **kwargs):
    generator = self.func(*args, **kwargs)
    if generator:
      try:
        next_func = generator.next()
      except StopIteration:
        pass
      except Exception, e:
        self.debug("ERROR WHILE RUNNING GENERATOR", e)
        generator.throw(e)

      self.generator = generator
      return next_func

  def do_work(self, data):
    if self.generator:
      try:
        next_func = self.generator.send(data)
      except StopIteration, e:
        raise e
      except Exception, e:
        self.debug("ERROR WHILE RUNNING GENERATOR", e)
        self.generator.throw(e)
        return

      return next_func
    else:
      self.debug("WHERE DID GENERATOR GO?", self)

# The Preparer runs multiple Preparables in Parallel.
class Preparer(Debuggable):
  def __init__(self, *args, **kwargs):
    self.pool = LoggingPool(processes=kwargs.get('processes', None)) # Figure it out!
    self.when_done = []
    self.preparables = []
    self.preparing = []
    self.done = False
    self.exceptions = []
    self.cache = {}
    self.caching = defaultdict(list)

  def add(self, prepare_cycle, args=[], kwargs={}):
    self.debug("ADDING PREPARABLE", prepare_cycle)
    self.preparables.append((prepare_cycle, args, kwargs))

  def run(self):
    self.debug("RUNNING", len(self.preparables), "JOBS")
    self.startup()
    self.spin()

  def unpack(self, data):
    if isinstance(data, dict):
      first_func = data.get('func')
      args = data.get('args', [])
      kwargs = data.get('kwargs', {})
      cache_key = data.get('cache_key')
    elif callable(data):
      first_func = data
      args = []
      kwargs = {}
      cache_key = None
    elif isinstance(data, tuple):
      self.debug("USING TUPLE")
      args = []
      kwargs = {}
      cache_key = None
      first_func = None

      if len(data) > 0:
        first_func = data[0]
      if len(data) > 1:
        args = data[1]
      if len(data) > 2:
        kwargs = data[2]
      if len(data) > 3:
        cache_key = data[3]
    else:
      print "Preparable function yielded a non preparable idea"

    return first_func, args, kwargs, cache_key

  def startup(self):
    self.executing = len(self.preparables)


    # This loop has some repeated code because of 
    # the callback creation inside of it necessary
    # for closures. Without the closures, the loop
    # would really only execute teh same thing multiple
    # times.
    for preparable in self.preparables:
      self.debug("RUNNING PREPARABLE", preparable)
      prepare_cycle, args, kwargs = preparable
      data = prepare_cycle.start(*args, **kwargs)
      if not data:
        self.debug("Finished Execution Early", hash(prepare_cycle))
        self.finished_job()
        continue

      res = self.unpack(data)
      if res:
        first_func, args, kwargs, cache_key = res
      else:
        return

      def make_cb(self, prepare_cycle, cache_key):
        def cb(x):
          self.run_next_func(prepare_cycle, x, cache_key)

        return cb

      if cache_key and cache_key in self.cache:
        self.run_next_func(prepare_cycle, self.cache[cache_key])
      elif cache_key and cache_key in self.caching:
        self.caching[cache_key].append(prepare_cycle)
      else:
        result = self.pool.apply_async(first_func, args, kwargs, make_cb(self, prepare_cycle, cache_key))
        self.preparing.append(result)
        self.caching[cache_key] = []

    ret = self.spin()
    return ret

  def spin(self):
    while True:
      time.sleep(0.01)
      self.find_exceptions()
      if self.done:
        break

  def finished_job(self):
    self.executing -= 1
    if self.executing == 0:
      self.debug('ALL DONE')
      if not len(self.exceptions):
        self.success = True
      self.done = True
      for done in self.when_done:
        done()

  def run_next_func(self, prepare_cycle, result, cache_key=None):
    next_func = None
    if cache_key:
      self.cache[cache_key] = result

    if cache_key in self.caching:
      cached = self.caching[cache_key]
      del self.caching[cache_key]

      for cycle in cached:
        self.debug("Passing cached result to", cycle, result)
        self.run_next_func(cycle, result)
    try:
      self.debug("Running next func for", prepare_cycle)
      data = prepare_cycle.do_work(result)
      res = self.unpack(data)
      if res:
        next_func, args, kwargs, cache_key = res
    except StopIteration:
      self.debug("Finished Execution", prepare_cycle)
      self.finished_job()
      return

    if next_func:
      if cache_key in self.cache:
        self.run_next_func(prepare_cycle, self.cache[cache_key])
      elif cache_key in self.caching:
        self.caching[cache_key].append(prepare_cycle)
      else:
        result = self.pool.apply_async(next_func, args, kwargs, lambda y: self.run_next_func(prepare_cycle, y, cache_key))
        self.preparing.append(result)
        self.caching[cache_key] = []
    else:
      self.finished_job()

  def print_summary(self):
    print "------------------------"
    print "Finished preparing data!"
    print "Cached data:", self.cache
    print "Errors: ", len(self.exceptions)
    print "Preparables run", len(self.preparables)
    if self.exceptions:
      print "Exceptions", self.exceptions

  def find_exceptions(self):
    next_batch = []
    for async in self.preparing:
      try:
        res = async.get(0);
      except multiprocessing.TimeoutError:
        next_batch.append(async)
      except Exception, e:
        self.exceptions.append(e)
        self.success = False
        self.finished_job()

    self.preparing = next_batch

