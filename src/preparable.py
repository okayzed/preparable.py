import time
import pickle_methods
import multiprocessing
from collections import defaultdict


from logging_pool import LoggingPool
DEBUG = False
def debug(*args):
  if DEBUG:
    print " ".join(map(str, args))

class Preparable(object):
  def __init__(self, multi_func, cache_key=None):
    self.func = multi_func
    self.cache_key = cache_key
    self.generator = None

  def __repr__(self):
    return "Preparable: 0x%0xf" % abs(hash(self))

  def start(self, *args, **kwargs):
    generator = self.func(*args, **kwargs)
    if generator:
      try:
        next_func = generator.next()
      except StopIteration:
        pass
      except Exception, e:
        debug("ERROR WHILE RUNNING GENERATOR", e)
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
        debug("ERROR WHILE RUNNING GENERATOR", e)
        self.generator.throw(e)
        return

      return next_func
    else:
      debug("WHERE DID GENERATOR GO?", self)

class Preparer(object):
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
    self.preparables.append((prepare_cycle, args, kwargs))
    debug("ADDING PREPARABLE", prepare_cycle)

  def run(self):
    debug("RUNNING", len(self.preparables), "JOBS")
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
      debug("RUNNING PREPARABLE", preparable)
      prepare_cycle, args, kwargs = preparable
      data = prepare_cycle.start(*args, **kwargs)
      if not data:
        debug("Finished Execution Early", hash(prepare_cycle))
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

      if cache_key in self.cache:
        self.run_next_func(prepare_cycle, self.cache[cache_key])
      elif cache_key in self.caching:
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
      debug('ALL DONE')
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

      for data in cached:
        self.run_next_func(prepare_cycle, result)
    try:
      data = prepare_cycle.do_work(result)
      res = self.unpack(data)
      if res:
        next_func, args, kwargs, cache_key = res
    except StopIteration:
      debug("Finished Execution", prepare_cycle)
      self.finished_job()
      return

    if next_func:
      if cache_key in self.cache:
        self.run_next_func(prepare_cycle, self.cache[cache_key])
      elif cache_key in self.caching:
        self.caching[cache_key].append((next_func, args, kwargs))
      else:
        result = self.pool.apply_async(next_func, args, {}, lambda y: self.run_next_func(prepare_cycle, y, cache_key))
        self.preparing.append(result)
        self.caching[cache_key] = []
    else:
      self.finished_job()

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

