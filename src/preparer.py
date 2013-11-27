import time
import pickle_methods
import multiprocessing
from collections import defaultdict

from logging_pool import LoggingPool
from logging_pool import LoggingPool
from debuggable_class import Debuggable
from fetcher import PrepFetcher

# The Preparer runs multiple Preparables in Parallel.
class Preparer(Debuggable):
  DEBUG=False
  def __init__(self, *args, **kwargs):
    self.pool = LoggingPool(processes=kwargs.get('processes', None)) # Figure it out!
    self.when_done = []
    self.preparables = []
    self.preparing = []
    self.done = False
    self.exceptions = []
    self.cache = {}
    self.caching = defaultdict(list)
    self.finished_tasks = defaultdict(bool)
    self.executing = None
    self.__task_id = 0

    self.in_progress = set()
    self.finished = []

  def add(self, prepare_cycle, args=[], kwargs={}):
    self.debug("ADDING PREPARABLE", prepare_cycle)

    self.preparables.append((prepare_cycle, args, kwargs))

    if self.executing:
      # start this thing up
      self.init_preparable(self.preparables[-1])

  def run(self):
    self.debug("RUNNING", len(self.preparables), "JOBS")
    self.startup()
    self.spin()

  def unpack(self, prepare_cycle, data):
    if isinstance(data, list):
      raise Exception("Children preparables aren't implemented yet")
      self.unpack_list(prepare_cycle, data)
    else:
      return self.unpack_preparable(prepare_cycle, data)


  def unpack_preparable(self, prepare_cycle, data):
    args = []
    kwargs = {}
    cache_key = None
    first_func = None
    if isinstance(data, dict):
      first_func = data.get('func')
      args = data.get('args', [])
      kwargs = data.get('kwargs', {})
      cache_key = data.get('cache_key')
    elif isinstance(data, PrepFetcher):
      first_func = data.fetch
      cache_key = data.get_cache_key()
    elif callable(data):
      first_func = data
    elif isinstance(data, tuple):
      self.debug("USING TUPLE")
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


  def unpack_list(self, prepare_cycle, prep_list):
    if isinstance(prep_list, list):
      waits = []
      for p in prep_list:
        res = self.unpack(prepare_cycle, p)

        waits.append(self.handle_task(prepare_cycle, res))

      def wait_for(waits):
        def waiter():
          while True:
            time.sleep(0.01)
            finished = True
            for w in waits:
              if not w in self.finished_tasks:
                finished = False

            if finished:
              print "FINISHED WAITING FOR", waits
              break

        return waiter

      print "WAITING FOR", waits
      return wait_for(waits)


  def init_preparable(self, preparable):
    self.debug("RUNNING PREPARABLE", preparable)
    prepare_cycle, args, kwargs = preparable
    data = prepare_cycle.start(*args, **kwargs)
    self.in_progress.add(prepare_cycle)
    if not data:
      self.debug("Finished Execution Early", hash(prepare_cycle))
      self.finished.append(prepare_cycle)
      self.finished_job()
      return

    res = self.unpack(prepare_cycle, data)
    self.handle_task(prepare_cycle, res)

  def startup(self):
    self.executing = len(self.preparables)


    # This loop has some repeated code because of
    # the callback creation inside of it necessary
    # for closures. Without the closures, the loop
    # would really only execute teh same thing multiple
    # times.
    for preparable in self.preparables:
      self.init_preparable(preparable)

    ret = self.spin()
    return ret

  def handle_task(self, prepare_cycle, res=None):
    self.__task_id += 1
    task_id = self.__task_id

    first_func = None
    if res:
      first_func, args, kwargs, cache_key = res
    else:
      self.finished_tasks[task_id] = True
      return task_id

    def make_cb(self, prepare_cycle, cache_key):
      def cb(x):
        self.run_next_func(prepare_cycle, x, cache_key)

      return cb

    if cache_key and cache_key in self.cache:
      self.finished_tasks[task_id] = True
      self.run_next_func(prepare_cycle, self.cache[cache_key])
    elif cache_key and cache_key in self.caching:
      self.caching[cache_key].append(prepare_cycle)
    else:
      result = self.pool.apply_async(first_func, args, kwargs, make_cb(self, prepare_cycle, cache_key))
      self.preparing.append(result)
      self.caching[cache_key] = []

    return task_id

  def spin(self):
    start = time.time()
    while True:
      time.sleep(0.01)
      self.find_exceptions()
      delta = time.time() - start
      if self.done or not self.preparing:
        break

  def finished_job(self):
    self.executing -= 1
    if self.executing == 0 or not self.preparing:
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

    res = None
    try:
      self.debug("Running next func for", prepare_cycle)
      data = prepare_cycle.do_work(result)
      res = self.unpack(prepare_cycle, data)
    except StopIteration:
      next_func = None

    if res:
      self.handle_task(prepare_cycle, res)
    else:
      self.debug("Finished Execution", prepare_cycle)
      self.finished.append(prepare_cycle)
      self.in_progress.remove(prepare_cycle)
      self.finished_job()

  def print_summary(self):
    print "------------------------"
    print "Finished preparing data!"
    print "Cached data:", self.cache
    print "Errors: ", len(self.exceptions)
    print "Preparables run", len(self.finished)
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

