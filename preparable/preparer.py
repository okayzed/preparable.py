import time
import itertools
import pickle_methods
import multiprocessing
from collections import defaultdict

from logging_pool import LoggingPool
from logging_pool import LoggingPool
from debuggable_class import Debuggable
from fetcher import PrepFetcher
from preparable import Preparable

class PrepResult(Debuggable):
  def __init__(self, result):
    self.result = result

# The PreparerTask is used by Preparer to keep track of running tasks that it
# needs to wait on. A PreparerTask is used for single Preparables yielded
class PreparerTask(Debuggable):
  __task_id = 0
  def __init__(self, func=None, args=[], kwargs={}, cache_key=None):
    PreparerTask.__task_id += 1
    self.task_id = PreparerTask.__task_id
    self.func = func
    self.args = args
    self.kwargs = kwargs
    self.cache_key = cache_key
    self.fetcher = None

  def set_fetcher(self, fetcher):
    self.fetcher = fetcher

  def set_result(self, res):
    if self.fetcher:
      self.fetcher.set_result(res)

    self.result = res


def handle_sub_task(sub_task):
  result = sub_task.fetch()
  return result

# PreparerMultiTask is used by the Preparer to keep track of multiple tasks at
# once (when an array of Preparables if yielded).
#
# It is responsible for watching any async calls used to fetch data for
# subtasks, as well as waiting for the cache to get filled for sub tasks that
# have a key pending.
class PreparerMultiTask(PreparerTask):
  def __init__(self, *args, **kwargs):
    super(PreparerMultiTask, self).__init__(*args, **kwargs)
    self.async = self # tricksy
    self.__async = []
    self.on_done = []
    self.pending_cache = []

  def wait_for_cache(self, key, task):
    self.pending_cache.append((key, task))

  def set_cache(self, cache):
    self.cache = cache

  def when_done(self, func):
    self.on_done.append(func)

  def add_async(self, async):
    self.__async.append(async)

  def get(self, timeout=0):
    for key, task in self.pending_cache:
      if key not in self.cache:
        raise multiprocessing.TimeoutError()
      task.set_result(self.cache[key])

    for async in self.__async:
      async.get(0)

    for cb in self.on_done:
      cb()


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
    self.executing = None

    self.in_progress = set()
    self.finished = []

  def init_preparable(self, preparable):
    prepare_cycle, args, kwargs = preparable
    data = prepare_cycle.start(*args, **kwargs)
    self.in_progress.add(prepare_cycle)
    self.unpack_and_handle(prepare_cycle, data)

  def startup(self):
    self.executing = len(self.preparables)

    for preparable in self.preparables:
      self.init_preparable(preparable)

    ret = self.spin()
    return ret

  def add(self, prepare_func, args=[], kwargs={}):
    if isinstance(prepare_func, Preparable):
      prepare_cycle = prepare_func
    elif callable(prepare_func):
      prepare_cycle = Preparable(prepare_func)
    else:
      raise Exception("Trying to add a non-preparable to a preparer")

    self.preparables.append((prepare_cycle, args, kwargs))

    if self.executing:
      # start this thing up
      self.init_preparable(self.preparables[-1])

    return prepare_cycle

  def run(self):
    self.startup()
    self.spin()

  def unpack_and_handle(self, prepare_cycle, data):
    if isinstance(data, PrepResult):
      prepare_cycle.set_result(data)
    elif isinstance(data, list):
      task = self.unpack_list(prepare_cycle, data)
      return
    elif data:
      task = self.unpack_preparable(prepare_cycle, data)
      if task.func:
        return self.handle_task(prepare_cycle, task)


    self.finished.append(prepare_cycle)
    self.in_progress.remove(prepare_cycle)
    self.finished_job()



  def unpack_preparable(self, prepare_cycle, data):
    args = []
    kwargs = {}
    cache_key = None
    first_func = None
    fetcher = None

    if isinstance(data, dict):
      first_func = data.get('func')
      args = data.get('args', [])
      kwargs = data.get('kwargs', {})
      cache_key = data.get('cache_key')
    elif isinstance(data, PrepFetcher):
      first_func = data.fetch
      cache_key = data.get_cache_key()
      fetcher = data
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

    task = PreparerTask(first_func, args=args, kwargs=kwargs, cache_key=cache_key)
    if fetcher:
      task.set_fetcher(fetcher)

    return task


  def unpack_list(self, prepare_cycle, prep_list):
    # TODO: validate prep_list is only preparables
    return self.handle_multi_task(prepare_cycle, prep_list)

  def rinse_cache(self, cache_key):
    if cache_key in self.caching:
      cached = self.caching[cache_key]
      result = self.cache[cache_key]
      for sub_task, cycle in cached:
        if isinstance(cycle, Preparable):
          self.debug("Passing cached result to", cycle, result)
          self.run_next_func(sub_task, cycle, result)
        else:
          raise Exception("What type of shenanigans is this")


  # handle task takes a PreparerTask and a Preparable and queues them up for
  # running
  def handle_task(self, prepare_cycle, task):
    first_func = None
    cache_key = task.cache_key
    if not task:
      raise "YOU HANDED NO TASK HERE"

    def make_cb(self, prepare_cycle, cache_key):
      def cb(x):
        self.run_next_func(task, prepare_cycle, x, cache_key)

      return cb

    if cache_key and cache_key in self.cache:
      self.run_next_func(task, prepare_cycle, self.cache[cache_key])
    elif cache_key and cache_key in self.caching:
      self.caching[cache_key].append((task, prepare_cycle))
    else:
      task.async = self.pool.apply_async(task.func, task.args, task.kwargs, make_cb(self, prepare_cycle, cache_key))
      self.preparing.append(task)
      self.caching[cache_key] = []

    return task

  # Takes a list of tasks and queues them up to run
  def handle_multi_task(self, prepare_cycle, tasks):
    to_prepare = []
    multitask = PreparerMultiTask()
    multitask.set_cache(self.cache)

    for task in tasks:
      cache_key = task.get_cache_key()
      if cache_key and cache_key in self.cache:
        task.set_result(self.cache[cache_key])
      elif cache_key and cache_key in self.caching:
        multitask.wait_for_cache(cache_key, task)
      else:
        self.caching[cache_key] = []
        to_prepare.append(task)

    self.executing += 1

    def update_cache(results):
      if results:
        for i, value in enumerate(results):
          to_prepare[i].set_result(value)

      for task in tasks:
        result = task.get_result()
        cache_key = task.get_cache_key()

        if result and not cache_key in self.cache:
          self.cache[cache_key] = result

        self.rinse_cache(cache_key)

    def make_cb(self, prepare_cycle, tasks):
      def cb():
        self.finished_job()
        self.run_next_func(task, prepare_cycle, map(lambda t: t.get_result(), tasks))
      return cb

    self.preparing.append(multitask)
    multitask.when_done(make_cb(self, prepare_cycle,tasks))
    if len(to_prepare):
      multitask.add_async(self.pool.map_async(handle_sub_task, to_prepare, callback=update_cache))

    return multitask

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
      if not len(self.exceptions):
        self.success = True
      self.done = True
      for done in self.when_done:
        done()

  def run_next_func(self, task, prepare_cycle, result, cache_key=None):
    task.set_result(result)
    if cache_key:
      self.cache[cache_key] = result

    cached = []
    if cache_key in self.caching:
      cached = self.caching[cache_key]
      del self.caching[cache_key]

    self.rinse_cache(cache_key)

    try:
      data = prepare_cycle.do_work(result)
      self.unpack_and_handle(prepare_cycle, data)
    except StopIteration:
      self.unpack_and_handle(prepare_cycle, None)

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
    for task in self.preparing:
      try:
        if task.async:
          res = task.async.get(0);
          task.set_result(res)
      except multiprocessing.TimeoutError:
        next_batch.append(task)
      except Exception, e:
        self.exceptions.append(e)
        self.success = False
        self.finished_job()

    self.preparing = next_batch
