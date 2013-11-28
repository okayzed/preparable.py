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
      self.fetcher.set_data(res)

    self.result = res

class PreparerMultiTask(PreparerTask):
  def __init__(self, *args, **kwargs):
    super(PreparerMultiTask, self).__init__(*args, **kwargs)

  def finish(self, value):
    self.set_result(value)

# The Preparer runs multiple Preparables in Parallel.
def handle_sub_task(sub_task):
  result = sub_task.fetch()
  return result

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

  def add(self, prepare_func, args=[], kwargs={}):
    if isinstance(prepare_func, Preparable):
      prepare_cycle = prepare_func
    elif callable(prepare_func):
      prepare_cycle = Preparable(prepare_func)
    else:
      raise "Trying to add a non-preparable to a preparer"

    self.debug("ADDING PREPARABLE", prepare_cycle)

    self.preparables.append((prepare_cycle, args, kwargs))

    if self.executing:
      # start this thing up
      self.init_preparable(self.preparables[-1])

    return prepare_cycle

  def run(self):
    self.debug("RUNNING", len(self.preparables), "JOBS")
    self.startup()
    self.spin()

  def unpack_and_handle(self, prepare_cycle, data):
    if isinstance(data, PrepResult):
      prepare_cycle.set_result(data)
    elif isinstance(data, list):
      task = self.unpack_list(prepare_cycle, data)
      # What happens here?
      return
    elif data:
      task = self.unpack_preparable(prepare_cycle, data)
      if task.func:
        return self.handle_task(prepare_cycle, task)


    self.debug("Finished Execution", prepare_cycle)
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

  def init_preparable(self, preparable):
    self.debug("RUNNING PREPARABLE", preparable)
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
      self.caching[cache_key].append(prepare_cycle)
    else:
      task.async = self.pool.apply_async(task.func, task.args, task.kwargs, make_cb(self, prepare_cycle, cache_key))
      self.preparing.append(task)
      self.caching[cache_key] = []

    return task

  def handle_multi_task(self, prepare_cycle, tasks):
    to_prepare = []
    cached_results = []
    for task in tasks:
      cache_key = task.get_cache_key()
      if cache_key and cache_key in self.cache:
        task.set_result(self.cache[cache_key])
        cached_results.append(self.cache[cache_key])

      elif cache_key and cache_key in self.caching:
        # FOR NOW, LETS JUST IGNORE THIS PROBLEM
        self.debug("Ignoring caching fetch in multi-prep, doing a redundant fetch")
        to_prepare.append(task)
      else:
        to_prepare.append(task)

    self.executing += 1
    task = PreparerMultiTask()
    def make_cb(self, prepare_cycle, tasks):
      def cb(results):
        for i, value in enumerate(results):
          tasks[i].set_data(value)

        self.finished_job()

        self.run_next_func(task, prepare_cycle, itertools.chain(cached_results, results))
      return cb

    self.preparing.append(task)
    task.async = self.pool.map_async(handle_sub_task, to_prepare, callback=make_cb(self, prepare_cycle, tasks))


    return task

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

  def run_next_func(self, task, prepare_cycle, result, cache_key=None):
    task.set_result(result)
    if cache_key:
      self.cache[cache_key] = result

    if cache_key in self.caching:
      cached = self.caching[cache_key]
      del self.caching[cache_key]

      for cycle in cached:
        if isinstance(cycle, Preparable):
          self.debug("Passing cached result to", cycle, result)
          self.run_next_func(task, cycle, result)
        if isinstance(cycle, PreparerMultiTask):
          self.run_next_func(task, cycle, result)

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
        res = task.async.get(0);
        task.set_result(res)
      except multiprocessing.TimeoutError:
        next_batch.append(task)
      except Exception, e:
        self.exceptions.append(e)
        self.success = False
        self.finished_job()

    self.preparing = next_batch
