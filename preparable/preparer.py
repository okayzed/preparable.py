from __future__ import print_function

# The highlevel:
# Things: Preparables, PrepResults, PrepFetchers, Preparer
# What it provides:
#   Parallelized Data Fetches
#   Cached Values shared across Data Fetchers
#   Simple (relatively)  API

# {{{ DETAILED INFO
# HOW IT WORKS
#   The preparer has a main loop that runs until its Prepare Queue has finished.
#   When the preparer is given Preparables to run, they are added to the Prepare Queue
#   When a Preparable yields a PrepFetcher, the Preparer creates a Task that it adds to its Task Queue
#   When a Preparable yields multiple PrepFetchers, the Preparer creates a MultiTask that it adds to its Task Queue
#       MultiTasks contain multiple sub Tasks that they are responsible for coordinating
#   When a Preparable yields a PrepResult, the Preparable is considered finished and is removed from the Prepare Queue

# HOW THE PREPARER COORDINATES
# SCHEDULING SINGLE TASKS
#   When its time to run a task, the Preparer checks for its cache key
#     If the cache key does not exist,
#       the Preparer creates a pending queue for that cache key
#       the Preparer fetches the data for the cache key in an async function
#       when this function comes back, the Preparer caches the result and sends the data back to the Preparable
#       for each waiting task in the pending queue, the task is given the result and resolved
#     If the cache key exists:
#       the Preparer sends the data to the Preparable immediately
#     If there is a pending queue for that cache key:
#       the Preparer places the task and Preparable in that queue

# SCHEDULING MULTITASKS
#   When a multi task is scheduled, the Preparer determines which cache keys are missing or pending.
#     For missing cache keys, a pending queue is created and the fetcher is added to a list of tasks to run.
#     For pending cache keys, the MultiTask is given the task and cache key
#   The list of tasks to run are scheduled as a parallel async job.
#   When the results come back, the cache keys are propagated
#   When the multi task is queried for its finished state, it:
#     Checks that all pending cache keys are resolved
#     Checks that all pending async calls are resolved
# }}}

# {{{ IMPORTS
import time
import itertools
from . import pickle_methods
import multiprocessing
from collections import defaultdict

from .logging_pool import LoggingPool
from .logging_pool import LoggingPool
from .debuggable_class import Debuggable
from .fetcher import PrepFetcher
from .preparable import Preparable

# }}}

# {{{ RESULT AND TASKS CLASS
class PrepResult(Debuggable):
  def __init__(self, result):
    self.result = result

# The PreparerTask is used by Preparer to keep track of running tasks that it
# needs to wait on. A PreparerTask is used when single Preparables are yielded.

# The PreparerTask is meant to be mostly logicless - it defers to the Preparer,
# it just lets the Preparer know if it is finished or not.
class PreparerTask(Debuggable):
  def __init__(self, func=None, args=[], kwargs={}, cache_key=None):
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

  def finished(self):
    return self.async.get(0)

def handle_sub_task(sub_task):
  result = sub_task.fetch()
  return result

# PreparerMultiTask is used by the Preparer to keep track of multiple tasks at
# once (when an array of Preparables if yielded).
#
# It is responsible for watching any async calls used to fetch data for
# subtasks, as well as waiting for the cache to get filled for sub tasks that
# have a key pending.
# The PreparerMultiTask is meant to be mostly logicless, too
# (and defers to the Preparer)
class PreparerMultiTask(PreparerTask):
  def __init__(self, *args, **kwargs):
    super(PreparerMultiTask, self).__init__(*args, **kwargs)
    self.async = []
    self.on_done = []
    self.pending_cache = []

  def wait_for_cache(self, key, task):
    self.pending_cache.append((key, task))

  def set_cache(self, cache):
    self.cache = cache

  def when_done(self, func):
    self.on_done.append(func)

  def add_async(self, async):
    self.async.append(async)

  # Checks to see if all pending sub tasks are finished
  def finished(self):
    for key, task in self.pending_cache:
      if key not in self.cache:
        raise multiprocessing.TimeoutError()
      task.set_result(self.cache[key])

    for async in self.async:
      async.get(0)

    for cb in self.on_done:
      cb()

# }}}

# {{{ PREPARER
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

  def __del__(self):
    try:
      self.pool.close()
    except:
      pass

  # Initializes a Preparable and adds it to the Prepare queue
  # Calls the preparable once and handles the yielded value
  def init_preparable(self, preparable):
    prepare_cycle, args, kwargs = preparable
    data = prepare_cycle.start(*args, **kwargs)
    self.in_progress.add(prepare_cycle)
    self.unpack_and_handle(prepare_cycle, data)

  # Start all preparables
  def startup(self):
    self.executing = len(self.preparables)

    for preparable in self.preparables:
      self.init_preparable(preparable)

  def add(self, prepare_func, args=[], kwargs={}):
    if isinstance(prepare_func, Preparable):
      prepare_cycle = prepare_func
    elif callable(prepare_func):
      prepare_cycle = Preparable(prepare_func)
    else:
      raise NonPreparableException()

    self.preparables.append((prepare_cycle, args, kwargs))

    if self.executing:
      # start this thing up
      self.init_preparable(self.preparables[-1])

    return prepare_cycle

  # Entry point from the outside.
  def run(self):
    self.startup()
    self.sleep_spin()

  # called on the value Preparables yield.
  # figures out if the Preparable is finished.
  # figures out if a Task or MultiTask needs to be created
  # and starts Preparing it.
  def unpack_and_handle(self, prepare_cycle, data):
    if isinstance(data, PrepResult):
      prepare_cycle.set_result(data)
    elif isinstance(data, list):
      task = self.unpack_yielded_list(prepare_cycle, data)
      return
    elif data:
      task = self.unpack_yielded_preparable(prepare_cycle, data)
      if task.func:
        return self.handle_task(prepare_cycle, task)


    self.finished.append(prepare_cycle)
    self.in_progress.remove(prepare_cycle)
    self.finished_job()


  # create a Task for a Preparable
  def unpack_yielded_preparable(self, prepare_cycle, data):
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
      print("Preparable function yielded a non preparable idea")

    task = PreparerTask(first_func, args=args, kwargs=kwargs, cache_key=cache_key)
    if fetcher:
      task.set_fetcher(fetcher)

    return task


  # create a MultiTask for a preparable
  def unpack_yielded_list(self, prepare_cycle, prep_list):
    # TODO: validate prep_list is only preparables
    return self.handle_multi_task(prepare_cycle, prep_list)


  # propagate cache key results to pending Tasks.
  def propagate_cache_key(self, cache_key, cached=None):
    if not cached:
      if cache_key in self.caching:
        cached = self.caching[cache_key]

    if cached:
      result = self.cache[cache_key]
      for sub_task, cycle in cached:
        if isinstance(cycle, Preparable):
          self.debug("Passing cached result to", cycle, result)
          self.resume_preparable(sub_task, cycle, result)
        else:
          raise ShenanigansException()


  # handle task takes a PreparerTask and a Preparable and queues them up for
  # running
  def handle_task(self, prepare_cycle, task):
    first_func = None
    cache_key = task.cache_key
    if not task:
      raise MissingTaskException()

    def make_cb(self, prepare_cycle, cache_key):
      def cb(x):
        self.resume_preparable(task, prepare_cycle, x, cache_key)

      return cb

    if cache_key and cache_key in self.cache:
      self.resume_preparable(task, prepare_cycle, self.cache[cache_key])
    elif cache_key and cache_key in self.caching:
      self.caching[cache_key].append((task, prepare_cycle))
    else:
      task.async = self.pool.apply_async(task.func, task.args, task.kwargs, make_cb(self, prepare_cycle, cache_key))
      self.preparing.append(task)
      self.caching[cache_key] = []

    return task

  # Takes a list of tasks, creates a MultiTask and figures out
  # which tasks are resolved, which need to be run and which
  # are pending cache keys.

  # Gives the MultiTask the list of cache_keys that are pending
  # Gives the MultiTask the async job that is spawned
  # Tells the Preparer to wait for the MultiTask to resolve
  def handle_multi_task(self, prepare_cycle, tasks):
    to_prepare = []
    multitask = PreparerMultiTask()
    multitask.set_cache(self.cache)

    # i kind of want to put this logic in MultiTask, but...
    # the Preparer looks like it wants some of the info that
    # is being generated here, too.
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

        self.propagate_cache_key(cache_key)

    def make_cb(self, prepare_cycle, tasks):
      def cb():
        self.finished_job()
        self.resume_preparable(task, prepare_cycle, [t.get_result() for t in tasks])
      return cb

    self.preparing.append(multitask)
    multitask.when_done(make_cb(self, prepare_cycle,tasks))
    if len(to_prepare):
      multitask.add_async(self.pool.map_async(handle_sub_task, to_prepare, callback=update_cache))

    return multitask

  # This is the main loop for the preparer. It does a sleep spin
  def sleep_spin(self):
    start = time.time()
    while True:
      time.sleep(0.01)
      self.finish_tasks()
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

  # this is the async re-entry point for continuing the preparation of
  # Preparables. it coordinates with the Preparable, handing it the results
  # that are coming from the cache or fetcher (via the generator.send()) and
  # handles the next yielded value. (May be a result, may be a preparable, may
  # be many)
  def resume_preparable(self, task, prepare_cycle, result, cache_key=None):
    task.set_result(result)
    if cache_key:
      self.cache[cache_key] = result
      self.propagate_cache_key(cache_key)

    try:
      data = prepare_cycle.do_work(result)
      self.unpack_and_handle(prepare_cycle, data)
    except StopIteration:
      self.unpack_and_handle(prepare_cycle, None)

  def print_summary(self):
    print("------------------------")
    print("Finished preparing data!")
    print("Cached data:", self.cache)
    print("Errors: ", len(self.exceptions))
    print("Preparables run", len(self.finished))
    if self.exceptions:
      print("Exceptions", self.exceptions)

  # Check to see if all tasks are finished
  def finish_tasks(self):
    next_batch = []
    for task in self.preparing:
      try:
        if task.finished:
          res = task.finished();
          task.set_result(res)
      except multiprocessing.TimeoutError:
        next_batch.append(task)
      except Exception as e:
        self.exceptions.append(e)
        self.success = False
        self.finished_job()

    self.preparing = next_batch
# }}}

# {{{ EXCEPTIONS
class ShenanigansException(Exception):
  pass
class MissingTaskException(Exception):
  pass
class NonPreparableException(Exception):
  pass
# }}}

# vim: set foldmethod=marker
