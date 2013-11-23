import time
import pickle_methods
import multiprocessing
from multiprocessing.pool import Pool

multiprocessing.log_to_stderr()

DEBUG = False
def debug(*args):
  if DEBUG:
    print " ".join(map(str, args))

# Pulled from somewhere
import traceback
import multiprocessing

# Shortcut to multiprocessing's logger
def error(msg, *args):
    return multiprocessing.get_logger().error(msg, *args)

class LogExceptions(object):
    def __init__(self, callable):
        self.__callable = callable
        return

    def __call__(self, *args, **kwargs):
        try:
            result = self.__callable(*args, **kwargs)

        except Exception as e:
            # Here we add some debugging help. If multiprocessing's
            # debugging is on, it will arrange to log the traceback
            error(traceback.format_exc())
            # Re-raise the original exception so the Pool worker can
            # clean up
            raise

        # It was fine, give a normal answer
        return result
    pass

class LoggingPool(Pool):
    def apply_async(self, func, args=(), kwds={}, callback=None):
        return Pool.apply_async(self, LogExceptions(func), args, kwds, callback)


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
        debug("ERROR WHILE RUNNING GENERATOR", Exception, e)
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
    elif callable(data):
      first_func = data
      args = []
      kwargs = {}
    else:
      print "Preparable function yielded a non preparable idea"

    return first_func, args, kwargs

  def startup(self):
    self.executing = len(self.preparables)
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
        first_func, args, kwargs = res
      else:
        return

      def make_cb(self, prepare_cycle):
        def cb(x):
          self.next_func(prepare_cycle, x)

        return cb
      result = self.pool.apply_async(first_func, args, kwargs, make_cb(self, prepare_cycle))
      self.preparing.append(result)

    ret = self.spin()
    return ret

  def spin(self):
    while True:
      time.sleep(0.01)
      self.find_exceptions()
      if self.done:
        break

  def on_done(self, done_func):
    self.when_done.append(done_func)

  def finished_job(self):
    self.executing -= 1
    if self.executing == 0:
      debug('ALL DONE')
      if not len(self.exceptions):
        self.success = True
      self.done = True
      for done in self.when_done:
        done()

  def next_func(self, prepare_cycle, result):
    next_func = None
    try:
      data = prepare_cycle.do_work(result)
      res = self.unpack(data)
      if res:
        next_func, args, kwargs = res
    except StopIteration:
      debug("Finished Execution", prepare_cycle)
      self.finished_job()
      return

    if next_func:
      result = self.pool.apply_async(next_func, args, {}, lambda y: self.next_func(prepare_cycle, y))
      self.preparing.append(result)
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

