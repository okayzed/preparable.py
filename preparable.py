from multiprocessing import Pool

DEBUG = False
def debug(*args):
  if DEBUG:
    print " ".join(map(str, args))

class Preparable(object):
  def __init__(self, multi_func):
    self.func = multi_func
    self.generator = None

  def __repr__(self):
    return "REPR: " + repr(self.func)

  def start(self, *args, **kwargs):
    generator = self.func(*args, **kwargs)
    if generator:
      try:
        next_func = generator.next()
      except Exception, e:
        generator.throw(e)
      self.generator = generator
      return next_func

  def do_work(self, data):
    if self.generator:
      try:
        next_func = self.generator.send(data)
      except Exception, e:
        self.generator.throw(e)

      return next_func
    else:
      debug("WHERE DID GENERATOR GO?", self)

class Preparer(object):
  def __init__(self, *args, **kwargs):
    self.pool = Pool(processes=None) # Figure it out!
    self.when_done = []
    self.preparables = []

  # add a preparable means:
  #  get the first func to async
  #  run it, get the results
  #  pass results to callback
  def add(self, prepare_cycle, args=[], kwargs={}):
    self.preparables.append((prepare_cycle, args, kwargs))
    debug("ADDING PREPARABLE", prepare_cycle)

  def run(self):
    debug("RUNNING", len(self.preparables), "JOBS")
    self.executing = len(self.preparables)
    for preparable in self.preparables:
      debug("RUNNING PREPARABLE", preparable)
      prepare_cycle, args, kwargs = preparable
      data = prepare_cycle.start(*args, **kwargs)
      if not data:
        debug("Finished Execution Early")
        self.finished_job()
        continue

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

      def make_cb(self, prepare_cycle):
        def cb(x):
          self.next_func(prepare_cycle, x)

        return cb
      self.pool.apply_async(first_func, args, kwargs, make_cb(self, prepare_cycle))

  def on_done(self, done_func):
    self.when_done.append(done_func)

  def finished_job(self):
    self.executing -= 1
    if self.executing == 0:
      debug('ALL DONE')
      for done in self.when_done:
        done()

  def next_func(self, prepare_cycle, result):
    next_func = None
    try:
      data = prepare_cycle.do_work(result)
      if data:
        next_func = data.get('func')
        args = data.get('args', [])
        kwargs = data.get('kwargs', {})
    except StopIteration:
      debug("Finished Execution")
      self.finished_job()
      return

    if next_func:
      self.pool.apply_async(next_func, args, {}, lambda y: self.next_func(prepare_cycle, y))
    else:
      self.finished_job()

