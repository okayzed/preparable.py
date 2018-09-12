from __future__ import print_function

from .debuggable_class import Debuggable

# The Preparable class is an abstraction around parallelizing data fetches. The
# function passed to the Preparable can fetch data in a way that looks linear
# but is parallelized and asychronous under the hood.
class Preparable(Debuggable):
  def __init__(self, multi_func, cache_key=None):
    self.func = multi_func
    self.cache_key = cache_key
    self.generator = None
    self.result = None

  def __repr__(self):
    return "Preparable: 0x%0xf %s" % (abs(hash(self)), self.func.__name__)

  def start(self, *args, **kwargs):
    generator = self.func(*args, **kwargs)
    if generator:
      try:
        next_func = next(generator)
      except StopIteration:
        pass
      except Exception as e:
        self.debug("ERROR WHILE RUNNING FETCHER", e)
        generator.throw(e)

      self.generator = generator
      return next_func

  def do_work(self, data):
    if self.generator:
      try:
        next_func = self.generator.send(data)
      except StopIteration as e:
        raise e
      except Exception as e:
        self.debug("ERROR WHILE RUNNING FETCHER", e)
        self.generator.throw(e)
        return

      return next_func
    else:
      self.debug("WHERE DID GENERATOR GO?", self)

  def set_result(self, val):
    self.result = val

  def get_result(self):
    # unpacking the result from the PrepResult (which is a shallow container)
    if self.result:
      return self.result.result
