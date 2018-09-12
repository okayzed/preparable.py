from __future__ import print_function

from .debuggable_class import Debuggable

class NotImplementedException(Exception):
  pass

class PrepFetcher(Debuggable):
  def __init__(self, *args, **kwargs):
    self.result = None
    if hasattr(self, 'init'):
      self.init(*args, **kwargs)

  def get_cache_key(self):
    return

  def fetch(self):
    raise NotImplementedException()

  def get_result(self):
    return self.result

  def set_result(self, data):
    self.result = data
