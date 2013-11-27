from debuggable_class import Debuggable

class NotImplementedException(Exception):
  pass

class PrepFetcher(Debuggable):
  def __init__(self, *args, **kwargs):
    super(self, Debuggable).__init__(self, *args, **kwargs)
    self.data = None

  def get_cache_key(self):
    return

  def fetch(self):
    raise NotImplementedException()

  def get_data(self):
    return self.data

  def set_data(self, data):
    self.data = data
