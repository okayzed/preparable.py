import time

from src import Preparer, Preparable, PrepFetcher
from src import debug

# {{{ top level preparable functions
def f(x):
    return x*x

def x(y):
    return y*y;

class XFetcher(PrepFetcher):
  def __init__(self, x):
    self.x = x

  def fetch(self):
    return self.x * self.x

class YFetcher(PrepFetcher):
  def __init__(self, x):
    self.x = x

  def fetch(self):
    return self.x * self.x

# }}}

# {{{ Multi step functions that rely on them
def multi_step_function(first_arg):
  debug("a step 0", first_arg)
  second_arg = yield {
    "func" : f,
    "args" : [first_arg],
    "kwargs" : {},
    "cache_key" : "one"
  }

  debug("a step 1 received", second_arg)

  third_arg = yield {
    "func" : x,
    "args" : [second_arg]
  }
  debug("a step 2 received", third_arg)

def multi_step_fetcher(first_arg):
  debug("d step 0", first_arg)
  second_arg = yield XFetcher(first_arg)
  debug("d step 1 received", second_arg)

  third_arg = yield YFetcher(second_arg)
  debug("d step 2 received", third_arg)

# }}}

if __name__ == "__main__":
  prep = Preparer()
  prep.add(Preparable(multi_step_function), [3])
  prep.add(Preparable(multi_step_fetcher), [2])

  prep.run()
  prep.print_summary()
