import time

from src.preparable import Preparer, Preparable
from src import debug

def f(x):
    return x*x

def x(y):
    return y*y;

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
if __name__ == "__main__":
  prep = Preparer()
  prep.add(Preparable(multi_step_function), [3])

  prep.run()
  prep.print_summary()
