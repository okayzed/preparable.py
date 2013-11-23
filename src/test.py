import time
import copy_reg

from preparable import Preparer, Preparable


def debug(*args):
  print " ".join(map(str, args))

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

def top_level():
  debug("top level")
  return "foo"

def multi_step_two():
  debug("c step 0")
  bar = yield top_level

def some_work(foo):
  debug("d DOING SOME WORK", foo)

def multi_step_three():
  debug("b step 0")

  first_arg = yield {
    "func" : some_work,
    "args" : ["foo"],
    "cache_key" : "one"
  }
  debug("b step 1")

class Stepper(object):
  def __init__(self):
    self.foo = "some data"

  def work(self):
    debug("DOING SOME CLASS WORK")
    data = yield { "func": self.other_work }
    self.foo = data

  def other_work(self):
    debug("DOING SOME OTHER CLASS WORK")
    return "OTHER WORK"


if __name__ == "__main__":
  prep = Preparer()
  prep.add(Preparable(multi_step_function), [3])
  prep.add(Preparable(multi_step_two))
  prep.add(Preparable(multi_step_three))
  prep.add(Preparable(Stepper().work))

  prep.run()

  print "\n\n"
  print "Finished preparing data!"
  print "Cached data:", prep.cache
  print "Preparables run", len(prep.preparables)
