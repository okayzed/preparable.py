import time
from preparable import Preparer, Preparable


def debug(*args):
#  return
  print " ".join(map(str, args))

def f(x):
    return x*x

def x(y):
    return y*y;

def multi_step_function(first_arg):
  debug("a step 0", first_arg)
  second_arg = yield {
    "func" : f,
    "args" : [5],
    "kwargs" : {}
  }

  debug("a step 1 received", second_arg)

  third_arg = yield {
    "func" : x,
    "args" : [1]
  }
  debug("a step 2 received", third_arg)

def multi_step_two():
  debug("c step 0")

def some_work(foo):
  debug("d DOING SOME WORK", foo)

def multi_step_three():
  debug("b step 0")

  first_arg = yield {
    "func" : some_work,
    "args" : ["foo"]
  }
  debug("b step 1")

class Stepper(object):
  def __init__(self):
    pass

  def work(self):
    debug("DOING SOME CLASS WORK")
    data = yield { "func": self.other_work }

  def other_work(self):
    debug("DOING SOME OTHER CLASS WORK")
    return "OTHER WORK"

if __name__ == "__main__":
  p = Preparable(multi_step_function)
  prep = Preparer()
  prep.add(Preparable(multi_step_function), [10])
  prep.add(Preparable(multi_step_two))
  prep.add(Preparable(multi_step_three))
  prep.add(Preparable(Stepper().work))

  done = False
  def when_done():
    debug("FINISHED ALL JOBS")
    prep.done = True

  prep.on_done(when_done)
  prep.run()

  while True:
    time.sleep(0.05)
    if prep.done:
      break
