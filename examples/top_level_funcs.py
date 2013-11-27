from src import Preparer
from src import debug

# {{{ top level functions, used by PREPARABLES
def f(x):
    return x*x

def x(y):
    return y*y;

def top_level():
  debug("top level")
  return "foo"

def some_work(foo):
  debug("d DOING SOME WORK", foo)

# }}}

# {{{ MULTI STEP FUNCTIONS

def multi_step_one(first_arg):
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

def multi_step_two():
  debug("b step 0")
  bar = yield top_level
  baz = yield {
    "func" : x,
    "args" : [10]
  }

  debug("b step 0.5")
  bax = yield (f,[5],)
  debug("b step 1")

def multi_step_three():
  debug("c step 0")

  first_arg = yield {
    "func" : some_work,
    "args" : ["foo"],
    "cache_key" : "one"
  }
  debug("c step 1")
# }}} 

# {{{ MAIN
if __name__ == "__main__":
  prep = Preparer()
  prep.add(multi_step_one, [3])
  prep.add(multi_step_two)
  prep.add(multi_step_three)

  prep.run()

  prep.print_summary()
# }}}
