from src import Preparer
from src import debug

# This shows how an instance method can used
# in conjunction with changing instance variables
# to run a preparable in a separate thread.
class Stepper(object):
  def __init__(self):
    self.foo = "some data"

  def work(self, first_arg):
    data = yield { "func": self.other_work, "args" : [first_arg] }
    self.foo = data
    second_data = yield { "func" : self.more_work }

  def other_work(self, arg):
    return "OTHER WORK: " + str(arg)

  def more_work(self):
    return "MORE:" + self.foo


# {{{ MAIN
if __name__ == "__main__":
  prep = Preparer()
  for x in xrange(100):
    prep.add(Stepper().work, [x])

  prep.run()

  prep.print_summary()
# }}}


