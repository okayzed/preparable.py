from preparable import Preparer
from preparable import debug

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
    debug("received", second_data)

  def other_work(self, arg):
    return "OTHER WORK: " + str(arg)

  def more_work(self):
    return "MORE:" + self.foo


# {{{ MAIN
if __name__ == "__main__":
  prep = Preparer()
  prep.add(Stepper().work, [3])
  prep.add(Stepper().work, [10])

  prep.run()

  prep.print_summary()
# }}}

