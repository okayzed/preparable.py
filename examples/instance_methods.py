from preparable import Preparer, PrepFetcher, PrepResult
from preparable import debug

# This shows how an instance method can used
# in conjunction with changing instance variables
# to run a preparable in a separate thread.

class FakeFetcher(PrepFetcher):
  def init(self, func, *args, **kwargs):
    self.func = func
    self.args = args or []
    self.kwargs = kwargs or {}


  def fetch(self):
    self.data = self.func(*self.args, **self.kwargs)
    return self.data

class Stepper(object):
  def __init__(self):
    self.foo = "some data"

  def work(self, first_arg):
    prep = FakeFetcher(self.other_work, first_arg)
    data = yield prep
    prep_two = FakeFetcher(self.more_work, data)
    second_data = yield prep_two
    self.data = second_data
    yield(PrepResult(second_data))

  def other_work(self, arg):
    return "OTHER WORK: " + str(arg)

  def more_work(self, foo):
    return "MORE:" + foo


# {{{ MAIN
if __name__ == "__main__":
  prep = Preparer()
  stepper = Stepper()
  ret = prep.add(stepper.work, [3])

  prep.run()
  print ret.get_result()
  prep.print_summary()
# }}}
