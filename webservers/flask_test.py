import sys
sys.path.append('..')

from flask import Flask
from src import Preparer, PrepFetcher, PrepResult

app = Flask(__name__)

class FakeFetcher(PrepFetcher):
  def __init__(self, func, *args, **kwargs):
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
    prep_two = FakeFetcher(self.more_work, "other arg")
    data_one, data_two = yield [prep, prep_two]

    prep_three = FakeFetcher(self.even_more_work, data_one, data_two)
    data_three = yield prep_three
    yield PrepResult(data_three)

  def other_work(self, arg):
    return "OTHER WORK: " + str(arg)

  def more_work(self, foo):
    return "MORE:" + foo

  def even_more_work(self, datum1, datum2):
    return "EVEN MORE: datum1: %s, datum2: %s" % (datum1, datum2)


@app.route("/")
def hello():

  p = Preparer()
  ret = p.add(Stepper().work, [10])
  sec = p.add(Stepper().work, [15])
  p.run()

  return ret.get_result() + " " + sec.get_result()

if __name__ == "__main__":
  app.run()

