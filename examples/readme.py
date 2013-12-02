from preparable import Preparer, PrepFetcher, PrepResult
import time

class Person(object):
  def __init__(self, id, friends):
    self.id = id
    self.friends = friends

class PersonFetcher(PrepFetcher):
  def __init__(self, id, friends=[]):
    self.id = id
    self.friends = friends

  def fetch(self):
    time.sleep(0.1)
    # in real code, this would be a synchronous DB call
    return Person(self.id, self.friends)

  def get_cache_key(self):
    return self.id

def friend_loader(data):
  person = yield PersonFetcher(data.id, data.friends) # should take 0.1s total
  friends = yield [ PersonFetcher(f) for f in person.friends ] # should take 0.1s total

  yield PrepResult(friends)

if __name__ == "__main__":
  prep = Preparer(processes=4)
  ret = prep.add(friend_loader, [Person(10, [5, 4, 39, 22])])
  prep.run()

  print ret.get_result()
