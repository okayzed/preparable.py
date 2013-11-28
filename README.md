# data dispatching done wrong

This is a poor imitation of the FB's data dispatch mechanism (aka Preparables).

It allows concurrent data dispatches to run in parallel and exposes the data to
multi-step functions with a simple API.

[ Read More About Preparables ](http://www.quora.com/Facebook-Infrastructure/What-are-preparables-and-how-are-they-implemented )

## Installation

    # get the latest version from pypi
    sudo pip install Preparable

## Usage


```python
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

def friend_loader(data):
  person = yield PersonFetcher(data.id, data.friends) # should take 0.1s total
  friends = yield [ PersonFetcher(f) for f in person.friends ] # should take 0.1s total

  yield PrepResult(friends)

if __name__ == "__main__":
  prep = Preparer(processes=4)
  ret = prep.add(friend_loader, [Person(10, [5, 4, 39, 22])])
  prep.run()

  print ret.get_result()
```

See examples/ directory for more examples of how preparers are invoked

See webservers/ directory for integration into webservers

## What's implemented

* Single Preparable Dispatch
* Multi Preparable Dispatch
* Simple Key Caching

## What's not Implemented

* Errors (partially implemented, fully undocumented)
* Children Preparables
