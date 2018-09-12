from __future__ import print_function

class Debuggable(object):
  DEBUG=False
  def debug(self, *args):
    cl = type(self)
    if cl.DEBUG:
      print(" ".join(map(str, args)))
