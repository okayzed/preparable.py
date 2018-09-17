from __future__ import print_function

# http://bytes.com/topic/python/answers/552476-why-cant-you-pickle-instancemethods
# http://stackoverflow.com/questions/1816958/cant-pickle-type-instancemethod-when-using-pythons-multiprocessing-pool-map

def _pickle_method(method):
  func_name = method.__func__.__name__
  obj = method.__self__
  cls = method.__self__.__class__
  return _unpickle_method, (func_name, obj, cls)

def _unpickle_method(func_name, obj, cls):
  for cls in cls.mro():
    try:
      func = cls.__dict__[func_name]
    except KeyError:
      pass
    else:
      break
  return func.__get__(obj, cls)

try:
    import copyreg
except:
    import copy_reg as copyreg

import types
copyreg.pickle(types.MethodType, _pickle_method, _unpickle_method)

