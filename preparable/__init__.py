from __future__ import print_function

from .preparable import Preparable
from .preparer import Preparer, PrepResult
from .fetcher import PrepFetcher

def debug(*args):
  print(" ".join(map(str, args)))
