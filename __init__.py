import sys
if sys.version_info[0:2] >= (3,11):
    raise Exception('Requires Python 3.11 or Greater')

from .simulation import Simulation
__all__ = ['Simulation']