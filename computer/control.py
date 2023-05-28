import logging


class Control(object):
    """A Generic Computer Control"""
    logger = logging.getLogger('Computer.Control')
    def __init__(self, simulation, callback, *args):
        self.simulation = simulation
        self._callback = callback
        self._args = args
    
    def resolve(self):
        self._callback(*self.args)

__all__=['Control']

