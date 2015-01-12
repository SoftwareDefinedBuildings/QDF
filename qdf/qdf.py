from quasar import *
import sys
import array
from twisted.internet import defer, protocol, reactor

class QDF2Distillate (object):

    #set by scheduler to {name:uuid}
    deps = []

    def __init__(self):
        pass


    def prereqs(self, changed_ranges):
        """

        :param changed_ranges: a list of (name, uuid, [start_time, end_time])
        :return: a list of (name, uuid, [start_time, end_time]) tuples.
        """
        return changed_ranges

    def compute(self, changed_ranges, input_streams):
        """

        :param changed_ranges: a list of (name, uuid, [start_time, end_time])
        :param input_streams: a dictionary of {name: [(time, value)]}
        """
        pass


