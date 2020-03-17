#!/usr/bin/python
# coding=utf-8
################################################################################

from dimension_reader import DimensionReader
from host_reader import HostReader


################################################################################

class TestDimensionReader(DimensionReader):
    """
    A dimension reader meant to be used in the test environment. It takes
    the dimension to be returns as a contructor arg and returns them transparently
    """

    def __init__(self, host_dimensions):
        super(TestDimensionReader, self).__init__()
        self.host_dimensions = host_dimensions

    def name(self):
        return "test"

    def configure(self, conf):
        pass

    def read(self, hosts):
        return self.host_dimensions


class TestHostReader(HostReader):
    """
    A host reader meant to be used in the test environment. It takes a list of
    hosts as a constructor args and returns them always
    """

    def __init__(self, hosts):
        super(TestHostReader, self).__init__()
        self.hosts = hosts

    def name(self):
        return "test"

    def configure(self, conf):
        pass

    def read(self):
        return self.hosts
