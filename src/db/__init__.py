#!/usr/bin/env python3

class SQLUtils(object):
    def __init__(self, *args, **kwargs):
        raise SyntaxError("This class is static and a new instance isn't allowed.")

    #@staticmethod
