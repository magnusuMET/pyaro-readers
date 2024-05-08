"""
Module containing pyaerocom custom exceptions
"""


class NasaAmesReadError(IOError):
    pass


class TimeZoneError(AttributeError):
    pass
