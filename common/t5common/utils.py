import logging
import os
import sys


_logger = None
def get_logger(level='info'):
    global _logger
    if _logger is None:
        _logger = logging.getLogger(sys.argv[0])
        hdlr = logging.StreamHandler(sys.stderr)

        _logger.setLevel(getattr(logging, level.upper()))
        _logger.addHandler(hdlr)
        hdlr.setFormatter(logging.Formatter('%(asctime)s | %(name)s - %(levelname)s | %(message)s'))
    return _logger


def read_token(path):
    """Helper for reading token or password files"""
    with open(os.path.expandvars(path), 'r') as f:
        return f.read().strip()
