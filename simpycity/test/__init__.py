import unittest
import os.path

def additional_tests():
    """
    Supports setup.py "test" command
    """
    start_dir = os.path.dirname(__file__)
    (top_level_dir, tail) = os.path.split(start_dir)
    loader = unittest.TestLoader()
    suite = loader.discover(start_dir, top_level_dir=top_level_dir)
    return suite
