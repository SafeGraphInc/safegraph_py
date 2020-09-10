# content of sgpy_test.py
from safegraph_py_functions import safegraph_py_functions as sgpy
import pytest

class TestClass:
    def test_one(self):
        x = "this"
        assert 'h' in x

    def test_two(self):
        x = "hello"
        assert hasattr(x, 'check')

    def test_three(self):
    	x = sgpy.test_me()
    	assert 'e' in x
