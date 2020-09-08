# content of sgpy_test.py

import pytest

def a(x):
    return x + 1


def test_answer():
    assert a(3) == 4

def wrong_answer():
	assert a(2) == 4
