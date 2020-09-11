# content of sgpy_test.py
from safegraph_py_functions import safegraph_py_functions as sgpy
import pytest
import pandas as pd

data = {'safegraph_place_id':  ['sg:64d0ee4695af4ab4906fe82997ead9ff', 'sg:001955fa1c994b4c8c877316a66dd986', 'sg:001e39c6b18645a5950b13a278b242c3', 'sg:00267c6356804259b6c92ba31c842f5a', 'sg:0029991464e349e8b5b985609360cfa4'],
        'visits_by_day': [[8,8,9,6,7,7,4], [3,9,9,4,11,7,4], [14,4,6,4,6,4,4], [1,3,1,3,0,0,2], [2,4,2,2,5,5,0]],
        'visitor_home_cbgs': [{"484391113102":4,"484391057043":4,"484391219014":4}, {"484391113205":4,"484391057222":4,"484391219218":4}, {"484391113305":4,"484391057333":4,"484391219318":5}, {"484391113405":4,"484391057444":4,"484391219412":4}, {"484391113505":4,"484391057555":4,"484391219512":4}],
        'date_range_start': ['2020-06-22T00:00:00-05:00', '2020-06-22T00:00:00-05:00', '2020-06-22T00:00:00-05:00', '2020-06-22T00:00:00-04:00', '2020-06-22T00:00:00-07:00']
        }

df = pd.DataFrame (data, columns = ['safegraph_place_id', 'visits_by_day', 'visitor_home_cbgs', 'date_range_start'])



class TestClass:
    def test_one(self):
        x = "this"
        assert 'h' in x

    def test_two(self):
        x = "hello"
        assert hasattr(x, 'check')

    def test_one(self):
        x = df
        assert hasattr(x, 'safegraph_place_id')
