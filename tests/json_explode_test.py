# content of json_explode_test.py
from safegraph_py_functions.safegraph_py_functions import *
import pytest
import pandas.util.testing as pdt


### Expected DFs

expected_output_data = {
        'visitor_home_cbgs_key': ["484391113102", "484391057043", "484391219014", "484391113205", "484391057222", "484391219218", "484391113305", "484391057333", "484391219318", "484391113405", "484391057444", "484391219412", "484391113505", "484391057555", "484391219512"],
        'visitor_home_cbgs_value': [4, 4, 4, 4, 4, 4, 4, 4, 5, 4, 4, 4, 4, 4, 4]
        }

### End Expected DFs


test_input_data = {'safegraph_place_id':  ['sg:64d0ee4695af4ab4906fe82997ead9ff', 'sg:001955fa1c994b4c8c877316a66dd986', 'sg:001e39c6b18645a5950b13a278b242c3', 'sg:00267c6356804259b6c92ba31c842f5a', 'sg:0029991464e349e8b5b985609360cfa4'],
        'visits_by_day': [[8,8,9,6,7,7,4], [3,9,9,4,11,7,4], [14,4,6,4,6,4,4], [1,3,1,3,0,0,2], [2,4,2,2,5,5,0]],
        'visitor_home_cbgs': [{"484391113102":4,"484391057043":4,"484391219014":4}, {"484391113205":4,"484391057222":4,"484391219218":4}, {"484391113305":4,"484391057333":4,"484391219318":5}, {"484391113405":4,"484391057444":4,"484391219412":4}, {"484391113505":4,"484391057555":4,"484391219512":4}],
        'date_range_start': ['2020-06-22T00:00:00-05:00', '2020-06-22T00:00:00-05:00', '2020-06-22T00:00:00-05:00', '2020-06-22T00:00:00-04:00', '2020-06-22T00:00:00-07:00']
        }

df = pd.DataFrame (test_input_data)

new_index = [0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4]

### Test section

def test_unpack_json():

    ''' This is a test of unpack json'''
    
    action = unpack_json(df)

    expected = pd.DataFrame(expected_output_data, index=new_index).rename_axis('orig_index')

    pdt.assert_frame_equal(action, expected)

def test_unpack_json_fast():
    ''' This is a test of unpack json fast '''

    action1 = unpack_json_fast(df)

    expected1 = pd.DataFrame(expected_output_data, index=new_index).rename_axis('orig_index')

    pdt.assert_frame_equal(action1, expected1)
