# content of json_explode_test.py
from safegraph_py_functions import safegraph_py_functions as sgpy
import pytest
import pandas as pd
import pandas.util.testing as pdt
import json


### Function to be tested -----

def load_json_nan(df, json_col):
  return df[json_col].apply(lambda x: json.loads(x) if type(x) == str else x)

def explode_json_array(df, array_column = 'visits_by_day', value_col_name=None, place_key='safegraph_place_id', file_key='date_range_start', array_sequence=None, keep_index=False, zero_index=False):
    if (array_sequence is None):
      array_sequence = array_column + '_sequence'
    if (value_col_name is None):
      value_col_name = array_column + '_value'
    if(keep_index):
        df['index_original'] = df.index
    df = df.copy()
    df.reset_index(drop=True, inplace=True) # THIS IS IMPORTANT; explode will not work correctly if index is not unique
    df[array_column + '_json'] = load_json_nan(df,array_column)
    day_visits_exp = df[[place_key, file_key, array_column+'_json']].explode(array_column+'_json')
    day_visits_exp['dummy_key'] = day_visits_exp.index
    day_visits_exp[array_sequence] = day_visits_exp.groupby([place_key, file_key])['dummy_key'].rank(method='first', ascending=True).astype('int64')
    if(zero_index):
      day_visits_exp[array_sequence] = day_visits_exp[array_sequence] -1
    day_visits_exp.drop(['dummy_key'], axis=1, inplace=True)
    day_visits_exp.rename(columns={array_column+'_json': value_col_name}, inplace=True)
    day_visits_exp[value_col_name] = day_visits_exp[value_col_name].astype('int64')
    df.drop([array_column+'_json'], axis=1, inplace=True)
    return pd.merge(df, day_visits_exp, on=[place_key,file_key])
    return all_sgpid_cbg_data

### End function to be tested

### Expected DFs

expected_data = {
        'safegraph_place_id':  ['sg:64d0ee4695af4ab4906fe82997ead9ff', 'sg:64d0ee4695af4ab4906fe82997ead9ff', 'sg:64d0ee4695af4ab4906fe82997ead9ff', 'sg:64d0ee4695af4ab4906fe82997ead9ff', 'sg:64d0ee4695af4ab4906fe82997ead9ff', 'sg:64d0ee4695af4ab4906fe82997ead9ff', 'sg:64d0ee4695af4ab4906fe82997ead9ff', 'sg:001955fa1c994b4c8c877316a66dd986', 'sg:001955fa1c994b4c8c877316a66dd986', 'sg:001955fa1c994b4c8c877316a66dd986', 'sg:001955fa1c994b4c8c877316a66dd986', 'sg:001955fa1c994b4c8c877316a66dd986', 'sg:001955fa1c994b4c8c877316a66dd986', 'sg:001955fa1c994b4c8c877316a66dd986', 'sg:001e39c6b18645a5950b13a278b242c3', 'sg:001e39c6b18645a5950b13a278b242c3', 'sg:001e39c6b18645a5950b13a278b242c3', 'sg:001e39c6b18645a5950b13a278b242c3', 'sg:001e39c6b18645a5950b13a278b242c3', 'sg:001e39c6b18645a5950b13a278b242c3', 'sg:001e39c6b18645a5950b13a278b242c3', 'sg:00267c6356804259b6c92ba31c842f5a', 'sg:00267c6356804259b6c92ba31c842f5a', 'sg:00267c6356804259b6c92ba31c842f5a', 'sg:00267c6356804259b6c92ba31c842f5a', 'sg:00267c6356804259b6c92ba31c842f5a', 'sg:00267c6356804259b6c92ba31c842f5a', 'sg:00267c6356804259b6c92ba31c842f5a', 'sg:0029991464e349e8b5b985609360cfa4', 'sg:0029991464e349e8b5b985609360cfa4', 'sg:0029991464e349e8b5b985609360cfa4', 'sg:0029991464e349e8b5b985609360cfa4', 'sg:0029991464e349e8b5b985609360cfa4', 'sg:0029991464e349e8b5b985609360cfa4', 'sg:0029991464e349e8b5b985609360cfa4'],
        'visits_by_day': [[8,8,9,6,7,7,4], [8,8,9,6,7,7,4], [8,8,9,6,7,7,4], [8,8,9,6,7,7,4], [8,8,9,6,7,7,4], [8,8,9,6,7,7,4], [8,8,9,6,7,7,4], [3,9,9,4,11,7,4], [3,9,9,4,11,7,4], [3,9,9,4,11,7,4], [3,9,9,4,11,7,4], [3,9,9,4,11,7,4], [3,9,9,4,11,7,4], [3,9,9,4,11,7,4], [14,4,6,4,6,4,4], [14,4,6,4,6,4,4], [14,4,6,4,6,4,4], [14,4,6,4,6,4,4], [14,4,6,4,6,4,4], [14,4,6,4,6,4,4], [14,4,6,4,6,4,4], [1,3,1,3,0,0,2], [1,3,1,3,0,0,2], [1,3,1,3,0,0,2], [1,3,1,3,0,0,2], [1,3,1,3,0,0,2], [1,3,1,3,0,0,2], [1,3,1,3,0,0,2], [2,4,2,2,5,5,0], [2,4,2,2,5,5,0], [2,4,2,2,5,5,0], [2,4,2,2,5,5,0], [2,4,2,2,5,5,0], [2,4,2,2,5,5,0], [2,4,2,2,5,5,0]],
        'visitor_home_cbgs': [{"484391113102":4,"484391057043":4,"484391219014":4}, {"484391113102":4,"484391057043":4,"484391219014":4}, {"484391113102":4,"484391057043":4,"484391219014":4}, {"484391113102":4,"484391057043":4,"484391219014":4}, {"484391113102":4,"484391057043":4,"484391219014":4}, {"484391113102":4,"484391057043":4,"484391219014":4}, {"484391113102":4,"484391057043":4,"484391219014":4}, {"484391113205":4,"484391057222":4,"484391219218":4}, {"484391113205":4,"484391057222":4,"484391219218":4}, {"484391113205":4,"484391057222":4,"484391219218":4}, {"484391113205":4,"484391057222":4,"484391219218":4}, {"484391113205":4,"484391057222":4,"484391219218":4}, {"484391113205":4,"484391057222":4,"484391219218":4}, {"484391113205":4,"484391057222":4,"484391219218":4}, {"484391113305":4,"484391057333":4,"484391219318":5}, {"484391113305":4,"484391057333":4,"484391219318":5}, {"484391113305":4,"484391057333":4,"484391219318":5}, {"484391113305":4,"484391057333":4,"484391219318":5}, {"484391113305":4,"484391057333":4,"484391219318":5}, {"484391113305":4,"484391057333":4,"484391219318":5}, {"484391113305":4,"484391057333":4,"484391219318":5}, {"484391113405":4,"484391057444":4,"484391219412":4}, {"484391113405":4,"484391057444":4,"484391219412":4}, {"484391113405":4,"484391057444":4,"484391219412":4}, {"484391113405":4,"484391057444":4,"484391219412":4}, {"484391113405":4,"484391057444":4,"484391219412":4}, {"484391113405":4,"484391057444":4,"484391219412":4}, {"484391113405":4,"484391057444":4,"484391219412":4}, {"484391113505":4,"484391057555":4,"484391219512":4}, {"484391113505":4,"484391057555":4,"484391219512":4}, {"484391113505":4,"484391057555":4,"484391219512":4}, {"484391113505":4,"484391057555":4,"484391219512":4}, {"484391113505":4,"484391057555":4,"484391219512":4}, {"484391113505":4,"484391057555":4,"484391219512":4}, {"484391113505":4,"484391057555":4,"484391219512":4}],
        'date_range_start': ['2020-06-22T00:00:00-05:00', '2020-06-22T00:00:00-05:00', '2020-06-22T00:00:00-05:00', '2020-06-22T00:00:00-05:00', '2020-06-22T00:00:00-05:00', '2020-06-22T00:00:00-05:00', '2020-06-22T00:00:00-05:00', '2020-06-22T00:00:00-05:00', '2020-06-22T00:00:00-05:00', '2020-06-22T00:00:00-05:00', '2020-06-22T00:00:00-05:00', '2020-06-22T00:00:00-05:00', '2020-06-22T00:00:00-05:00', '2020-06-22T00:00:00-05:00', '2020-06-22T00:00:00-05:00', '2020-06-22T00:00:00-05:00', '2020-06-22T00:00:00-05:00', '2020-06-22T00:00:00-05:00', '2020-06-22T00:00:00-05:00', '2020-06-22T00:00:00-05:00', '2020-06-22T00:00:00-05:00', '2020-06-22T00:00:00-04:00', '2020-06-22T00:00:00-04:00', '2020-06-22T00:00:00-04:00', '2020-06-22T00:00:00-04:00', '2020-06-22T00:00:00-04:00', '2020-06-22T00:00:00-04:00', '2020-06-22T00:00:00-04:00', '2020-06-22T00:00:00-07:00', '2020-06-22T00:00:00-07:00', '2020-06-22T00:00:00-07:00', '2020-06-22T00:00:00-07:00', '2020-06-22T00:00:00-07:00', '2020-06-22T00:00:00-07:00', '2020-06-22T00:00:00-07:00'],
        'visits_by_day_value': [8,8,9,6,7,7,4,3,9,9,4,11,7,4,14,4,6,4,6,4,4,1,3,1,3,0,0,2,2,4,2,2,5,5,0],
        'visits_by_day_sequence': [1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 6, 7]
        }


### End Expected DFs


data = {'safegraph_place_id':  ['sg:64d0ee4695af4ab4906fe82997ead9ff', 'sg:001955fa1c994b4c8c877316a66dd986', 'sg:001e39c6b18645a5950b13a278b242c3', 'sg:00267c6356804259b6c92ba31c842f5a', 'sg:0029991464e349e8b5b985609360cfa4'],
        'visits_by_day': [[8,8,9,6,7,7,4], [3,9,9,4,11,7,4], [14,4,6,4,6,4,4], [1,3,1,3,0,0,2], [2,4,2,2,5,5,0]],
        'visitor_home_cbgs': [{"484391113102":4,"484391057043":4,"484391219014":4}, {"484391113205":4,"484391057222":4,"484391219218":4}, {"484391113305":4,"484391057333":4,"484391219318":5}, {"484391113405":4,"484391057444":4,"484391219412":4}, {"484391113505":4,"484391057555":4,"484391219512":4}],
        'date_range_start': ['2020-06-22T00:00:00-05:00', '2020-06-22T00:00:00-05:00', '2020-06-22T00:00:00-05:00', '2020-06-22T00:00:00-04:00', '2020-06-22T00:00:00-07:00']
        }

df = pd.DataFrame (data, columns = ['safegraph_place_id', 'visits_by_day', 'visitor_home_cbgs', 'date_range_start'])


cols = ['safegraph_place_id', 'visits_by_day', 'visitor_home_cbgs', 'date_range_start', 'visitor_home_cbgs_key', 'visitor_home_cbgs_value']

new_index = [0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4]

### Test section

def add(a, b):
    return a + b

hold1 = 5
hold2 = 5

def always_pass_test():
    value = add(hold1, hold2)
    assert value == 10

def test_unpack_json():

    ''' This is a test of unpack json'''
    
    action = explode_json_array(df)

    expected = pd.DataFrame(expected_data)

    pdt.assert_frame_equal(action, expected)


### |-------------- Only uncomment when you need to test pytest functionality -------------|

# def test_fail():

#     test_df = sgpy.unpack_json(df)

#     df_array_standard = explode_json_array(df)

#     pdt.assert_frame_equal(test_df, df_array_standard)