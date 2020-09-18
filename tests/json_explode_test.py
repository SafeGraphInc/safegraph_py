# content of json_explode_test.py
from safegraph_py_functions import safegraph_py_functions as sgpy
import pytest
import pandas as pd
import pandas.util.testing as pdt
import json


### Function to be tested -----

def load_json_nan(df, json_col):
  return df[json_col].apply(lambda x: json.loads(x) if type(x) == str else x)

def unpack_json(df, json_column='visitor_home_cbgs', index_name= None, key_col_name=None,
                         value_col_name=None):
    # these checks are a inefficent for multithreading, but it's not a big deal
    if key_col_name is None:
        key_col_name = json_column + '_key'
    if value_col_name is None:
        value_col_name = json_column + '_value'
    if (df.index.unique().shape[0] < df.shape[0]):
        raise ("ERROR -- non-unique index found")
    df = df.copy()
    df[json_column + '_dict'] = load_json_nan(df,json_column)
    all_sgpid_cbg_data = []  # each cbg data point will be one element in this list
    if index_name is None:
      for index, row in df.iterrows():
          this_sgpid_cbg_data = [{'orig_index': index, key_col_name: key, value_col_name: value} for key, value in
                                row[json_column + '_dict'].items()]
          all_sgpid_cbg_data = all_sgpid_cbg_data + this_sgpid_cbg_data
    else:
      for index, row in df.iterrows():
        temp = row[index_name]
        this_sgpid_cbg_data = [{'orig_index': index, index_name:temp, key_col_name: key, value_col_name: value} for key, value in
                               row[json_column + '_dict'].items()]
        all_sgpid_cbg_data = all_sgpid_cbg_data + this_sgpid_cbg_data
    
    all_sgpid_cbg_data = pd.DataFrame(all_sgpid_cbg_data)
    all_sgpid_cbg_data.set_index('orig_index', inplace=True)
    return all_sgpid_cbg_data

### End function to be tested

### Expected DFs

expected_data = {
        'visitor_home_cbgs_key': ["484391113102", "484391057043", "484391219014", "484391113205", "484391057222", "484391219218", "484391113305", "484391057333", "484391219318", "484391113405", "484391057444", "484391219412", "484391113505", "484391057555", "484391219512"],
        'visitor_home_cbgs_value': [4, 4, 4, 4, 4, 4, 4, 4, 5, 4, 4, 4, 4, 4, 4]
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
    
    action = unpack_json(df)

    expected = pd.DataFrame(expected_data, index=[i for i in new_index])

    pdt.assert_frame_equal(action, expected)


### |-------------- Only uncomment when you need to test pytest functionality -------------|

# def test_fail():

#     test_df = sgpy.unpack_json(df)

#     df_array_standard = explode_json_array(df)

#     pdt.assert_frame_equal(test_df, df_array_standard)
