# content of read_pattern_test.py
from safegraph_py_functions.safegraph_py_functions import read_pattern_single, read_pattern_multi
import pytest
import pandas as pd
import pandas.util.testing as pdt
import json
import gzip


### Expected DFs

file_path = "tests/mock_data_v2020_09/weekly_demo.csv.gz"

file_path_multi = "tests/mock_data_v2020_09/weekly_multi"

### End Expected DFs


week1_path = "tests/mock_data_v2020_09/weekly_multi/weekly_demo1.csv.gz"
week2_path = "tests/mock_data_v2020_09/weekly_multi/weekly_demo2.csv.gz"

week1 = pd.read_csv(week1_path, dtype={'postal_code': str, 'phone_number': str, 'naics_code': str, 'latitude': float, 'longitude': float, 'poi_cbg': str, 'census_block_group': str,'primary_number': str}, compression='gzip')
week2 = pd.read_csv(week2_path, dtype={'postal_code': str, 'phone_number': str, 'naics_code': str, 'latitude': float, 'longitude': float, 'poi_cbg': str, 'census_block_group': str,'primary_number': str}, compression='gzip')

# combined = pd.concat([week1, week2], ignore_index=True)

### Test section

def add(a, b):
    return a + b

hold1 = 5
hold2 = 5

def always_pass_test():
    value = add(hold1, hold2)
    assert value == 10

def test_read_pattern_single():

    ''' This is a test of read pattern single'''
    
    action = read_pattern_single(f_path=file_path)

    expected = pd.read_csv(file_path, dtype={'postal_code': str, 'phone_number': str, 'naics_code': str, 'latitude': float, 'longitude': float, 'poi_cbg': str, 'census_block_group': str,'primary_number': str}, compression='gzip')

    pdt.assert_frame_equal(action, expected)

def test_read_pattern_multi():
    ''' This is a test of unpack json fast '''

    action1 = read_pattern_multi(file_path_multi)

    expected1 = pd.concat([week1, week2], ignore_index=True)
    
    pdt.assert_frame_equal(action1, expected1)

### |-------------- Only uncomment when you need to test pytest FAIL functionality -------------|

# def test_fail():

#     test_df = sgpy.unpack_json(df)

#     df_array_standard = explode_json_array(df)

#     pdt.assert_frame_equal(test_df, df_array_standard)
