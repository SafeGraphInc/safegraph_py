# content of read_pattern_test.py
from safegraph_py_functions.safegraph_py_functions import read_pattern_single, read_pattern_multi
import pytest
import pandas as pd
import pandas.util.testing as pdt
import json
import gzip


### Expected DFs

file_path = "weekly_demo.csv.gz"

### End Expected DFs


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
    
    # action = read_pattern_single(f_path=file_path)

    action = pd.read_csv(file_path, compression='gzip')

    expected = pd.read_csv(file_path, compression='gzip')

    pdt.assert_frame_equal(action, expected)

# def test_unpack_json_fast():
#     ''' This is a test of unpack json fast '''

#     action1 = unpack_json_fast(df)

#     expected1 = pd.DataFrame(expected_data, index=new_index).rename_axis('orig_index')

#     pdt.assert_frame_equal(action1, expected1)

### |-------------- Only uncomment when you need to test pytest FAIL functionality -------------|

# def test_fail():

#     test_df = sgpy.unpack_json(df)

#     df_array_standard = explode_json_array(df)

#     pdt.assert_frame_equal(test_df, df_array_standard)
