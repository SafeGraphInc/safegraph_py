# content of read_core_test.py
from safegraph_py_functions.safegraph_py_functions import read_core_folder, read_core_folder_zip
import pytest
import pandas as pd
import pandas.util.testing as pdt
import json
import gzip


### Expected DFs

core_unzip_1 = "tests/mock_data_v2020_09/core_unzipped/core_poi-part1.csv.gz"

core_unzip_2 = "tests/mock_data_v2020_09/core_unzipped/core_poi-part2.csv.gz"

core_folder_unzipped = "tests/mock_data_v2020_09/core_unzipped/"

core_zipped = "tests/mock_data_v2020_09/Core_from_unzipped.zip"


### End Expected DFs

gen_dtypes = {'postal_code': str, 'phone_number': str, 'naics_code': str, 'latitude': float, 'longitude': float, 'poi_cbg': str, 'census_block_group': str,'primary_number': str}


df1 = pd.read_csv(core_unzip_1, dtype=gen_dtypes, compression='gzip')
df2 = pd.read_csv(core_unzip_2, dtype=gen_dtypes, compression='gzip')


### Test section

def add(a, b):
    return a + b

hold1 = 5
hold2 = 5

def always_pass_test():
    value = add(hold1, hold2)
    assert value == 10

def test_read_core_folder():

    ''' This is a test of read pattern single'''
    
    action = read_core_folder(core_folder_unzipped)

    expected = pd.concat([df2, df1], axis=0)

    pdt.assert_frame_equal(action, expected)

def test_read_core_folder_zip():
    ''' This is a test of unpack json fast '''

    action1 = read_core_folder_zip(core_zipped)

    expected1 = pd.concat([df2, df1], axis=0, ignore_index=True)

    pdt.assert_frame_equal(action1, expected1)

### |-------------- Only uncomment when you need to test pytest FAIL functionality -------------|

# def test_fail():

#     test_df = sgpy.unpack_json(df)

#     df_array_standard = explode_json_array(df)

#     pdt.assert_frame_equal(test_df, df_array_standard)
