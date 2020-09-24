# content of read_geo_test.py
from safegraph_py_functions.safegraph_py_functions import read_geo_zip
import pytest
import pandas as pd
import pandas.util.testing as pdt
import json


### Files for analysis

geo_file = "tests/mock_data_v2020_09/geo_data/core_poi-geometry.csv.gz"

geo_file_zip = "tests/mock_data_v2020_09/geo_data/Core-Geo-georgia-GA-CORE_POI-GEOMETRY-2020_06-2020-07-28.zip"


### End files for analysis

gen_dtypes = {'postal_code': str, 'phone_number': str, 'naics_code': str, 'latitude': float, 'longitude': float, 'poi_cbg': str, 'census_block_group': str,'primary_number': str}

### Test section

def add(a, b):
    return a + b

hold1 = 5
hold2 = 5

def always_pass_test():
    value = add(hold1, hold2)
    assert value == 10

def test_read_geo_zip():

    ''' This is a test of read pattern single'''
    
    action = read_geo_zip(geo_file_zip)

    expected = pd.read_csv(geo_file, dtype=gen_dtypes, compression='gzip')

    pdt.assert_frame_equal(action, expected)

### |-------------- Only uncomment when you need to test pytest FAIL functionality -------------|

# def test_fail():

#     test_df = sgpy.unpack_json(df)

#     df_array_standard = explode_json_array(df)

#     pdt.assert_frame_equal(test_df, df_array_standard)
