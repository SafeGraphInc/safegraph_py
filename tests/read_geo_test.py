# content of read_geo_test.py
from safegraph_py_functions.safegraph_py_functions import read_geo_zip
import pytest
import pandas as pd
import pandas.util.testing as pdt
import json

### Files for analysis

input_test_geo_file = "tests/mock_data_v2020_09/geo_data/core_poi-geometry.csv.gz"
input_geo_file_zip = "tests/mock_data_v2020_09/geo_data/Core-Geo-georgia-GA-CORE_POI-GEOMETRY-2020_06-2020-07-28.zip"

### End files for analysis

sg_dtypes = {'postal_code': str, 'phone_number': str, 'naics_code': str, 'latitude': float, 'longitude': float, 'poi_cbg': str, 'census_block_group': str,'primary_number': str}

### Test section

def test_read_geo_zip():

    ''' This is a test of read pattern single'''
    
    action = read_geo_zip(input_geo_file_zip)

    expected = pd.read_csv(input_test_geo_file, dtype=sg_dtypes, compression='gzip')

    pdt.assert_frame_equal(action, expected)
