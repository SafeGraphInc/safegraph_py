# content of read_pattern_test.py
from safegraph_py_functions.safegraph_py_functions import read_pattern_single, read_pattern_multi
import pytest
import pandas as pd
import pandas.util.testing as pdt
import json
import gzip

### Files for analysis

input_test_data_file_path = "tests/mock_data_v2020_09/weekly_demo.csv.gz"

input_test_data_multi_file_path = "tests/mock_data_v2020_09/weekly_multi"

### End files for analysis

week1_path = "tests/mock_data_v2020_09/weekly_multi/weekly_demo1.csv.gz"
week2_path = "tests/mock_data_v2020_09/weekly_multi/weekly_demo2.csv.gz"
sg_dtypes = {'postal_code': str, 'phone_number': str, 'naics_code': str, 'latitude': float, 'longitude': float, 'poi_cbg': str, 'census_block_group': str,'primary_number': str}
week1 = pd.read_csv(week1_path, dtype=sg_dtypes, compression='gzip')
week2 = pd.read_csv(week2_path, dtype=sg_dtypes, compression='gzip')

### Test section

def test_read_pattern_single():

    ''' This is a test of read pattern single'''
    
    action = read_pattern_single(f_path=input_test_data_file_path)

    expected = pd.read_csv(input_test_data_file_path, dtype={'postal_code': str, 'phone_number': str, 'naics_code': str, 'latitude': float, 'longitude': float, 'poi_cbg': str, 'census_block_group': str,'primary_number': str}, compression='gzip')

    pdt.assert_frame_equal(action, expected)

def test_read_pattern_multi():
    ''' This is a test of unpack json fast '''

    action1 = read_pattern_multi(input_test_data_multi_file_path)

    expected1 = pd.concat([week1, week2])

    pdt.assert_frame_equal(action1, expected1)
