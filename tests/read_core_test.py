# content of read_core_test.py
from safegraph_py_functions.safegraph_py_functions import *
import pytest
import pandas.util.testing as pdt


### Files for analysis

input_core_unzip_part1 = "tests/mock_data_v2020_09/core_unzipped/core_poi-part1.csv.gz"

input_core_unzip_part2 = "tests/mock_data_v2020_09/core_unzipped/core_poi-part2.csv.gz"

core_folder_unzipped = "tests/mock_data_v2020_09/core_unzipped/"

core_zipped = "tests/mock_data_v2020_09/Core_from_unzipped.zip"


### End files for analysis

sg_dtypes = {'postal_code': str, 'phone_number': str, 'naics_code': str, 'latitude': float, 'longitude': float, 'poi_cbg': str, 'census_block_group': str,'primary_number': str}

df1 = pd.read_csv(input_core_unzip_part1, dtype=sg_dtypes, compression='gzip')
df2 = pd.read_csv(input_core_unzip_part2, dtype=sg_dtypes, compression='gzip')


### Test section

def test_read_core_folder():

    ''' This is a test of read pattern single'''
    
    action = read_core_folder(core_folder_unzipped)

    expected = pd.concat([df2, df1], axis=0)

    pdt.assert_frame_equal(action, expected)

def test_read_core_folder_zip():
    ''' This is a test of unpack json fast '''

    action1 = read_core_folder_zip(core_zipped)

    expected1 = pd.concat([df1, df2], axis=0, ignore_index=True)

    pdt.assert_frame_equal(action1, expected1)
