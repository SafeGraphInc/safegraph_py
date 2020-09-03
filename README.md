# `safegraph_py`

`safegraph_py` is a Python library designed to make your experiece with SafeGraph data as easy as possible.

These functions are [demonstrated on SafeGraph data in this Colab Notebook](https://colab.research.google.com/drive/1V7hnyYuY_dUXQEPkCMZkgMuBFQV4iA_4?usp=sharing). 

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install `safegraph_py`.

```bash
pip install -q --upgrade git+https://github.com/SafeGraphInc/safegraph_py
```

## Usage

```python
from safegraph_py_functions import safegraph_py_functions as sgpy

sgpy.test_me() # returns 'Hello World' to ensure you have downloaded the library
sgpy.help() # returns a list of all active functions and their arguments in the safegraph_py library
sgpy.read_pattern_single(f_path) # returns a Pandas DF from a single patterns file
# etc. . . 
```

## Functions

#### unpack_json(df_, json_column='visitor_home_cbgs', key_col_name='visitor_home_cbg', value_col_name='cbg_visitor_count')

The unpack_json function is used to explode JSON objects within pandas, vertically, into a new DF. The default for this function is set to the visitor_home_cbgs collumn, but can be set to any of the JSON columns you might come across in the SafeGraph data. 
<br>
* To change the column name where the Key from the Key:Value pair will go, simply add the argument 'key_col_name'
* To change the column name where the Value from the Key:Value pair will go, simply add the argument 'value_col_name'

#### unpack_json_fast(df, json_column='visitor_home_cbgs', key_col_name='visitor_home_cbg', value_col_name='cbg_visitor_count', chunk_n= 1000)
Multithreaded version of unpack_json(), reference above for more details. The parameter 'chunk_n' is the size of one chunk. The dataframe is then split into len(df)//chunk_n chunks. These chunks are what is distributed across multiple threads. 

#### unpack_json_and_merge(df, json_column='visitor_home_cbgs', key_col_name='visitor_home_cbg', value_col_name='cbg_visitor_count', keep_index=False)

The unpack_json_and_merge function is used to explode JSON objects within pandas, vertically, and then adds it back to the current DF. The default for this function is set to the visitor_home_cbgs column, but can be set to any of the JSON collumns you might come across in the SafeGraph data.
<br>
* To change the column name where the Key from the Key:Value pair will go, simply add the argument 'key_col_name'
* To change the column name where the Value from the Key:Value pair will go, simply add the argument 'value_col_name'

#### unpack_json_and_merge_fast(df, json_column='visitor_home_cbgs', key_col_name='visitor_home_cbg', value_col_name='cbg_visitor_count', keep_index=False, chunk_n=1000)
Multithreaded version of unpack_json_and_merge(), reference above for more details. The parameter 'chunk_n' is the size of one chunk. The dataframe is then split into len(df)//chunk_n chunks. These chunks are what is distributed across multiple threads. 

#### explode_json_array(df_, array_column = 'visits_by_day', value_col_name='day_visit_counts',place_key='safegraph_place_id', file_key='date_range_start', array_sequence='day', keep_index=False, zero_index=False)

The explode_json_array function is similar to the unpack_json functions, except it is designed to handle the arrays that are just Values (as opposed to Key:Value pairs). The default for this function is set to the 'visits_by_day' column, but can be set to any simple array column in the SafeGraph data by reassigning the _array_column_ argument.
<br>
* To change the column name where the array values will be displayed, simply add the argument value_col_name
* To change the column name where the array sequence will be displayed (i.e. - days, months, hours, etc), simply add the argument _array_sequence_

#### explode_json_array_fast(df, array_column = 'visits_by_day', value_col_name='day_visit_counts',place_key='safegraph_place_id', file_key='date_range_start', array_sequence='day', keep_index=False, zero_index=False, chunk_n = 1000)
Multithreaded version of unpack_json_and_merge(), reference above for more details. The parameter 'chunk_n' is the size of one chunk. The dataframe is then split into len(df)//chunk_n chunks. These chunks are what is distributed across multiple threads. 

#### read_core_folder(path_to_core, compression='gzip', args, kwargs)

The read_core_folder function is designed to take an unpacked core file and read in the 5 core values - thus creating a complete Core Files DF with specified datatypes. All Pandas arguments and keywords arguments can be passed into this function.

#### read_core_folder_zip(path_to_core, compression='gzip', args, kwargs)

The read_core_folder_zip is designed to take the raw zipped file you recieve directly from SafeGraph and create a complete Core Files DF with specified datatypes. All Pandas arguments and keywords arguments can be passed into this function.

#### read_geo_zip(path_to_geo, compression='gzip', args, kwargs)

The read_geo_zip is designed to take the raw zipped geo file you recieve directly from the [SafeGraph shop](https://shop.safegraph.com/) and create a pandas DF. All Pandas arguments and keywords arguments can be passed into this function.

#### read_pattern_single(f_path, compression='gzip', args, kwargs)

The read_pattern_single function is designed to allow the user to read in a singular patterns file of any type (weekly or monthly) and create a pandas DF with specified datatypes. All Pandas arguments and keywords arguments can be passed into this function.

#### read_pattern_multi(path_to_pattern, compression='gzip', args, kwargs)

The read_pattern_multi function is designed to read in multiple pattern files and combine them into 1 DF with specified datatypes (Warning: if columns are not specified, you can run out of memory very quickly and have an error). All Pandas arguments and keywords arguments can be passed into this function.

#### merge_core_pattern(core_df, patterns_df, how='inner', args, kwargs)

The merge_core_pattern function is designed to take a patterns DF and cross examine it with a core DF. The resulting pandas DF will be a DF with all of the values from your patterns DF as well as the matching values from your core DF (merge done on 'safegraph_place_id'). All Pandas arguments and keywords arguments can be passed into this function.

#### merge_socialDist_by_dates(path_to_social_dist, start_date, end_date, args, kwargs)

The merge_socialDist_by_dates function is designed to merge the social distancing data from a given start_date to a given end_date. The resulting pandas DF will be a DF of all social distancing data from the start_date to the end_date. All Pandas arguments and keywords arguments can be passed into this function.
* start_date and end_date are strings formated as: "year-month-day"

## Contributing
Pull requests are welcome. For major changes, please [open an issue](https://github.com/SafeGraphInc/safegraph_py/issues/new) first to discuss what you would like to change.

## You may also be interested in: 
* The [awesome-safegraph-datascience](https://github.com/SafeGraphInc/awesome-safegraph-datascience) repo
* SafeGraph's [data science resources page](https://docs.safegraph.com/docs/data-science-resources)

## License
[SafeGraph](https://github.com/SafeGraphInc/safegraph_py/blob/master/LICENSE)
