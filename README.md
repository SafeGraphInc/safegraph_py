# `safegraph_py`

`safegraph_py` is a Python library designed to make your experience with SafeGraph data as easy as possible.

These functions are [demonstrated on SafeGraph data in this Colab Notebook](https://colab.research.google.com/drive/1V7hnyYuY_dUXQEPkCMZkgMuBFQV4iA_4?usp=sharing). 

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install `safegraph_py`.

```bash
pip install -q --upgrade git+https://github.com/SafeGraphInc/safegraph_py
```

## Sections
This package is divided into two sections...the first is safegraph_py_functions (shortened to sgpy), which is for working with the JSONs and zipped files of the SafeGraph patterns data. The other is cbg_functions (shortened to cbg), which is for accessing SafeGraph's archives of the 2016-2019 ACS census block group data from the US Census.

## Safegraph_py_functions

## Usage

```python
from safegraph_py_functions import safegraph_py_functions as sgpy

sgpy.test_me() # returns 'Hello World' to ensure you have downloaded the library
sgpy.help() # returns a list of all active functions and their arguments in the safegraph_py library
sgpy.read_pattern_single(f_path) # returns a Pandas DF from a single patterns file
# etc. . . 
```

## Functions

>_A quick note before delving into the functions. There are 2 types of JSON objects that SafeGraph uses, thus 2 different functions are required. The 'unpack_json' function is >designed specifically for key:value JSON object such as: 'visitor_home_cbgs', 'visitor_daytime_cbgs', etc... For columns that contain a JSON list of values, we have the >'explode_json_array' function. These columns only have a list of values (a list of 1 data type), such as: 'visits_by_day', 'visits_by_each_hour', etc... They cannot be used >interchangeably due to the nature of the JSON objects._ 

#### unpack_json(df_, json_column='visitor_home_cbgs', key_col_name='visitor_home_cbg', value_col_name='cbg_visitor_count')

The unpack_json function is used to explode JSON objects within pandas, vertically, into a new DF. The default for this function is set to the visitor_home_cbgs column, but can be set to any of the JSON columns you might come across in the SafeGraph data. NOTE: This should be used with Key:Value columns only -- i.e. The 'visitor_home_cbgs' column. The key:values of the 'visitor_home_cbgs' look as follows: {"360610112021": 603, "460610112021": 243, "560610112021": 106, "660610112021": 87, "660610112021": 51}
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

#### explode_json_array(df_, array_column ='visits_by_day', value_col_name='day_visit_counts',place_key='safegraph_place_id', file_key='date_range_start', array_sequence='day', keep_index=False, zero_index=False)

The explode_json_array function is similar to the unpack_json functions, except it is designed to handle the arrays that are just Values (as opposed to Key:Value pairs). The default for this function is set to the 'visits_by_day' column, but can be set to any simple array column in the SafeGraph data by reassigning the _array_column_ argument. NOTE: This function should only be used with JSON objects of a list of Values (as opposed to key:value pairs). For instance in the 'visits_by_day' we have a JSON list of values only. The column appears as follows: [33, 22, 33, 22, 33, 22, 22]
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

## cbg_functions

## Usage

```python
from safegraph_py_functions import cbg_functions as cbg

sgpy.test_me_cbg() # returns 'Hello World' to ensure you have downloaded the library
sgpy.help_cbg() # returns a list of all active functions and their arguments in the cbg_functions library
sgpy.get_cbg_field_descriptions(year) - This function creates a reference table of Census data columns and their definitions
# etc. . . 
```

## Functions

#### get_cbg_field_descriptions(year=2019)

This function authenticates and creates a PyDrive client, and creates a Pandas DataFrame (via the previous two functions) providing descriptions of each Census column for user reference. Information is available for 2016 to 2019.

Example of 2019 output: 
get_cbg_field_descriptions()

(There are ten field levels, but for space, only up to seven are shown.)

table_id	table_number	table_title	table_topics	table_universe	field_level_1	field_level_2	field_level_3	field_level_4	field_level_5	field_level_6	field_level_7
B01001e1	B01001	        Sex By Age	Age and Sex	    Total population	Estimate	SEX BY AGE	    Total population	Total	         NaN	    NaN	            NaN	
B01001e10	B01001	        Sex By Age	Age and Sex	    Total population	Estimate	SEX BY AGE	    Total population	Total	         Male	    22 to 24 years	NaN
B01001e11	B01001	        Sex By Age	Age and Sex	    Total population	Estimate	SEX BY AGE	    Total population	Total	         Male	    25 to 29 years	NaN
B01001e12	B01001	        Sex By Age	Age and Sex	    Total population	Estimate	SEX BY AGE	    Total population	Total	         Male	    30 to 34 years	NaN
B01001e13	B01001	        Sex By Age	Age and Sex	    Total population	Estimate	SEX BY AGE	    Total population	Total	         Male	    35 to 39 years	NaN

#### get_census_columns(columns, year)

This function authenticates and creates a PyDrive client, and creates a Pandas DataFrame (via the first two functions) providing Census data for every census block group present in the data for the selected columns in the selected year (years available are 2016-2019). The input columns must be in a list and match the names given in the reference table in the above function.

Example output:
get_census_columns(['B01001e1', 'B01001e10', 'B01001e11', 'B01001e12'], 2019):

census_block_group	B01001e1	B01001e10	B01001e11	B01001e12
010010201001		730			30			9			30
010010201002		1263		34			22			42
010010202001		835			3			12			17
010010202002		1124		42			87			80
010010203001		2774		51			144			143

## Contributing
Pull requests are welcome. For major changes, please [open an issue](https://github.com/SafeGraphInc/safegraph_py/issues/new) first to discuss what you would like to change.

## You may also be interested in: 
* The [awesome-safegraph-datascience](https://github.com/SafeGraphInc/awesome-safegraph-datascience) repo
* SafeGraph's [data science resources page](https://docs.safegraph.com/docs/data-science-resources)

## License
[SafeGraph](https://github.com/SafeGraphInc/safegraph_py/blob/master/LICENSE)
