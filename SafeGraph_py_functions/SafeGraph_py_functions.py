
import pandas as pd
import json
import os
import numpy
import glob


### -------------------------------------Test and Help function -------------------------------------------------------

def Test_me():
    print("hello world")


def help():
    print('''
 
 ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
.d8888b.            .d888          .d8888b.                            888           8888888b.           888    888                             888      d8b 888                                       
d88P  Y88b          d88P"          d88P  Y88b                          888           888   Y88b          888    888                             888      Y8P 888                                       
Y88b.               888            888    888                          888           888    888          888    888                             888          888                                       
 "Y888b.    8888b.  888888 .d88b.  888        888d888 8888b.  88888b.  88888b.       888   d88P 888  888 888888 88888b.   .d88b.  88888b.       888      888 88888b.  888d888 8888b.  888d888 888  888 
    "Y88b.     "88b 888   d8P  Y8b 888  88888 888P"      "88b 888 "88b 888 "88b      8888888P"  888  888 888    888 "88b d88""88b 888 "88b      888      888 888 "88b 888P"      "88b 888P"   888  888 
      "888 .d888888 888   88888888 888    888 888    .d888888 888  888 888  888      888        888  888 888    888  888 888  888 888  888      888      888 888  888 888    .d888888 888     888  888 
Y88b  d88P 888  888 888   Y8b.     Y88b  d88P 888    888  888 888 d88P 888  888      888        Y88b 888 Y88b.  888  888 Y88..88P 888  888      888      888 888 d88P 888    888  888 888     Y88b 888 
 "Y8888P"  "Y888888 888    "Y8888   "Y8888P88 888    "Y888888 88888P"  888  888      888         "Y88888  "Y888 888  888  "Y88P"  888  888      88888888 888 88888P"  888    "Y888888 888      "Y88888 
                                                              888                                    888                                                                                           888 
                                                              888                               Y8b d88P                                                                                      Y8b d88P 
                                                              888                                "Y88P"                                                                                        "Y88P"  
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

 HELP:

  Welcome to the safegraph helper function. Below you will find a list of functions and their arguments to aid in your datascience journey. If you have further questions that cannot
  be answered by this help command, please do not hesitate to ask for assistance in the #python_troubleshooting slack channel. 

        Key:
            *    -    Required Argument
            &    -    Boolean value
            
            

  Available Functions: 

    + Test_me() - A function to test the Python Libray
  
  ----------------------[JSON Section]----------------------
    
    + explode_visitor_home_cbg() - a function to explode visitor home cbg column vertically 
        **Arguments: 
            df*
            json_column 
            key_column_name 
            value_column_name 
            keep_index (&)
            
    + explode_json_array() - explodes any generic json array | Explodes 'visit_by_day' by default. . . 
        **Arguments:
            df*
            array_column
            new_col
            place_key
            file_key
            array_sequence
            keep_index (&)
            verbose (&)
            zero_index (&)

-----------------[CORE and PATTERNS section]----------------------
            
    + read_core_folder() - a function that concats the core files together into 1 dataframe
        **Arguments:
            path_to_core*
            use_cols

    + read_pattern_demo() - gives a quick read of a patterns file to see how the data looks
        **Arguments:
            f_path*
            use_cols
            compression
            nrows
    
    + read_pattern_single() - used to read in SafeGraph data pre June 15th
        **Arguments:
            f_path*
            use_cols
            compression       
    
    + read_pattern_multi() - used to read in SafeGraph pattern data that is broken into multiple files
        **Arguments:
            path_to_pattern*
            use_cols
            compression
            
    + merge_pattern_core() - used to combine the core file and the pattern files on the SafeGraph ID
        **Arguments:
            patterns_df*
            core_df*
            how

  ''')

### -------------------------------------- JSON Functions ---------------------------------------------------------------

def vertically_explode_json(df_, json_column='visitor_home_cbgs', key_col_name='visitor_home_cbg',
                            value_col_name='cbg_visitor_count'):
    df = df_.copy()
    if (df.index.unique().shape[0] < df.shape[0]):
        raise ("ERROR -- non-unique index found")
    df[json_column + '_dict'] = [json.loads(cbg_json) for cbg_json in df[json_column]]
    all_sgpid_cbg_data = []  # each cbg data point will be one element in this list
    for index, row in df.iterrows():
        this_sgpid_cbg_data = [{'orig_index': index, key_col_name: key, value_col_name: value} for key, value in
                               row[json_column + '_dict'].items()]
        all_sgpid_cbg_data = all_sgpid_cbg_data + this_sgpid_cbg_data
    output = pd.DataFrame(all_sgpid_cbg_data)
    output.set_index('orig_index', inplace=True)
    return (output)

def explode_visitor_home_cbg(df, json_column='visitor_home_cbgs', key_col_name='visitor_home_cbg', value_col_name='cbg_visitor_count', keep_index=False):
    if(keep_index):
        df['index_original'] = df.index
    df = df.dropna(subset = [json_column]).copy() # Drop nan jsons
    df.reset_index(drop=True, inplace=True) # Every row must have a unique index
    df_exp = vertically_explode_json(df, json_column=json_column, key_col_name=key_col_name, value_col_name=value_col_name)
    df = df.merge(df_exp, left_index=True, right_index=True).reset_index(drop=True)
    return(df)


def explode_json_array(df_, array_column = 'visits_by_day', new_col='day_visit_counts',place_key='safegraph_place_id', file_key='date_range_start', array_sequence='day', keep_index=False, verbose=True, zero_index=False):
    # This function vertically explodes an array column in SafeGraph data and creates a second new column indicating the index value from the array
    df = df_.copy()
    if(verbose): print("Running explode_json_array()")
    if(keep_index):
        df['index_original'] = df.index
    df.reset_index(drop=True, inplace=True) # THIS IS IMPORTANT; explode will not work correctly if index is not unique
    df[array_column+'_json'] = [json.loads(myjson) for myjson in df[array_column]]
    day_visits_exp = df[[place_key, file_key, array_column+'_json']].explode(array_column+'_json')
    day_visits_exp['dummy_key'] = day_visits_exp.index
    day_visits_exp[array_sequence] = day_visits_exp.groupby([place_key, file_key])['dummy_key'].rank(method='first', ascending=True).astype('int64')
    if(zero_index):
      day_visits_exp[array_sequence] = day_visits_exp[array_sequence] -1
    day_visits_exp.drop(['dummy_key'], axis=1, inplace=True)
    day_visits_exp.rename(columns={array_column+'_json': new_col}, inplace=True)
    day_visits_exp[new_col] = day_visits_exp[new_col].astype('int64')
    df.drop([array_column+'_json'], axis=1, inplace=True)
    df = pd.merge(df, day_visits_exp, on=[place_key,file_key])
    return(df)

def unpack_json_key_value_data(df, json_column='visitor_home_cbgs', key_col_name='visitor_home_cbg', value_col_name='cbg_visitor_count', keep_index=False):
    if(keep_index):
        df['index_original'] = df.index
    df = df.dropna(subset = [json_column]).copy() # Drop nan jsons
    df.reset_index(drop=True, inplace=True) # Every row must have a unique index
    df_exp = vertically_explode_json(df, json_column=json_column, key_col_name=key_col_name, value_col_name=value_col_name)
    df = df.merge(df_exp, left_index=True, right_index=True).reset_index(drop=True)
    return(df)

### ------------------------------------------ END JSON SECTION--------------------------------------------------------
### __________________________________________CORE AND PATTERNS SECTION -----------------------------------------------

# Hard coded variable for columns

use_cols_core = ["safegraph_place_id", "location_name", "parent_safegraph_place_id", "safegraph_brand_ids", "brands",
            "top_category", "sub_category", 'naics_code', "latitude", "longitude", "street_address", "city", "region",
            "postal_code", "iso_country_code", "phone_number", "open_hours", "category_tags"]

use_cols_pattern = ["safegraph_place_id",	"location_name",	"street_address",	"city",	"region",	"postal_code",	"iso_country_code",
                    "safegraph_brand_ids",	"brands",	"date_range_start",	"date_range_end",	"raw_visit_counts",	"raw_visitor_counts",
                    "visits_by_day",	"visits_by_each_hour",	"poi_cbg",	"visitor_home_cbgs",	"visitor_daytime_cbgs",	"visitor_country_of_origin",
                    "distance_from_home",	"median_dwell",	"bucketed_dwell_times",	"related_same_day_brand",	"related_same_week_brand",	"device_type"]


def read_core_folder(path_to_core, use_cols=use_cols_core):
    core_files = glob.glob(os.path.join(path_to_core, "*.csv.gz"))
    print(f"You are about to load in {len(core_files)} core files")

    li = []
    for core in core_files:
        print(core)
        df = pd.read_csv(core, usecols=use_cols, compression='gzip',
                         dtype={'postal_code': str, 'phone_number': str, 'naics_code': str})
        li.append(df)

    SG_core = pd.concat(li, axis=0)
    return SG_core


def read_pattern_demo(f_path, use_cols=use_cols_pattern, compression='gzip', nrows=100):
    df = pd.read_csv(f_path, dtype={'postal_code': str, 'phone_number': str, 'naics_code': str}, nrows=nrows, compression=compression,
                     use_cols=use_cols)
    return df

def read_pattern_single(f_path, use_cols=use_cols_pattern, compression='gzip'):
  df = pd.read_csv(f_path, dtype={'postal_code': str, 'phone_number': str, 'naics_code': str}, usecols=use_cols, compression=compression)
  return df


def read_pattern_multi(path_to_pattern, use_cols=use_cols_pattern, compression='gzip'):
    pattern_files = glob.glob(os.path.join(path_to_pattern, "*.csv.gz"))
    print(f"You are about to load in {len(pattern_files)} pattern files")

    li = []
    for pattern in pattern_files:
        print(pattern)
        df = pd.read_csv(pattern, usecols=use_cols, compression=compression,
                         dtype={'postal_code': str, 'phone_number': str, 'naics_code': str})
        li.append(df)

    SG_pattern = pd.concat(li, axis=0)
    return SG_pattern

def merge_pattern_core(patterns_df, core_df, how='left'):
  merged_df = pd.merge(patterns_df, core_df, on='safegraph_place_id', how=how)
  return merged_df