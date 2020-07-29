import pandas as pd
import json
import os
import numpy
import glob
from zipfile import ZipFile


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

    + unpack_json_separate() - a function to explode JSON objects within pandas vertically into a new DF
        **Arguments: 
            df*
            json_column 
            key_column_name 
            value_column_name 

    + unpack_json_together() - a function to explode JSON objects within pandas vertically and add it to the current DF 
        **Arguments:
            df*
            json_column
            key_col
            value_col
            keep_index (&)

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

    + merge_core_pattern() - used to combine the core file and the pattern files on the SafeGraph ID
        **Arguments:
            core_df*
            patterns_df*
            how

  ''')


### -------------------------------------- JSON Functions ---------------------------------------------------------------

def unpack_json(df_, json_column='visitor_home_cbgs', key_col_name='visitor_home_cbg',
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
    return output


def unpack_json_and_merge(df, json_column='visitor_home_cbgs', key_col_name='visitor_home_cbg',
                         value_col_name='cbg_visitor_count', keep_index=False):
    if (keep_index):
        df['index_original'] = df.index
    df = df.dropna(subset=[json_column]).copy()  # Drop nan jsons
    df.reset_index(drop=True, inplace=True)  # Every row must have a unique index
    df_exp = unpack_json(df, json_column=json_column, key_col_name=key_col_name, value_col_name=value_col_name)
    df = df.merge(df_exp, left_index=True, right_index=True).reset_index(drop=True)
    return df


### ------------------------------------------ END JSON SECTION--------------------------------------------------------

### ---------------------------------------CORE AND PATTERNS SECTION -----------------------------------------------


def read_core_folder(path_to_core, use_cols=None):
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

### added a new core read that takes the information straight from the zipped file (like you get it from the catelog)

def read_core_folder_zip(path_to_core, use_cols=None):
    zip_file = ZipFile(path_to_core)

    li = []

    dfs = {text_file.filename: pd.read_csv(zip_file.open(text_file.filename), usecols=use_cols, compression='gzip',
                         dtype={'postal_code': str, 'phone_number': str, 'naics_code': str})
           for text_file in zip_file.infolist()
           if text_file.filename.endswith('.csv.gz')}

    SG_core = pd.concat(dfs, axis=0, ignore_index=True)

    return SG_core

def read_pattern_demo(f_path, use_cols=None, compression='gzip', nrows=100):
    df = pd.read_csv(f_path, dtype={'postal_code': str, 'phone_number': str, 'naics_code': str}, nrows=nrows,
                     compression=compression,
                     usecols=use_cols)
    return df


def read_pattern_single(f_path, use_cols=None, compression='gzip'):
    df = pd.read_csv(f_path, dtype={'postal_code': str, 'phone_number': str, 'naics_code': str}, usecols=use_cols,
                     compression=compression)
    return df


def read_pattern_multi(path_to_pattern, use_cols=None, compression='gzip'):
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


def merge_core_pattern(core_df, patterns_df, how='inner'):
    merged_df = pd.merge(core_df, patterns_df, on='safegraph_place_id', how=how)
    return merged_df