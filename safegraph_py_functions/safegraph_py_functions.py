import pandas as pd
import json
import os
import numpy
import glob
from zipfile import ZipFile
from functools import partial
from multiprocessing import Pool


### -------------------------------------Test and Help function -------------------------------------------------------

def test_me():
    print("Hello World")

def version():
    print("safegraph_py v1.1.0")
    
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
            $    -    Pandas *args and **kwargs are activated



  Available Functions: 

    + test_me() - A function to test the Python Libray

  ----------------------[JSON Section]----------------------

    + unpack_json() - a function to explode JSON objects within pandas vertically into a new DF
        **Arguments: 
            df*
            json_column 
            key_col_name 
            value_col_name 

    + unpack_json_and_merge() - a function to explode JSON objects within pandas vertically and add it to the current DF 
        **Arguments:
            df*
            json_column
            key_col_name
            value_col_name
            keep_index (&)

    + explode_json_array() - This function vertically explodes an array column in SafeGraph data and creates a second new column indicating the index value from the array
        **Arguments:
            df*
            array_column
            value_col_name
            place_key
            file_key
            array_sequence
            keep_index (&)
            zero_index (&)

----------------------[JSON Fast Section]----------------------

    + unpack_json_fast() - Multi-threaded version of unpack_json(). Reference unpack_json() for details and arguments.
    
    + unpack_json_and_merge_fast() - Multi-threaded version of unpack_json_and_merge(). Reference unpack_json_and_merge() for details and arguments.
    
    + explode_json_array_fast() - Multi-threaded version of explode_json_array(). Reference explode_json_array() for details and arguments.
    
-----------------[CORE, GEO, and PATTERNS section]----------------------

    + read_core_folder() - a function that concats the core files together into 1 dataframe
        **Arguments:
            path_to_core*
            compression
            $

    + read_core_folder_zip() - used to read in the Core data from the zipped core file
        **Arguments:
            path_to_core*
            compression
            $

    + read_geo_zip() - used to read in the Core Geo data from a zipped file
        **Arguments:
            path_to_geo*
            compression
            $

    + read_pattern_single() - used to read in SafeGraph data pre June 15th
        **Arguments:
            f_path*
            compression 
            $      

    + read_pattern_multi() - used to read in SafeGraph pattern data that is broken into multiple files
        **Arguments:
            path_to_pattern*
            compression
            $

    + merge_core_pattern() - used to combine the core file and the pattern files on the SafeGraph ID
        **Arguments:
            core_df*
            patterns_df*
            how
            $
            
    -----------------[Social Distancing section]----------------------

    + merge_socialDist_by_dates() - a function that concats the multiple different dates of social distancing data together into 1 dataframe
        **Arguments:
            path_to_social_dist*
            start_date*  (date as string "year-month-day")
            end_date*    (date as string "year-month-day")
            $
    

  ''')


### -------------------------------------- JSON Functions ---------------------------------------------------------------

# json.loads() but handling of missing/nan/non-string data. 

def load_json_nan(df, json_col):
  return df[json_col].apply(lambda x: json.loads(x) if type(x) == str else x)

def unpack_json(df, json_column='visitor_home_cbgs', index_name= None, key_col_name=None,
                         value_col_name=None):
    # these checks are a inefficent for multithreading, but it's not a big deal
    if key_col_name is None:
        key_col_name = json_column + '_key'
    if value_col_name is None:
        value_col_name = json_column + '_value'
    if (df.index.unique().shape[0] < df.shape[0]):
        raise ("ERROR -- non-unique index found")
    df = df.copy()
    df[json_column + '_dict'] = load_json_nan(df,json_column)
    all_sgpid_cbg_data = []  # each cbg data point will be one element in this list
    if index_name is None:
      for index, row in df.iterrows():
          this_sgpid_cbg_data = [{'orig_index': index, key_col_name: key, value_col_name: value} for key, value in
                                row[json_column + '_dict'].items()]
          all_sgpid_cbg_data = all_sgpid_cbg_data + this_sgpid_cbg_data
    else:
      for index, row in df.iterrows():
        temp = row[index_name]
        this_sgpid_cbg_data = [{'orig_index': index, index_name:temp, key_col_name: key, value_col_name: value} for key, value in
                               row[json_column + '_dict'].items()]
        all_sgpid_cbg_data = all_sgpid_cbg_data + this_sgpid_cbg_data
    
    all_sgpid_cbg_data = pd.DataFrame(all_sgpid_cbg_data)
    all_sgpid_cbg_data.set_index('orig_index', inplace=True)
    return all_sgpid_cbg_data


def unpack_json_and_merge(df, json_column='visitor_home_cbgs', key_col_name=None,
                         value_col_name=None, keep_index=False):
    if (keep_index):
        df['index_original'] = df.index
    df.reset_index(drop=True, inplace=True)  # Every row must have a unique index
    df_exp = unpack_json(df, json_column=json_column, key_col_name=key_col_name, value_col_name=value_col_name)
    df = df.merge(df_exp, left_index=True, right_index=True).reset_index(drop=True)
    return df

def explode_json_array(df, array_column = 'visits_by_day', value_col_name=None, place_key='safegraph_place_id', file_key='date_range_start', array_sequence=None, keep_index=False, zero_index=False):
    if (array_sequence is None):
      array_sequence = array_column + '_sequence'
    if (value_col_name is None):
      value_col_name = array_column + '_value'
    if(keep_index):
        df['index_original'] = df.index
    df = df.copy()
    df.reset_index(drop=True, inplace=True) # THIS IS IMPORTANT; explode will not work correctly if index is not unique
    df[array_column + '_json'] = load_json_nan(df,array_column)
    day_visits_exp = df[[place_key, file_key, array_column+'_json']].explode(array_column+'_json')
    day_visits_exp['dummy_key'] = day_visits_exp.index
    day_visits_exp[array_sequence] = day_visits_exp.groupby([place_key, file_key])['dummy_key'].rank(method='first', ascending=True).astype('int64')
    if(zero_index):
      day_visits_exp[array_sequence] = day_visits_exp[array_sequence] -1
    day_visits_exp.drop(['dummy_key'], axis=1, inplace=True)
    day_visits_exp.rename(columns={array_column+'_json': value_col_name}, inplace=True)
    day_visits_exp[value_col_name] = day_visits_exp[value_col_name].astype('int64')
    df.drop([array_column+'_json'], axis=1, inplace=True)
    return pd.merge(df, day_visits_exp, on=[place_key,file_key])

### ------------------------------------------ END JSON SECTION--------------------------------------------------------

### ------------------------------------------ JSON FAST SECTION--------------------------------------------------------

# index_name if you want your index (such as CBG) to be it's own column, then provide this 
def unpack_json_fast(df, json_column = 'visitor_home_cbgs', index_name = None, key_col_name = None, value_col_name = None, chunk_n = 1000):
    if index_name is None:
        df = df[[json_column]]
    else:
        df = df[[json_column, index_name]]
    chunks_list = [df[i:i+chunk_n] for i in range(0,df.shape[0],chunk_n)]
    
    partial_unpack_json = partial(unpack_json, json_column=json_column, index_name= index_name, key_col_name= key_col_name, value_col_name= value_col_name)
    with Pool() as pool:
        results = pool.map(partial_unpack_json,chunks_list)
    return pd.concat(results)

def unpack_json_and_merge_fast(df, json_column='visitor_home_cbgs', key_col_name=None,
                         value_col_name=None, keep_index=False, chunk_n = 1000):
    if (keep_index):
        df['index_original'] = df.index
    
    df.reset_index(drop=True, inplace=True)  # Every row must have a unique index
    df_exp = df[[json_column]] 
    df_exp = unpack_json_fast(df_exp, json_column=json_column, key_col_name=key_col_name, value_col_name=value_col_name, chunk_n=chunk_n)
    df = df.merge(df_exp, left_index=True, right_index=True).reset_index(drop=True)
    return df

def explode_json_array_fast(df, array_column = 'visits_by_day', place_key='safegraph_place_id', file_key='date_range_start', value_col_name=None, array_sequence=None, keep_index=False, zero_index=False, chunk_n = 1000):
    df_subset = df[[array_column,place_key,file_key]] # send only what we need
    chunks_list = [df_subset[i:i+chunk_n] for i in range(0,df_subset.shape[0],chunk_n)] 
    partial_explode_json = partial(explode_json_array, array_column=array_column, value_col_name= value_col_name, place_key= place_key,
                       file_key = file_key, array_sequence = array_sequence, zero_index = zero_index)
    with Pool() as pool:
        results = pool.map(partial_explode_json,chunks_list)
    df_subset = pd.concat(results)
    df_subset.drop([array_column],axis=1,inplace=True) # preparing to merge by dropping duplicates
    
    return df.merge(df_subset, on=[place_key,file_key])

### ------------------------------------------ END JSON FAST SECTION--------------------------------------------------------

### ---------------------------------------CORE, GEO, AND PATTERNS SECTION -----------------------------------------------


def read_core_folder(path_to_core, compression='gzip',*args, **kwargs):
    core_files = glob.glob(os.path.join(path_to_core, "*.csv.gz"))
    print(f"You are about to load in {len(core_files)} core files")

    li = []
    for core in core_files:
        print(core)
        df = pd.read_csv(core, compression=compression, dtype={'postal_code': str, 'phone_number': str, 'naics_code': str}, *args, **kwargs)
        li.append(df)

    SG_core = pd.concat(li, axis=0)
    return SG_core

### added a new core read that takes the information straight from the zipped file (like you get it from the catelog)

def read_core_folder_zip(path_to_core, compression='gzip', *args, **kwargs):
    zip_file = ZipFile(path_to_core)

    dfs = {text_file.filename: pd.read_csv(zip_file.open(text_file.filename), compression=compression, dtype={'postal_code': str, 'phone_number': str, 'naics_code': str}, *args, **kwargs)
           for text_file in zip_file.infolist()
           if text_file.filename.endswith('.csv.gz')}

    SG_core = pd.concat(dfs, axis=0, ignore_index=True)

    return SG_core

def read_geo_zip(path_to_geo, compression='gzip', *args, **kwargs):
  zf = ZipFile(path_to_geo)
  result=pd.read_csv(zf.open('core_poi-geometry.csv.gz'), compression=compression, dtype={'postal_code': str, 'phone_number': str, 'naics_code': str}, *args, **kwargs)
  return result


def read_pattern_single(f_path, compression='gzip', *args, **kwargs):
    df = pd.read_csv(f_path, dtype={'postal_code': str, 'phone_number': str, 'naics_code': str}, compression=compression, *args, **kwargs)
    return df


def read_pattern_multi(path_to_pattern, compression='gzip', *args, **kwargs):
    pattern_files = glob.glob(os.path.join(path_to_pattern, "*.csv.gz"))
    print(f"You are about to load in {len(pattern_files)} pattern files")

    li = []
    for pattern in pattern_files:
        print(pattern)
        df = pd.read_csv(pattern, compression=compression, *args, **kwargs,
                         dtype={'postal_code': str, 'phone_number': str, 'naics_code': str})
        li.append(df)

    SG_pattern = pd.concat(li, axis=0)
    return SG_pattern


def merge_core_pattern(core_df, patterns_df, how='inner', *args, **kwargs):
    merged_df = pd.merge(core_df, patterns_df, on='safegraph_place_id', how=how, *args, **kwargs)
    return merged_df


### --------------------------------------- END CORE, GEO, AND PATTERNS SECTION -----------------------------------------------

### --------------------------------------- SOCIAL DISTANCING SECTION -----------------------------------------------

## start_date and end_date = string formated as "year-month-day" 
    ## ex: start_date = "2020-06-01", end_date = "2020-06-07"

def merge_socialDist_by_dates(path_to_social_dist,start_date,end_date, *args, **kwargs):
    path = os.path.join(path_to_social_dist,start_date[:4])
    if(start_date[5:7] == end_date[5:7]): # same month
        path = os.path.join(path,end_date[5:7])
        files = [file.path for day in os.scandir(path) for file in os.scandir(day.path)][int(start_date[-2:])-1:int(end_date[-2:])]
    else:
        path2 = os.path.join(path, end_date[5:7]) # not same month, so different folder
        path = os.path.join(path, start_date[5:7])
        last_day = int(sorted(os.listdir(path))[-1]) # last day of month    
        files = [file.path for day in os.scandir(path) for file in os.scandir(day.path)][int(start_date[-2:])-1:last_day]
        files.extend([file.path for day in os.scandir(path2) for file in os.scandir(day.path)][:int(end_date[-2:])])
    li = []
    for file in files:
        temp_df = pd.read_csv(file,dtype= {'origin_census_block_group':str}, *args, **kwargs)
        li.append(temp_df)
    return pd.concat(li, axis=0,ignore_index=True)

### --------------------------------------- END SOCIAL DISTANCING SECTION -----------------------------------------------
