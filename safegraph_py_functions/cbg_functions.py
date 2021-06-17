import pandas as pd
import json
import os
import numpy
import glob
from zipfile import ZipFile
from functools import partial
from multiprocessing import Pool

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from google.colab import auth
from oauth2client.client import GoogleCredentials

def test_me_sbg():
    print("Hello World")

def version_sbg():
    print("safegraph_py v1.1.1")
    
def help_sbg():
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

  Welcome to the safegraph helper function. Below you will find a list of functions and their arguments to aid in your data science journey. If you have further questions that cannot
  be answered by this help command, please do not hesitate to ask for assistance in the #python_troubleshooting slack channel. 

        Key:
            *    -    Required Argument



  Available Functions: 

    + test_me_sbg() - A function to test the Python library script...a proper result should be the printing of "Hello World".

    + get_drive_id() - A function to pull input files from Google Drive. It requires a year and a dictionary of Google Drive IDs with the requisite respective data. This function is used automatically within other functions, so possession of said dictionary is not necessary.
        **Arguments: 
            year*
            drive_ids*

    + pd_read_csv_drive() - A function to pull input files from Google Drive into pandas dataframes. This function takes the output of the chosen year from the previous function, get_drive_id, as its first input. The second input is a Google Drive object, automatically created within the functions that use these functions.
        **Arguments:
            id*
            drive*
            dtype

    + get_cbg_field_descriptions() - This function authenticates and creates a PyDrive client, and creates a Pandas DataFrame (via the previous two functions) providing descriptions of each Census column for user reference. There is one present for years 2016-2019, and the default is 2019.
        **Arguments:
            year*

    + get_census_columns() - This function authenticates and creates a PyDrive client, and creates a Pandas DataFrame (via the first two functions) providing Census data for every census block group present in the data for the selected columns in the selected year (years available are 2016-2019). Columns must be in a list and match the names given in the reference table in the above function.
        **Arguments:
            columns*
            year*
  ''')

def get_drive_id(year, drive_ids): #function to pull input files from Google Drive
    return(drive_ids[str(year)])

def pd_read_csv_drive(id, drive, dtype = None): #function to pull input files from Google Drive into pandas dataframes
    downloaded = drive.CreateFile({'id':id})
    downloaded.GetContentFile('Filename.csv')  
    return(pd.read_csv('Filename.csv',dtype=dtype))

def get_cbg_field_descriptions(year=2019):
  year = str(year)
  auth.authenticate_user()  # Authenticate and create the PyDrive client. 
  gauth = GoogleAuth()
  gauth.credentials = GoogleCredentials.get_application_default()
  drive = GoogleDrive(gauth)

  final_table = pd_read_csv_drive(get_drive_id(year,{'2016' : '13dLXo67IZDh3OZl042GQYG16Qn_NB7sz',
                  '2017' : '1b2RVBDgzdrDJkL0OCYRMGBc5zbLhv5MB',
                  '2018' : '1r7z3efdS5viIRMsQzu9ExHoIL29QjaVi',
                  '2019' : '1fJsLm6voxWsTq5FQrUzO9PpBltQ8n_lJ'
                  }), drive)
  return(final_table)

def get_census_columns(columns, year):
    auth.authenticate_user()  # Authenticate and create the PyDrive client. 
    gauth = GoogleAuth()
    gauth.credentials = GoogleCredentials.get_application_default()
    drive = GoogleDrive(gauth)

    year = str(year)

    file_list_2016 = dict([(i['title'], i['id']) for i in drive.ListFile({'q': "'17TexQTOM0RmpbekqNBvsiKs_jPH8ynQR' in parents"}).GetList() if i['title'].startswith('cbg')])
    if 'cbg_patterns.csv' in file_list_2016:
      del file_list_2016['cbg_patterns.csv']
    file_list_2017 = dict([(i['title'], i['id']) for i in drive.ListFile({'q': "'1jVF5Z5gf84AL09pGq_4nKAsfIM-lVrqi' in parents"}).GetList()])
    file_list_2018 = dict([(i['title'], i['id']) for i in drive.ListFile({'q': "'1g5uFI6ZfV2lPieZF63b4VYhqNiwe7dLC' in parents"}).GetList()])
    file_list_2019 = dict([(i['title'], i['id']) for i in drive.ListFile({'q': "'1LJFuG74zoy1FpMg3tcJ_UBCCA4OwGcpU' in parents"}).GetList()])

    file_list_dict = {
      '2016': file_list_2016,
      '2017': file_list_2017,
      '2018': file_list_2018,
      '2019': file_list_2019}

    column_dict = {}
    columns_short = [str.lower(i[0:3]) for i in columns]
    for i in range(0, len(columns)):
      for j in list(get_drive_id(year, file_list_dict).keys()):
        if columns_short[i] in j:
          if j not in column_dict:
            column_dict[j] = ['census_block_group']
          column_dict[j].append(columns[i])
    table_dict = dict([(i, pd_read_csv_drive(get_drive_id(year, file_list_dict)[i], drive)[column_dict[i]]) for i in column_dict.keys()])
    dfs = table_dict.values()
    dfs = [x.set_index('census_block_group') for x in dfs]
    df = pd.concat(dfs, axis=1, keys=range(1, len(dfs) + 1))
    df.columns = df.columns.map('{0[1]}'.format)
    for col in df.columns:
      df[col] = df[col].apply(lambda x: int(x) if x == x else "")
    df.reset_index(inplace = True)
    df['census_block_group'] = [i.zfill(12)for i in df['census_block_group'].astype(str)]
    return df
