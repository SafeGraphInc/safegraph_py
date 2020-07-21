
import numpy as np
import pandas as pd
import os


# ~~~~~~~~~~~~~~ Loading Public GDrive into Colab Notebooks ~~~~~~~~~~

def pd_read_csv_drive(id, drive, dtype=None):
  downloaded = drive.CreateFile({'id':id}) 
  downloaded.GetContentFile('Filename.csv')  
  return(pd.read_csv('Filename.csv',dtype=dtype))

def get_drive_id(filename):

    # Note: OpenCensusData public GDrive folder: https://drive.google.com/open?id=1btSS6zo7_wJCCXAigkbhnaoeU-Voa9pG
    # Note: Sample of Patterns data public GDrive folder: https://drive.google.com/open?id=1xC8RFmrF3f6laRH08kOPBRLHEwJ8c41h
    drive_ids = {'cbg_b01.csv' : '1QqttoDRoKpZM2TyyRwJ8B9c5bYZrHysB',
                'cbg_b02.csv' : '1Zqqf3iLDkDWPl2theLlUm_cAbvznj-Kx',
                'cbg_b03.csv' : '1LVvZfx3hHiwN3YBh7pz43Y2KWV61OJFA',
                'cbg_b15.csv' : '1xeSZShcX3egZFsalGOFD6Ze2jTof6ri-',
                'cbg_b19.csv' : '1d9GscpWbrnP2xNLKKlgd6xLcFTzJydY4',
                'cbg_field_descriptions.csv' : '1a7_7WxY6eaUIObkVwfknl9C7nPltYxPd',
                'cbg_fips_codes.csv' : '1dB_HeAw11TmsZ8MATMedC9j2csTRAiVm',
                'home_panel_summary.csv': '1aiwhO6Pw1ZfUOoqUf6mS70s9tJgsAVp5',
                'core_poi-patterns.csv' : '1vOiASCoWVIppoYK8DiyLShhH7xbZhfxA'
    }
    if(filename):
        return(drive_ids[filename])
    else:
        return(drive_ids)

# ~~~~~~~~~~~~~~ Wrangle Open Census Data Data Functions~~~~~~~~~~

from functools import reduce


def flatten_list(mylist):
    newlist = []
    for item in mylist:
        if(type(item) != list):
            item = [item]
        newlist = newlist + item
    return(newlist)  

def get_cbg_field_desc(ocd_dir=None, drive=None):
    if(ocd_dir):
        df = pd.read_csv(os.path.join(ocd_dir,"metadata","cbg_field_descriptions.csv"))
    elif(drive):
        df = pd_read_csv_drive(get_drive_id('cbg_field_descriptions.csv'), drive)
    return(df)

def get_age_by_sex_groups():
    age_groups = {'Ages 15-29': ['B01001e30', 'B01001e31', 'B01001e32', 'B01001e33', 'B01001e34', 'B01001e35','B01001e6', 'B01001e7', 'B01001e8', 'B01001e9', 'B01001e10', 'B01001e11'],
                  'Ages 30-49' : ['B01001e36', 'B01001e37', 'B01001e38', 'B01001e39','B01001e12', 'B01001e13', 'B01001e14', 'B01001e15'],
                  'Ages 50 and over' : ['B01001e40', 'B01001e41', 'B01001e42', 'B01001e43', 'B01001e44', 'B01001e45', 'B01001e46', 'B01001e47', 'B01001e48', 'B01001e49','B01001e16', 'B01001e17', 'B01001e18', 'B01001e19', 'B01001e20', 'B01001e21', 'B01001e22', 'B01001e23', 'B01001e24', 'B01001e25'],
    }
    
    age_groups_new_codes = {'Ages 15-29' : 'B01P1529',
                            'Ages 30-49' : 'B01P3049',
                            'Ages 50 and over' : 'B01P50ov'
                           }
    
    return(age_groups, age_groups_new_codes)
 
def get_household_income_groups():
    inc_groups =  {'Less than $59,999' : ['B19001e2','B19001e3', 'B19001e4', 'B19001e5', 'B19001e6', 'B19001e7', 'B19001e8', 'B19001e9','B19001e10', 'B19001e11',],
                  '$60,000 To $99,999' : ['B19001e12','B19001e13'],
                  '$100,000 Or More' : ['B19001e14','B19001e15','B19001e16','B19001e17']
                 }
    
    # Some new made-up codes
    inc_groups_new_codes = {'Less than $59,999' : 'B19xxx59',
                            '$60,000 To $99,999' : 'B1960x99',
                            '$100,000 Or More' : 'B19100xx'
                           }
    
    return(inc_groups, inc_groups_new_codes)

def get_edu_attainment_groups():
    
    edu_groups =  {"Less than High School Diploma" : ['B15003e10','B15003e11','B15003e12','B15003e13','B15003e14','B15003e15','B15003e16','B15003e5','B15003e6','B15003e7','B15003e8','B15003e9','B15003e2','B15003e3','B15003e4'],
                   "HS Diploma or GED" : ['B15003e17','B15003e18'],
                   "Some College and/or Associate's Degree" : ['B15003e19','B15003e20','B15003e21'],
                   "Bachelor's Degree" : ['B15003e22'],
                   "Master's, Doctorate, and/or Prof School Degree" : ['B15003e23','B15003e24','B15003e25']
                 }
    
    # Some new made-up codes
    edu_groups_new_codes = {'Less than High School Diploma' : 'B15003aa',
                            "HS Diploma or GED" : 'B15003bb',
                            "Some College and/or Associate's Degree" : 'B15003cc',
                            "Bachelor's Degree" : 'B15003dd',
                            "Master's, Doctorate, and/or Prof School Degree" :'B15003ee'
                 }
    
    return(edu_groups, edu_groups_new_codes)

def pull_vals_of_dict_into_list(my_dict):
    return(flatten_list([val for key,val in my_dict.items()]))

def get_final_table_ids(field_level_1):
    # final_codes are the table_ids we expect in our final cleaned and aggregated data 
    # (including made up codes; fake agg codes are substituted in for the unaggregated codes_
        
    inc_read_codes, inc_final_codes = get_household_income_groups()
    edu_read_codes, edu_final_codes = get_edu_attainment_groups()
    age_read_codes, age_final_codes = get_age_by_sex_groups()
    
    
    final_codes = {'Sex By Age' : pull_vals_of_dict_into_list(age_final_codes),
                   'Hispanic Or Latino Origin' : ['B03003e3', 'B03003e2'],
                   'Race' : ['B02001e2','B02001e3','B02001e4','B02001e5','B02001e6','B02001e7','B02001e8'],
                   'Educational Attainment For The Population 25 Years And Over' : pull_vals_of_dict_into_list(edu_final_codes),
                   'Aggregate Household Income In The Past 12 Months (In 2016 Inflation-Adjusted Dollars)' : pull_vals_of_dict_into_list(inc_final_codes)
                  }
    
    return(final_codes[field_level_1])

def get_census_prefix(field_level_1_list, cbg_field_desc):
    prefixes = [table_id[0:3].lower() for table_id in cbg_field_desc[cbg_field_desc.field_level_1.isin(field_level_1_list)].table_id]
    return(list(set(prefixes)))


def aggregate_census_columns(cen_df_, cbg_field_desc_, agg_groups, agg_groups_new_codes, field_level_1_str, field_level_3_str):
    cen_df = cen_df_.copy()
    
    # Make the Aggregations into new columns
    new_field_desc_list = []
    for agg_group, codes in agg_groups.items():
        new_made_up_code = agg_groups_new_codes[agg_group]
        cen_df[new_made_up_code] =  cen_df[codes].sum(axis='columns') 
        cen_df.drop(codes,axis='columns',inplace=True) # drop the old columns we just aggregated
        new_field_desc_list.append( pd.DataFrame({'table_id' : new_made_up_code,
                                                  'field_level_1' : field_level_1_str,
                                                  'field_level_2' : agg_group,
                                                  'field_level_3' : field_level_3_str
                                                 }, index=[0])
                                  ) # add new aggregations to our field_desc object
        
    new_field_desc = pd.concat(new_field_desc_list)
    cbg_field_desc_ = pd.concat([cbg_field_desc_, new_field_desc],sort=True)
    return(cen_df, cbg_field_desc_)

def aggregate_ageSex_vars(cen_df_, cbg_field_desc_):
    
    cen_df = cen_df_.copy() # to avoid assignment warning
    age_groups, age_groups_new_codes = get_age_by_sex_groups()
    field_level_1_str = 'Sex By Age'
    field_level_3_str = 'Total Population -- (Estimate)'
    cen_df, cbg_field_desc_ = aggregate_census_columns(cen_df, cbg_field_desc_, age_groups, age_groups_new_codes, field_level_1_str, field_level_3_str)
   
    return(cen_df, cbg_field_desc_)

def aggregate_HouseholdIncome_vars(cen_df_, cbg_field_desc_):
    
    cen_df = cen_df_.copy() # to avoid assignment warning
    inc_groups, inc_groups_new_codes = get_household_income_groups()
    field_level_1_str = 'Household Income In The Past 12 Months (In 2016 Inflation-Adjusted Dollars)'
    field_level_3_str = 'Households -- (Estimate)'
    cen_df, cbg_field_desc_ = aggregate_census_columns(cen_df, cbg_field_desc_, inc_groups, inc_groups_new_codes, field_level_1_str, field_level_3_str)
   
    return(cen_df, cbg_field_desc_)

def aggregate_edu_variables(cen_df_, cbg_field_desc_):
    
    cen_df = cen_df_.copy() # to avoid assignment warning
    edu_groups, edu_groups_new_codes = get_edu_attainment_groups()
    field_level_1_str = 'Educational Attainment For The Population 25 Years And Over'
    field_level_3_str = 'Population 25 Years And Over -- (Estimate)'
    cen_df, cbg_field_desc_ = aggregate_census_columns(cen_df,cbg_field_desc_, edu_groups, edu_groups_new_codes, field_level_1_str, field_level_3_str)
   
    return(cen_df, cbg_field_desc_)


def reaggregate_census_data(cen_df, cbg_field_desc, demos_to_analyze, verbose=False):
    # Manually re-aggregate some categories into fewer columns
    # optionally print shape of df after each reaggregation to see total columns shrinking
    
    if(verbose): print("Starting reaggregations (this is a slow step).\ncensus data starting shape: \n{0}".format(cen_df.shape))
    if 'Educational Attainment For The Population 25 Years And Over' in demos_to_analyze:
        cen_df, cbg_field_desc = aggregate_edu_variables(cen_df, cbg_field_desc)
        if(verbose): print("Education aggregation complete.\n{0}".format(cen_df.shape))
    if 'Sex By Age' in demos_to_analyze:
        cen_df, cbg_field_desc = aggregate_ageSex_vars(cen_df, cbg_field_desc)
        if(verbose): print("Age aggregation complete.\n{0}".format(cen_df.shape))
    if 'Aggregate Household Income In The Past 12 Months (In 2016 Inflation-Adjusted Dollars)' in demos_to_analyze:
        cen_df, cbg_field_desc = aggregate_HouseholdIncome_vars(cen_df, cbg_field_desc)
        if(verbose): print("Income aggregation complete.\n{0}".format(cen_df.shape))
    
    # Drop all the columns that are not essential to our cause
    columns_to_keep = flatten_list([flatten_list(get_final_table_ids(demo)) for demo in demos_to_analyze]) + ['census_block_group', 'B01001e1']
    cen_df = cen_df[columns_to_keep]
    if(verbose): print("Dropped unused columns.\n{0}".format(cen_df.shape))
    
    return(cen_df, cbg_field_desc)

def get_raw_census_data(demos_to_analyze, open_census_data_dir, drive=None, verbose=False):
    # demos_to_analyze is a list of length 1 to 5 containing field_level_1 values  
    # open_census_data_dir is the path where the Open Census Data is located
    # alternatively if you pass a drive object from google coLab e.g. drive = GoogleDrive(gauth), 
    #.  then these functions will read from the public Google Drive sharing open census data. 
    # Note: OpenCensusData public GDrive folder: https://drive.google.com/drive/u/1/folders/1btSS6zo7_wJCCXAigkbhnaoeU-Voa9pG
    
    # These are the supported options for field_level_1 strings: 
    #    'Sex By Age', 
    #    'Race', 
    #    'Hispanic Or Latino Origin', 
    #    'Educational Attainment For The Population 25 Years And Over',
    #    'Aggregate Household Income In The Past 12 Months (In 2016 Inflation-Adjusted Dollars)'
    
    cbg_field_desc = get_cbg_field_desc(ocd_dir=open_census_data_dir, drive=drive)
    prefixes = set(get_census_prefix(demos_to_analyze, cbg_field_desc) + ['b01']) # 'b01' we need for total_population

    if(verbose): print("Pulling census data from {0} for:\n{1}".format((open_census_data_dir or drive), '\n'.join(demos_to_analyze)))
    if(open_census_data_dir):
        ocd_files = [os.path.join(open_census_data_dir, 'data',filepath) for filepath in os.listdir(open_census_data_dir + 'data/') if filepath[4:7] in prefixes]
        census_df_raw = [pd.read_csv(file,dtype = {'census_block_group': str}) for file in ocd_files]
    elif(drive):
        ocd_files = ['cbg_'+prefix+'.csv' for prefix in prefixes]
        census_df_raw = [pd_read_csv_drive(get_drive_id(file), drive, dtype = {'census_block_group': str}) for file in ocd_files]
    cen_df = reduce(lambda  left,right: pd.merge(left,right,on='census_block_group'), census_df_raw)
    return(cen_df, cbg_field_desc) 

def normalize_demos_to_fractions(cen_df, demos_to_analyze, verbose=False):
    for this_demo_cat in demos_to_analyze:
        demo_codes = get_final_table_ids(this_demo_cat)
        demo_totals = cen_df[demo_codes].sum(axis=1)
        for this_code in demo_codes:
            cen_df[this_code+"_frac"] = cen_df[this_code] / demo_totals
    if(verbose): print("Added normalized columns as fractions.\n{0}".format(cen_df.shape))
    return(cen_df)

# ~~~~~~~~~~~~~~ Wrangle Patterns Data Functions~~~~~~~~~~
import json

def get_home_panel(patterns_dir=None,  drive=None):
    if(patterns_dir):
        home_panel = pd.read_csv(os.path.join(patterns_dir, "home_panel_summary.csv"), dtype = {'census_block_group': str}).drop(['year', 'month','state'],axis='columns')
    elif(drive):
        home_panel = pd_read_csv_drive(get_drive_id('home_panel_summary.csv'), drive, dtype = {'census_block_group': str}).drop(['year', 'month','state'],axis='columns')
    home_panel = home_panel.groupby(['census_block_group']).sum().reset_index() # CLEAN -- there are some CBGs with records split across states, erroneously. 
    return(home_panel)

def read_patterns_data(patterns_path, drive=None):
    if(patterns_path):
        all_patterns_files = [os.path.join(patterns_path,filepath) for filepath in os.listdir(patterns_path) if 'patterns' in filepath.lower()]
        patterns_raw = pd.concat((pd.read_csv(f) for f in all_patterns_files))
    elif(drive):
        all_patterns_files = [key for key in get_drive_id(None).keys() if 'patterns' in key]
        patterns_raw = pd.concat((pd_read_csv_drive(get_drive_id(f), drive) for f in all_patterns_files))
    return(patterns_raw)

def filter_patterns_dat(patt_df, brands_whitelist, sgpid_whitelist, verbose=False):
    df = patt_df.copy()
    if(verbose): print("Unfiltered patterns:\n{0}".format(df.shape))

    # Filter to brands and/or safegraph_place_ids
    if(brands_whitelist):
        df = df[df.brands.isin(brands_whitelist)]
        if(verbose): 
            print("Filtering to these brands: \n{0}".format(brands_whitelist))
            print("Filtered to select brands:\n{0}".format(df.shape))
    if(sgpid_whitelist):
        df = df[df.safegraph_place_id.isin(sgpid_whitelist)]
        if(verbose): 
            print("Filtering to these sgpids: \n{0}".format(sgpid_whitelist))
            print("Filtered to select sgpids:\n{0}".format(df.shape))
    return(df)

def vertically_explode_json(df, json_column='visitor_home_cbgs', index_column='safegraph_place_id', key_col_name='visitor_home_cbg', value_col_name='visitor_count'):
    # This function vertically explodes a JSON column in SafeGraph Patterns
    # The resulting dataframe has one row for every data element in the all the JSON of all the original rows

    # Inputs
    #    df -- a pandas.DataFrame(dataframe with at 2 columns: 
    #    1) index_column (default = safegraph_place_id), this is what the exploded json data will be joined to in the final result
    #    2) json_column -- each element of this column is a stringified json blog
    #    key_col_name -- arbitrary string, the name of the column in the output which contains the keys of the key:values of the JSON
    #    value_col_name -- arbitrary string, the name of the column in the output which contains the values of the key:values of the JSON
    # Outputs
    #    df -- a pandas.DataFrame with 3 columns
    #    1) index_column
    #    2) key_col_name
    #    3) value_col_name
    
    df = df.dropna(subset = [json_column]).copy() # Drop nan jsons 
    df[json_column+'_dict'] = [json.loads(cbg_json) for cbg_json in df[json_column]]

    # extract each key:value inside each visitor_home_cbg dict (2 nested loops) 
    all_sgpid_cbg_data = [] # each cbg data point will be one element in this list
    for index, row in df.iterrows():
        this_sgpid_cbg_data = [ {index_column : row[index_column], key_col_name:key, value_col_name:value} for key,value in row[json_column+'_dict'].items() ]
  
        # concat the lists
        all_sgpid_cbg_data = all_sgpid_cbg_data + this_sgpid_cbg_data

    return(pd.DataFrame(all_sgpid_cbg_data))

def extract_visitor_home_cbgs(patt_df, columns_from_patterns=['safegraph_place_id','brands'], verbose=False):
    if(verbose): print("Exploding visitor_home_cbgs for {0} records (This is a slow step).".format(patt_df.shape[0]))
    home_visitor_cbgs = vertically_explode_json(patt_df) # this is slow step
    visitors_df = pd.merge(home_visitor_cbgs, patt_df[columns_from_patterns])
    if(verbose): print("Size after exploding visitor_home_cbgs: \n{0}".format(visitors_df.shape))
    return(visitors_df)


# ~~~~~~~~~~~~~~ Data Wrangling Functions~~~~~~~~~~
def join_visitors_census_and_panel(visitors_df_, home_panel_, cen_df_, verbose=False):
    visitors_j1 = pd.merge(visitors_df_, home_panel_, left_on='visitor_home_cbg', right_on='census_block_group', suffixes=("_left", "")).drop('visitor_home_cbg',axis='columns')
    visitors_j2 = pd.merge(visitors_j1, cen_df_, on='census_block_group')
    if(verbose): print("Shape of fully-joined dataframe: \n{0}".format(visitors_j2.shape))
    return(visitors_j2)


def allocate_visits_by_demos(df_, demos_list, sample_col='visitor_count', verbose=False):
    df = df_.copy()
    # For every demo_group (e.g. "Race"), allocate counts according to the demo fraction 
    for demo_group in demos_list:
        demo_codes = get_final_table_ids(demo_group)
        for dc in demo_codes:
            df[sample_col+'_'+dc+'_D_adj'] = df[dc+'_frac'] * df[sample_col] # We will use this to agg within demos across CBGs
            df[sample_col+'_'+dc+'_POP_D_adj'] = df[dc+'_frac'] * df[sample_col+'_cbg_adj'] # We will use this to re-weight at end
        if(verbose): print("Allocated counts for {0}. df shape: {1}".format(demo_group,df.shape))
    return(df)

def sum_across_cbgs(df, group_key='brands', verbose=False):
    cols_to_keep = [group_key,'visitor_count', 'number_devices_residing', 'B01001e1'] + [col for col in df.columns if  '_adj' == col[-4::]]
    df = df[cols_to_keep].copy() # drop original columns
    df['cbg_count'] = [1]*df.shape[0] # we use this to keep track of how many CBG were measured for each brand
    summs = df.groupby([group_key]).sum().reset_index()
    if(verbose): print("summed data. df shape: {0}".format(summs.shape))
    return(summs)

def wrangle_summs_into_long_format(summs_, demos_list, group_key='brands', verbose=False):
    # This is a data-wrangling function to re-format the data. 
    # summs_ has one row for each brand and many columns (one for each demo ) aka wide format.
    # To make analysis and vis easier, we want one row for each brand-demo-group pair, and one column for the visitor counts measurement. 
    # To do this, for each brand, we transpose the row (.T) into transposed, then extract useful values from the original column headers as new columns in transposed
    # The D_adj and the POP_D_adj are distinct measurements, so we pivot the data again for the final desired format
    # The final dataframe has 5 columns, and 1 row for every brand-demo pairing
    
    all_demo_codes = flatten_list([get_final_table_ids(demo) for demo in demos_list])

    data_summary_list = []
    for this_group in summs_[group_key].tolist():
        row = summs_[summs_[group_key] == this_group].squeeze()
        row.drop(group_key, inplace=True)
        transposed = row.T.reset_index()
        transposed.columns = ['attribute','value']
        transposed['demo_code'] = transposed.attribute.str.split('_').str[2]
        transposed['measure'] = ['visitor_count_'+'_'.join(i) for i in transposed.attribute.str.split('_').str[3:]]
        transposed = transposed[~transposed.measure.isna() & transposed.demo_code.isin(all_demo_codes)]
        pvt = transposed.pivot(index='demo_code',columns='measure',values='value').reset_index()
        # Caution, something weird happens with the dtypes during this pivot, so be careful. see: https://stackoverflow.com/questions/46859400/pandas-pivot-changes-dtype
        pvt.columns.name = None
        pvt[group_key] = this_group
        data_summary_list.append(pvt)

    demo_summary = pd.concat(data_summary_list)
    demo_summary = pd.merge(demo_summary, summs_[[group_key,'cbg_count']])
    if(verbose): print("wrangled data: df shape: {0}".format(demo_summary.shape))
    return(demo_summary)

def allocate_sum_wrangle_demos(df_, demos_list, group_key='brands', verbose=False):
    demos_df = allocate_visits_by_demos(df_, demos_list, verbose=verbose)
    summs_df = sum_across_cbgs(demos_df, group_key=group_key, verbose=verbose)
    demo_summary_df = wrangle_summs_into_long_format(summs_df, demos_list, group_key=group_key, verbose=verbose)
    return(demo_summary_df)

def get_totals_for_each_brand_and_demo(df, cbg_field_desc_, sample_col='visitor_count', group_key='brands'):
    meas_vars = [col for col in df.columns if sample_col in col]
    df = pd.merge(df, cbg_field_desc_[['table_id','field_level_1']], left_on='demo_code' , right_on='table_id').drop('table_id',axis=1).rename(columns={'field_level_1':'demo_category'})
    total_visitors_est = df.groupby(['demo_category', group_key]).sum(numeric_only=None).reset_index()[['demo_category', group_key] + meas_vars]
    df = pd.merge(df, total_visitors_est, on = ['demo_category',group_key], suffixes=('','_total'))
    return(df)

def convert_cols_to_frac_of_total(df, columns_to_adjust, column_with_total, col_suffix='_rate'):
    for col in columns_to_adjust:
        df[col+col_suffix] = (df[col] / df[column_with_total]).astype('float')
    return(df)


# ~~~~~~~~~~~~~~ Visualization Functions~~~~~~~~~~
import matplotlib
import matplotlib.pyplot as plt
import bokeh
import random
from bokeh.palettes import brewer

def build_label_color_map(label_set,colorset='Set1', rand_draw=False):
    palette = brewer[colorset][max(3,min(8, len(label_set)))] # Paired, Set3, Set2, Set1
    offset = 0
    if(rand_draw): 
        size_of_pallette = 8 # https://bokeh.pydata.org/en/latest/docs/reference/palettes.html
        offset = np.random.choice(range(8))
        palette = brewer[colorset][8] # Paired, Set3, Set2, Set1
    label_color_map = pd.DataFrame({'label':label_set, 'color':[palette[(idx+offset)%len(palette)] for idx, label in enumerate(label_set)]})
    return(label_color_map)

def format_group_labels(group_labels, space_split=3):
    # format group_labels
    group_labels = group_labels.str.replace(pat='$',repl='\$', regex=False)
    group_labels = [gl.split(" Alone")[0] for gl in group_labels]
    return(['\n' + ' \n '.join( [' '.join(gl.split(' ')[0:space_split]), ' '.join(gl.split(' ')[space_split:])]) for gl in group_labels])

def get_income_col_order():
    a,b = get_household_income_groups()
    return(pd.DataFrame({'demo_code' : [b[this] for this in  list(a.keys())], 'col_order' : list(range(len(list(a.keys()))))}))

def get_edu_col_order():
    a,b = get_edu_attainment_groups()
    return(pd.DataFrame({'demo_code' : [b[this] for this in  list(a.keys())], 'col_order' : list(range(len(list(a.keys()))))}))

def get_age_col_order():
    a,b = get_age_by_sex_groups()
    return(pd.DataFrame({'demo_code' : [b[this] for this in  list(a.keys())], 'col_order' : list(range(len(list(a.keys()))))}))

def get_race_col_order():
    col_order = get_final_table_ids('Race')
    return(pd.DataFrame({'demo_code' : col_order, 'col_order' : list(range(len(col_order)))}))

def get_hispanic_col_order():
    col_order = get_final_table_ids('Hispanic Or Latino Origin')
    return(pd.DataFrame({'demo_code' : col_order, 'col_order' : list(range(len(col_order)))}))
                   
def get_col_orders():
    return(pd.concat([get_income_col_order(), 
                      get_edu_col_order(),
                      get_age_col_order(),
                      get_race_col_order(),
                     get_hispanic_col_order()]))


    
def make_demographics_chart(res2plot, 
                    chart_type='bar', 
                    group_key='brands',
                    column_to_plot='visitor_count_D_adj_rw_rate', 
                    error_column='conf_interval_rw_rate', 
                    bar_groups='field_level_2', 
                    show_error=False,
                    group_label='field_level_2',
                    fig_size=[7,7]):
    
    # For easier exploratory data analysis
    # chart_type must be 'line', 'bar', or 'stacked_bar'
    
    # prep to plot 
    plt.rcParams['figure.figsize'] = fig_size
    res2plot = pd.merge(res2plot, get_col_orders(), how = 'left').sort_values(by=[group_key,'field_level_1','col_order'], ascending=True)
    brands2plot = pd.Series(res2plot[group_key].unique()).sort_values()
    if(show_error):
        err_linewidth, capsize = (2,5)
    else:
        err_linewidth, capsize = (0,0)
        errors = None   
    color_map = build_label_color_map(brands2plot, colorset='Set1', rand_draw=False)
    
    # plot the data according to chart_type
    if(chart_type=='bar'):
        barWidth = 0.7 / len(brands2plot)
        for idx, this_brand in brands2plot.iteritems():
            bars_data=res2plot[res2plot[group_key]==this_brand][column_to_plot]*100
            x_pos = [x + (idx+1)*barWidth for x in np.arange(len(bars_data))]
            if(show_error): errors=res2plot[res2plot[group_key]==this_brand][error_column]*100
            eb1 = plt.bar(x_pos, bars_data, yerr=errors, color=color_map[color_map.label==this_brand].color.iloc[0], width=barWidth, edgecolor='k', label=this_brand, capsize=capsize)
    
    elif(chart_type=='line'):
        barWidth=0
        for idx, this_brand in brands2plot.iteritems():
            bars_data=res2plot[res2plot[group_key]==this_brand][column_to_plot]*100
            x_pos = np.arange(len(bars_data))
            if(show_error): errors=res2plot[res2plot[group_key]==this_brand][error_column]*100
            plot_obj = plt.errorbar(x_pos, bars_data, yerr=errors, color=color_map[color_map.label==this_brand].color.iloc[0], label=this_brand, linewidth=5, marker = 'o', markeredgecolor='k', markersize=12, alpha=0.8, capsize=capsize)
            
            if(show_error): # Style error bars
                plot_obj[-1][0].set_color('k') # https://stackoverflow.com/a/23002214/2098573
                plot_obj[-1][0].set_linewidth(err_linewidth)
                [cap.set_color('k') for cap in plot_obj[1]]
    
    elif(chart_type=='stacked_bar'):
        barWidth = 0.6
        last_bars = np.array([0]*len(brands2plot))
        demos2plot = res2plot[res2plot[group_key]==brands2plot[0]][bar_groups]
        color_map = build_label_color_map(demos2plot, rand_draw=False)
        fig, ax = plt.subplots(1, 1)
        ax.yaxis.grid(zorder=0)
        for idx, this_demo in demos2plot.iteritems():
            bars_data=res2plot[res2plot[bar_groups]==this_demo][column_to_plot]*100
            x_pos = [x for x in np.arange(len(bars_data))]
            if(show_error): 
                errors=res2plot[res2plot[bar_groups]==this_demo][error_column]*100
            plt.bar(x_pos, bars_data, yerr=errors, color=color_map[color_map.label==this_demo].color.iloc[0], width=barWidth, edgecolor='black', label=this_demo, bottom=last_bars, capsize=capsize)
            last_bars = last_bars + bars_data.values
            
    else:
        print("No recognized chart_type")
        return(None)
    
    # label/style the chart and legend
    if(chart_type in ['bar', 'line']):
        xlabel='Demo Group'
        group_labels = format_group_labels(res2plot[res2plot[group_key] == brands2plot[0]][group_label])
        plt.xticks([r + barWidth for r in range(len(bars_data))], group_labels, rotation=90, size=16)
        plt.legend(fontsize=18)
    elif(chart_type=='stacked_bar'):
        xlabel = 'Brand'
        group_labels = format_group_labels(brands2plot,space_split=1) 
        plt.xticks([r for r in range(len(brands2plot))], group_labels, rotation=90, size=16)
        # Create legend & Show graphic
        handles, labels = plt.gca().get_legend_handles_labels()
        order = range(len(demos2plot))[::-1] # We want to reverse the legend order so it matches the order on the chart
        plt.legend([handles[idx] for idx in order],[labels[idx] for idx in order],fontsize=18, bbox_to_anchor=(1,0.5))
       
    plt.xlabel(xlabel, fontweight='bold')
    plt.ylabel("% of Total Visitors", size=20)
    plt.ylim((0,100))   
    plt.show()
    return(None)
    



# ~~~~~~~~~~~~~~ Stats Functions~~~~~~~~~~

import scipy
from scipy import stats

def apply_normal_lower_ci(x, mean_col, std_col, ci=0.95):
    return(scipy.stats.norm.interval(ci, loc=x[mean_col], scale=x[std_col])[0])

def apply_normal_upper_ci(x, mean_col, std_col, ci=0.95):
    return(scipy.stats.norm.interval(ci, loc=x[mean_col], scale=x[std_col])[1])

def get_conf_interval_of_mean_estimate(df, sample_col='visitor_count_D_adj', conf_interval=0.95):
    df['poisson_as_normal_aprx_std'] = np.sqrt(df[sample_col].tolist())
    df['conf_interval'] = df[sample_col] - df.apply(apply_normal_lower_ci, axis=1, mean_col=sample_col, std_col='poisson_as_normal_aprx_std', ci=conf_interval)
    return(df)

def compute_adjust_factor(df, population_col, sample_col):
    adjust_factor = df[population_col] / df[population_col].sum() * df[sample_col].sum() / df[sample_col].clip(lower=0.00001)
    return(adjust_factor)

def apply_strata_reweighting(df,
                             cols_to_adjust = ['visitor_count_D_adj', 'conf_interval'],
                             raw_column='visitor_count_D_adj',
                             adjusted_column='visitor_count_POP_D_adj'):
    
        
        df['demo_adjust_factor'] = df[adjusted_column] / df[raw_column].clip(lower=0.00001)
        for col in cols_to_adjust:
            df[col+'_rw'] = df[col] * df['demo_adjust_factor']
        return(df)


# ~~~~~~~~~~~~~~ Wrapper Functions~~~~~~~~~~

def get_patterns_master(patterns_dir, drive=None, brands=None, sgpids=None, verbose=False):
    if((not brands) & (not sgpids)):
        print("Error: Must give either a brand_list or sgpid_whitelist in get_patterns()")
        return(None)
    patterns_raw = read_patterns_data(patterns_dir, drive=drive)
    patterns_filtered = filter_patterns_dat(patterns_raw, brands, sgpids, verbose=verbose)
    visitors_df = extract_visitor_home_cbgs(patterns_filtered, verbose=verbose) #   this is a slow step
    return(visitors_df)

def get_census_master(demos_to_analyze, open_census_dir=None, drive=None, verbose=False):
    # Read the appropriate census files, given the categories requested, perform aggregations
    census_df, cbg_field_desc = get_raw_census_data(demos_to_analyze, open_census_dir, drive=drive, verbose=verbose)
    census_df, cbg_field_desc_mod = reaggregate_census_data(census_df, cbg_field_desc, demos_to_analyze, verbose=verbose)
    # add new columns which re-normalize counts to fraction-within-demo-category for each CBG
    census_df = normalize_demos_to_fractions(census_df, demos_to_analyze, verbose=verbose)
    return(census_df, cbg_field_desc_mod)

def combine_and_analyze(visitors_df, 
                        home_panel, 
                        census_df, 
                        cbg_field_desc_mod,
                        demos_list,
                        pop_col = 'B01001e1',
                        sample_size_col = 'number_devices_residing',
                        sample_col = 'visitor_count',
                        group_key = 'brands',
                        conf_interval=0.95,
                        verbose=False):

    # join datasets together
    visitors_join = join_visitors_census_and_panel(visitors_df, home_panel, census_df, verbose=verbose)
    # log post-hoc stratification re-weighting at CBG level to use later
    visitors_join['cbg_adjust_factor'] = compute_adjust_factor(visitors_join, pop_col, sample_size_col)
    visitors_join[sample_col+'_cbg_adj'] = visitors_join[sample_col] * visitors_join['cbg_adjust_factor']
    # analyze
    demo_summary = allocate_sum_wrangle_demos(visitors_join, demos_list, group_key=group_key, verbose=verbose)
    demo_stats = get_conf_interval_of_mean_estimate(demo_summary, sample_col=sample_col+'_D_adj', conf_interval=conf_interval)
    demos_reweighted = apply_strata_reweighting(demo_stats, cols_to_adjust=[sample_col+'_D_adj', 'conf_interval'], raw_column=sample_col+'_D_adj', adjusted_column=sample_col+'_POP_D_adj') # at demo level
    final_results = get_totals_for_each_brand_and_demo(demos_reweighted, cbg_field_desc_mod, sample_col=sample_col+'_D_adj_rw', group_key=group_key)
    final_results = convert_cols_to_frac_of_total(final_results, [sample_col+'_D_adj_rw', 'conf_interval_rw'], sample_col+'_D_adj_rw_total')
    final_results = pd.merge(final_results, cbg_field_desc_mod, left_on = 'demo_code', right_on='table_id').dropna(axis=1,how='all')
    
    return(visitors_join, final_results)

def master_demo_analysis(open_census_data_dir,
                         patterns_dir, 
                         drive,
                         demos_to_analyze, 
                         brands_list, 
                         sgpid_whitelist, 
                         group_key = 'brands',
                         verbose=False):
    
    visitors_df = get_patterns_master(patterns_dir=patterns_dir, 
                               drive=drive,
                               brands=brands_list, 
                               sgpids=sgpid_whitelist,
                               verbose=verbose)
    if(verbose): print("completed visitors_df {}".format(visitors_df.shape))
    
    home_panel = get_home_panel(patterns_dir=patterns_dir, drive=drive)
    if(verbose): print("completed homePanel {}".format(home_panel.shape))


    census_df, cbg_field_desc_mod = get_census_master(demos_to_analyze,
                                                      open_census_dir=open_census_data_dir,
                                                      drive=drive,
                                                      verbose=verbose)
    if(verbose): print("completed census {}".format(census_df.shape))

    visitors_join, final_results = combine_and_analyze(visitors_df, 
                                                       home_panel, 
                                                       census_df,
                                                       cbg_field_desc_mod,
                                                       demos_to_analyze,
                                                       group_key = group_key,
                                                       verbose=verbose)
    return(visitors_join, final_results)
    
    
