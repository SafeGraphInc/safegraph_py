
### -------------------------------------Test and Help functions -------------------------------------------------------

def Test_me():
    print("hello world")


def help():
    print('''
  HELP:

  Welcome to the safegraph helper function. Below you will find a list of functions and their arguments to aid in your datascience journey. If you have further questions that cannot
  be answered by this help command, please do not hesitate to ask for assistance in the #python_troubleshooting slack channel. 

        Key:
            *    -    Required Argument
            &    -    Boolean value
            
            

  Available Functions: 

    + Test_me() - A function to test the Python Libray
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


  ''')

### -------------------------------------- JSON Functions ---------------------------------------------------------------

def vertically_explode_json(df_, json_column='visitor_home_cbgs', key_col_name='visitor_home_cbg',
                            value_col_name='cbg_visitor_count'):
    # This function vertically explodes a JSON column in SafeGraph Patterns
    # The resulting dataframe has one row for every data element in all the JSON of all the original rows
    # This is a slow step. If you are working with more than 20,000 rows you should explore faster implementations like pyspark, see here: https://docs.safegraph.com/docs/faqs#section-how-do-i-work-with-the-patterns-columns-that-contain-json

    # Inputs
    #    df -- a pandas.DataFrame -- dataframe with a unique df.index for every row
    #    json_column -- each element of this column is a stringified json blog. No elements can be NULL. # TODO: convert NA JSON columns to empty `{}` so function can handle them and just pass those rows through
    #    key_col_name -- arbitrary string, the name of the column in the output which contains the keys of the key:values of the JSON
    #    value_col_name -- arbitrary string, the name of the column in the output which contains the values of the key:values of the JSON
    # Outputs
    #    df -- a pandas.DataFrame with 2 new columns
    #    1) key_col_name
    #    2) value_col_name

    df = df_.copy()
    if (df.index.unique().shape[0] < df.shape[0]):
        raise ("ERROR -- non-unique index found")
    df[json_column + '_dict'] = [json.loads(cbg_json) for cbg_json in df[json_column]]
    all_sgpid_cbg_data = []  # each cbg data point will be one element in this list
    for index, row in df.iterrows():
        # extract each key:value inside each visitor_home_cbg dict (2 nested loops)
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
