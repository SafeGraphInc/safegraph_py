
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
