def Test_me():
    print("hello world")


def help():
    print('''
  HELP:

  Welcome to the safegraph helper function. Below you will find a list of functions and their arguments to aid in your datascience journey. If you have further questions that cannot
  be answered by this help command, please do not hesitate to ask for assistance in the #python_troubleshooting slack channel. 

        Key:
            *        - Required Argument
            &       - Boolean value
            
            

  Available Functions: 

    + Test_me() - A function to test the Python Libray
    + explode_visitor_home_cbg() - a function to explode visitor home cbg column vertically 
        **Arguments: 
            df*
            json_column 
            key_column_name 
            value_column_name 
            keep_index (&)
    + explode_json_array() - explodes any generic json array
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

