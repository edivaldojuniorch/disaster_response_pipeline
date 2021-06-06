import sys
import pandas as pd
from sqlalchemy import create_engine


def load_data(messages_filepath, categories_filepath):
    """
    Responsible for load the data inside both files referenced as arg1 and arg2.

    INPUT:
        messages_filepath: full path from CSV's messages file
        categories_filepath: full path from CSV'sthe categories file

    OUTPUT:
        df: a data frame with the loaded files together

    """
    # load messages dataset
    messages_file_path_and_name = messages_filepath
    messages = pd.read_csv(messages_file_path_and_name)

    # load categories dataset
    categories_file_path_and_name = categories_filepath
    categories = pd.read_csv(categories_file_path_and_name)
    categories.set_index('id')


    # merge datasets
    df = messages.merge(categories,left_index=True, right_index=True)
    df.drop("id_y", axis=1)
    df.rename(columns={"id_x": "id"})

    # create a dataframe of the 36 individual category columns
    # get the firts cell to extract the columns labels
    col_title_to_split = categories[categories["id"] == 2]["categories"]

    # split the first cell value by ";" and get a list
    col_title  = col_title_to_split.str.split(pat=";",expand=False).tolist()

    # Go through the whole df cell from the `certogories` columns to generate the newers columns
    categories[col_title[0]] = categories.categories.str.split(";",expand=True)

    # select the first row of the categories dataframe
    col_title_to_split = categories[categories["id"] == 2]["categories"]


    # use this row to extract a list of new column names for categories.
    # one way is to apply a lambda function that takes everything 
    # up to the second to last character of each string with slicing

    # split the first cell value by ";" and get a list
    col_title  = col_title_to_split.str.split(pat=";",expand=False).tolist()

    # Go through the whole df cell from the `certogories` columns to generate the newers columns
    categories[col_title[0]] = categories.categories.str.split(";",expand=True)

    for column in categories:

        # columns to avoid 
        if column != "id" and column != "categories":

            # set each value to be the last character of the string
            categories[column] = categories[column].str[-1]

            # convert column from string to numeric
            categories[column] = pd.to_numeric(categories[column])
    

    # drop the original categories column from `df`
    df = df.drop(['categories', 'id_y'],axis=1)

    # concatenate the original dataframe with the new `categories` dataframe
    df = df.merge(categories,left_index=True, right_index=True)

    return df


def clean_data(df):
    """
    Defined to remove the duplicated values from the DataFrame loded.

    INPUT:
        df:Raw Pandas DataFrame defined in load process

    OUTPUT:
        df:Cleaned Pandas DataFrame from duplicated value

    """
    
    # drop duplicates
    df = df.drop_duplicates()

    return df



def save_data(df, database_filename):
    """
    Save data to a SQLite data base addressed by filename

    INPUT:
        df: Pandas DataFrame ready to get its data loaded to DataBase
        database_filename: SQLiter filename to recieve the data loaded on df
    OUTPUT:
        none

    """
    # load to database
    engine = create_engine('sqlite:///' + database_filename)
    df.to_sql('InsertTableName', engine, index=False)

    pass  




def main():
    if len(sys.argv) == 4:

        messages_filepath, categories_filepath, database_filepath = sys.argv[1:]

        print('Loading data...\n    MESSAGES: {}\n    CATEGORIES: {}'
              .format(messages_filepath, categories_filepath))

        df = load_data(messages_filepath, categories_filepath)

        print('Cleaning data...')
        df = clean_data(df)
        
        print('Saving data...\n    DATABASE: {}'.format(database_filepath))
        save_data(df, database_filepath)
        
        print('Cleaned data saved to database!')
    
    else:
        print('Please provide the filepaths of the messages and categories '\
              'datasets as the first and second argument respectively, as '\
              'well as the filepath of the database to save the cleaned data '\
              'to as the third argument. \n\nExample: python process_data.py '\
              'disaster_messages.csv disaster_categories.csv '\
              'DisasterResponse.db')


if __name__ == '__main__':
    main()