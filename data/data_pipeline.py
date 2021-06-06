# import packages
import pandas as pd
from sqlalchemy import create_engine
import sys


def load_data(data_file):
    # read in file

    # load messages dataset
    messages_file_path_and_name = "messages.csv"
    messages = pd.read_csv(messages_file_path_and_name)
    messages.set_index('id')

    # load categories dataset
    categories_file_path_and_name = "categories.csv"
    categories = pd.read_csv(categories_file_path_and_name)
    categories.set_index('id')
    categories.head()


    # merge datasets
    df = messages.merge(categories,left_index=True, right_index=True)
    df.drop("id_y", axis=1)
    df.rename(columns={"id_x": "id"})
    df.head()

    # create a dataframe of the 36 individual category columns

    # get the firts cell to extract the columns labels
    col_title_to_split = categories[categories["id"] == 2]["categories"]

    # split the first cell value by ";" and get a list
    col_title  = col_title_to_split.str.split(pat=";",expand=False).tolist()

    # Go through the whole df cell from the `certogories` columns to generate the newers columns
    categories[col_title[0]] = categories.categories.str.split(";",expand=True)

    categories.head()


    # select the first row of the categories dataframe
    col_title_to_split = categories[categories["id"] == 2]["categories"]


    # use this row to extract a list of new column names for categories.
    # one way is to apply a lambda function that takes everything 
    # up to the second to last character of each string with slicing

    # split the first cell value by ";" and get a list
    col_title  = col_title_to_split.str.split(pat=";",expand=False).tolist()

    # Go through the whole df cell from the `certogories` columns to generate the newers columns
    categories[col_title[0]] = categories.categories.str.split(";",expand=True)
    categories.head()

    for column in categories:
    
    if column != "id" and column != "categories":
        # set each value to be the last character of the string
        categories[column] = categories[column].str[-1]

        # convert column from string to numeric
        categories[column] = pd.to_numeric(categories[column])
    
    categories.head()

    # drop the original categories column from `df`

    df = df.drop(['categories', 'id_y'],axis=1)
    df.head()

    # concatenate the original dataframe with the new `categories` dataframe
    df = df.merge(categories,left_index=True, right_index=True)
    df.head()

    # check number of duplicates
    sum(df.duplicated())

    # drop duplicates
    df = df.drop_duplicates()

    # check number of duplicates
    sum(df.duplicated())
    


    # clean data

    # load to database
    engine = create_engine('sqlite:///InsertDatabaseName.db')
    df.to_sql('InsertTableName', engine, index=False)


    # define features and label arrays


    return X, y


def build_model():
    # text processing and model pipeline


    # define parameters for GridSearchCV


    # create gridsearch object and return as final model pipeline


    return model_pipeline


def train(X, y, model):
    # train test split


    # fit model


    # output model test results


    return model


def export_model(model):
    # Export model as a pickle file



def run_pipeline(data_file):
    X, y = load_data(data_file)  # run ETL pipeline
    model = build_model()  # build model pipeline
    model = train(X, y, model)  # train model pipeline
    export_model(model)  # save model


if __name__ == '__main__':
    data_file = sys.argv[1]  # get filename of dataset
    run_pipeline(data_file)  # run data pipeline