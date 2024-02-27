#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
import db_dtypes
from openai import OpenAI
from google.cloud import bigquery
from google.oauth2 import service_account

pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', None)  # Show all rows
pd.set_option('display.max_colwidth', None)  # Show full content of each column


#Connect to OpenAI API
openai_client = OpenAI(
     api_key=os.environ.get("OPENAI_API_KEY"),
)

#Connect to BigQuery API
credentials = service_account.Credentials.from_service_account_file('/Users/ryancabrera/Documents/bq_playground/lively-wonder-406517-3a13b96c8b4e.json')
project_id = 'lively-wonder-406517'
bq_client = bigquery.Client(credentials= credentials,project=project_id)

# Ask user which BigQuery DataSet they are interested in
bq_dataset = input('What is the name of the dataset you are interested in? ')


# Construct tables_query off of that input
tables_query = "select ddl from  `bigquery-public-data." + bq_dataset + "." + "INFORMATION_SCHEMA.TABLES`"


# Run metadata query and output results to a dataframe
tables_query_job = bq_client.query(tables_query)
df_tables = tables_query_job.to_dataframe()


# Create single concatenated string with metadata
ddl_string =  "Database Schema Definition:\n" + "\n".join(df_tables['ddl'].tolist())

# Define function to ask yes/no questions
def get_user_query(prompt):
    while True:
        # Ask the user for input
        user_input = input(prompt).strip().lower()  # Normalize the input to lowercase and remove leading/trailing spaces
        if user_input in ['yes', 'no']:
            return user_input
        else:
            print("Please enter 'Yes' or 'No'.")


# Main loop for asking and running queries
while True:
    # Ask user what query they want to generate
    user_question = input('What question would you like to generate a SQL query for? ')

    # Feed user's question to ChatGPT, append this to the original message
    follow_up_response = openai_client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {
                "role": "system",
                "content": "You will be asked for help writing SQL queries against public datasets in GCP BigQuery. "
                            "You will be provided with the DDL for multiple tables to help generate queries. "
                            "Do not generate any queries until asked."
                            " When asked to generate a query, at the top of each query add --QUERY_START, and at the bottom of the query, add --QUERY_END."
            },
            {"role": "user", "content": "Here is the DDL: " + ddl_string},
            {"role": "user", "content": user_question},
        ]
    )

    # Show user the SQL query ChatGPT has generated
    query_sql = str(follow_up_response.choices[0].message.content)
    print(query_sql)

    # Ask user if they want to run the SQL query
    ask_user_run_query = get_user_query("Would you like to run this query? (Yes/No) ")

    if ask_user_run_query == 'yes':
        print("Running Query...")
        user_query_job = bq_client.query(query_sql)
        user_query_df = user_query_job.to_dataframe()
        print("Here are the results:")
        print(user_query_df)
    else:
        print("Ok, I will not run the query.")

    # Ask if the user wants to run another query off the same dataset
    run_another_query = get_user_query("Do you want to run another query off the same dataset? (Yes/No) ")
    if run_another_query == 'no':
        print("Ending the application.")
        break




