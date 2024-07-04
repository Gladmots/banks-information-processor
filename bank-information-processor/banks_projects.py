import requests
import pandas as pd
import sqlite3
from bs4 import BeautifulSoup
import logging
import os
import csv

# Configure logging
logging.basicConfig(filename='code_log.txt', level=logging.INFO, format='%(asctime)s : %(message)s')

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the code execution to a log file. Function returns nothing'''
    logging.info(message)
    print(message)

def extract(url, table_attribs):
    log_progress('Extracting data from URL...')
    
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    tables = soup.find_all('table', {'class': 'wikitable'})
    for i, table in enumerate(tables):
        print(f"Table {i}:\n{pd.read_html(str(table))[0].head()}\n")
    
    df_list = pd.read_html(str(tables[2]))  # Adjust this index to extract the correct table
    df = df_list[0]
    
    log_progress('Data extraction complete. Initiating Transformation process')
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    log_progress('Transforming data...')
    
    exchange_rates = pd.read_csv(csv_path)
    rates = exchange_rates.set_index('Currency').to_dict()['Rate']

    df['MC_GBP_Billion'] = df['Market cap(US$ billion)'] * rates['GBP']
    df['MC_EUR_Billion'] = df['Market cap(US$ billion)'] * rates['EUR']
    df['MC_INR_Billion'] = df['Market cap(US$ billion)'] * rates['INR']
    
    log_progress('Data transformation complete. Initiating Loading process')
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    log_progress('Saving data to CSV...')
    df.to_csv(output_path, index=False)
    log_progress('Data saved to CSV file')

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    log_progress('Loading data to database...')
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    log_progress('Data loaded to Database as a table, Executing queries')

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    log_progress('Running query on database...')
    result = pd.read_sql_query(query_statement, sql_connection)
    print(result)
    log_progress('Process Complete')

def load_bank_data(csv_file):
    ''' This function loads bank data from a CSV file and returns it as a list of lists '''
    bank_data = []
    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header row
        for row in reader:
            bank_data.append(row)
    return bank_data

# Main execution
if __name__ == "__main__":
    # Define parameters
    csv_file = 'banks_data.csv'
    data_url = 'https://en.wikipedia.org/wiki/List_of_largest_banks'
    exchange_rate_csv_path = './exchange_rate.csv'
    output_csv_path = './banks_data.csv'
    db_name = 'Banks.db'
    table_name = 'Largest_banks'
    
    log_progress('Preliminaries complete. Initiating ETL process')
    
    # Extract data
    df = extract(data_url, table_name)
    
    # Transform data
    df = transform(df, exchange_rate_csv_path)
    
    # Load data to CSV
    load_to_csv(df, output_csv_path)
    
    # Initiate SQLite3 connection
    log_progress('SQL Connection initiated')
    sql_connection = sqlite3.connect(db_name)
    
    # Load data to database
    load_to_db(df, sql_connection, table_name)
    
    # Run a query to verify data
    query_statement = f'SELECT * FROM {table_name}'
    run_query(query_statement, sql_connection)
    
    # Close SQLite3 connection
    sql_connection.close()
    log_progress('Server Connection closed')
    
    # Load and print bank data from CSV
    bank_data = load_bank_data(csv_file)
    print(bank_data)
