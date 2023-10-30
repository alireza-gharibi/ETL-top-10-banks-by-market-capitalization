#!/usr/bin/env python
# coding: utf-8


import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
import requests as r
from datetime import datetime
import numpy as np


# Connecting to the URL...
try :
    url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
    response = r.get(url) # put r.get(url) here(in first stages) to know if there is a network error and 
    # (cotinued) prevent the function from furthur running
    soup = BeautifulSoup(response.content, 'lxml')
except r.exceptions.RequestException :
    print("The URL cant't be reached!")
    


def extract():
    '''
    extracting 'top banks by market capitalization' table from the URL
    '''
    name = []
    MC_USD_Billion = [] 
    
    rows_list = soup.find('table').find_all('tr') #rows of table
    for list_element in rows_list[1:]: #iterating over each row
        row = list_element.find_all('td')
        name.append(row[1].get_text(strip=True)) #extracting bank name column
        MC_USD_Billion.append(row[2].get_text(strip=True)) #extracting market capitalization column

    df = pd.DataFrame({"Name":name,"MC_USD_Billion":MC_USD_Billion}) #creating dataframe
    return df



#downloading Exchange rate
def exchange_rate():
    
    '''
    the function tries to download a csv file and save it as a dataframe and return the dataframe. 
    if it fails to downlaod the csv file from the website, it tries to use the existing csv file 
    which was downloaded previously. if there is no local csv file available either. it returns 'None'
    and prints a message that No local 'exchange rate data available!'
    '''
    
    try:
        exchange_file_url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
        exchange_file = r.get(exchange_file_url)
        with open('downloaded_exchange.csv',"wb") as d:
            d.write(exchange_file.content)
        # creating a dataframe from the csv file:
        with open('downloaded_exchange.csv','r') as d:
            df=pd.read_csv(d)
            return df
    except r.exceptions.RequestException : 
        # 'RequestException' includes almost all kinds of request exception
        try: 
            print("The exchange rate website can't be reached! \nTrying to use local exchange rate data...")
            with open('downloaded_exchange.csv','r') as d:
                df=pd.read_csv(d)
                return df
        except :
            print('No local exchange rate data available!')
            return None
    
    



def transform(df,e_rate):
    '''
    The function transforms the dataframe 'df' by adding columns for Market Capitalization 
    in GBP, EUR and INR, rounded to 2 decimal places, 
    based on the exchange rate information 'e_rate'.
    '''
    
    df['MC_USD_Billion'] = df['MC_USD_Billion'].astype(float) # casting 'MC_USD_Billion' column to float
    #extracting 'GBP' exchange rate from 'e_rate' dataframe and casting it to float:
    GBP=float(e_rate[e_rate['Currency']=='GBP']['Rate']) #using boolean indexing to extract 'GBP'
    df['MC_GBP_Billion'] = np.round(df['MC_USD_Billion'] * GBP,2)
    #extracting 'EUR' exchange rate from 'e_rate' dataframe and casting it to float:
    EUR=float(e_rate[e_rate['Currency']=='EUR']['Rate']) #using boolean indexing to extract 'EUR'
    df['MC_EUR_Billion'] = np.round(df['MC_USD_Billion'] * EUR,2)
    #extracting 'INR' exchange rate from 'e_rate' dataframe and casting it to float:
    INR = EUR=float(e_rate[e_rate['Currency']=='INR']['Rate']) #using boolean indexing to extract 'INR'
    df['MC_INR_Billion'] = np.round(df['MC_USD_Billion'] * INR,2)
    return df




def load_to_csv(df, csv_path):
    df.to_csv(csv_path)





def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)





def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)





def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./ETL World's Largest Banks log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')





''' Here, i define the required entities(variables) and call the relevant 
functions in the correct order to complete the project.'''

db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_output.csv'

log_progress('Preliminaries complete. Initiating ETL process')
df = extract()
log_progress('Data extraction complete')
log_progress('Fetching Exchange rate')
e_rate = exchange_rate()
log_progress('Exchange rate loaded. Initiating Transformation process')
df = transform(df,e_rate)
log_progress('Data transformation complete. Initiating loading process')
load_to_csv(df, csv_path)
log_progress('Data saved to CSV file')
sql_connection = sqlite3.connect(db_name) #creating database and connection
log_progress('SQL Connection initiated.')
load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table. Running the queries')
print("\033[1m" + 'First Query:\n' + "\033[0m") #making the text bold
query_statement = f"SELECT AVG(MC_GBP_Billion) 'average market capitalization' FROM {table_name}"
run_query(query_statement, sql_connection)
print("\033[1m" + '\nSecond Query:\n' + "\033[0m") #making the text bold
query_statement = f"SELECT * FROM {table_name}"
run_query(query_statement, sql_connection)
log_progress('Process Complete.')
sql_connection.close()





