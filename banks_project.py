import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import requests
import numpy as np
import sqlite3

url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribute = ['Name','MC_USD_Billion']
ftable_attribute =['Name','MC_USD_Billion','MC_GBP_Billion','MC_EUR_Billion','MC_INR_Billion']
target_file = './Largest_bank_data.csv'
db_name = 'Banks.db'
table_name='Largest_banks'
log_file = 'code_log.txt'

rate_file = pd.read_csv('./exchange_rate.csv')

exchange_rate = rate_file.set_index('Currency').to_dict()['Rate']



# Code for ETL operations on Country-GDP data

# Importing the required libraries

def log_progress(message):
    timestamp_format = '%Y-%y-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file,'a') as f:
        f.write(timestamp + ',' + message + '\n')

def extract(url, table_attribs):
    page = requests.get(url).text
    html = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = html.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            data_dic = {'Name':str(col[1].find_all('a')[1].text),
                       'MC_USD_Billion':float(col[2].contents[0].strip().replace('\n', '')) }
            df1 = pd.DataFrame(data_dic, index=[0])
            df = pd.concat([df,df1],ignore_index=True)    
    
    return df

def transform(df, csv_path):
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]

    return df

def load_to_csv(df, output_path):
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name,sql_connection,if_exists='replace',index=False)


def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement,sql_connection)
    print(query_output)

# Log the initialization of the ETL process 
log_progress("ETL Job Started") 
 
# Log the beginning of the Extraction process 
log_progress("Extract phase Started") 
extracted_data = extract(url,table_attribute) 
 
# Log the completion of the Extraction process 
log_progress("Extract phase Ended") 

# # Log the beginning of the Transformation process 
log_progress("Transform phase Started") 
transformed_data = transform(extracted_data,target_file) 
print("Transformed Data") 
print(transformed_data) 

# Log the completion of the Transformation process 
log_progress("Transform phase Ended") 


# Log the beginning of the Loading process 
log_progress("Load phase Started") 
load_to_csv(transformed_data,target_file)

# Log the completion of the Loading process 
log_progress("Load phase Ended") 

conn = sqlite3.connect(db_name)

 
# Log the beginning of the Loading process 
log_progress("Load to db phase Started") 
load_to_db(transformed_data,conn,table_name) 
 
# Log the completion of the Loading process 
log_progress("Load to db phase Ended") 



run_query(f"SELECT * FROM Largest_banks",conn)
run_query(f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks",conn)
run_query(f"SELECT Name from Largest_banks LIMIT 5",conn)
# Log the completion of the ETL pr