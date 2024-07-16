import pandas as pd
import requests as rq
import sqlite3
import numpy as np
import datetime
from bs4 import BeautifulSoup

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

def extract(url,table_attributes):
    html_page = rq.get(url).text
    data = BeautifulSoup(html_page,'html.parser')
    df = pd.DataFrame(columns=table_attributes)
    
    tables = data.find_all('tbody')
    
    rows = tables[0].find_all('tr')
    
    for row in rows:
        col = row.find_all('td')
        if len(col)>0:
            data_dict = {
                "Name" : col[1].get_text(strip=True),
                "MC_USD_Billion" : col[2].contents[0][:-1]
            }
            df1 = pd.DataFrame(data_dict,index=[0])
            df = pd.concat([df,df1],ignore_index=True)
    
    return df

def transform(df,exchange_file_path):
    exchange_rate = pd.read_csv(exchange_file_path)
    
    mc_list = df['MC_USD_Billion'].tolist()
    mc_list = [float("".join(x.split(","))) for x in mc_list]
    mc_list = np.array(mc_list)
    
    mc_eur_list = np.round(np.multiply(mc_list,exchange_rate['Rate'][0]),2)
    mc_gbp_list = np.round(np.multiply(mc_list,exchange_rate['Rate'][1]),2)
    mc_inr_list = np.round(np.multiply(mc_list,exchange_rate['Rate'][2]),2)
    
    #Append three list above to df with three new columns
    new_colums = pd.DataFrame({'MC_EUR_Billion' : mc_eur_list , 'MC_GBP_Billion' : mc_gbp_list, 'MC_INR_Billion' : mc_inr_list})
    df = pd.concat([df,new_colums],axis=1)
    print(df)
    return df
def load_to_csv(df,file_path):
    df.to_csv(file_path,index=False)

def load_to_db(df,sql_connection,table_name):
    df.to_sql(table_name,sql_connection,if_exists='replace',index = False)
    
def run_query(query_statement,sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement,sql_connection)
    print(query_output)
    return query_output    


url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attributes = ["Name","MC_USD_Billion"]
db_name = "Banks.db"
table_name = "Largest_banks"
csv_path = "D:/Source VS Code/Coursera/Final_Project_in_Python_for_Data_Science/exchange_rate.csv"

def main():
    log_progress("ETL Process Started")
    df = extract(url,table_attributes)
    print(df)
    log_progress("Data Extracted")
    df = transform(df,csv_path)
    log_progress("Data Transformed")
    load_to_csv(df,"./largest_banks_data.csv")
    log_progress("Data saved to CSV file")
    sql_connection = sqlite3.connect(db_name)
    log_progress("SQL connection initiated")
    load_to_db(df,sql_connection,table_name)
    log_progress("Data saved to Database. Running query")
    query_statement1 = f"SELECT * FROM Largest_banks"
    run_query(query_statement1,sql_connection)
    log_progress("Run the first query completed")
    query_statement2 = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
    run_query(query_statement2,sql_connection)
    log_progress("Run the second query completed")
    query_statement3 = f"SELECT Name from Largest_banks LIMIT 5"
    run_query(query_statement3,sql_connection)
    log_progress("Run the third query completed")
    query_statement4 = f"SELECT Name from Largest_banks LIMIT 5"
    run_query(query_statement4,sql_connection)
    log_progress("Run the fourth query completed")
    log_progress("Process completed")
    sql_connection.close()

main()
    
    