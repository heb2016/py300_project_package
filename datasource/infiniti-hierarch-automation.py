
#!/usr/bin/env python3

from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker
import psycopg2
import sys
import os
from os.path  import join, dirname
from datetime import date, timedelta, datetime
from urllib   import parse
from pandas   import DataFrame, ExcelWriter 
import pandas as pd

import email
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders


import logging
import traceback

logfile = 'logfile.log'
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())


userfile_dir = os.path.abspath('./user')
userfile_name = 'user.txt'

sql_dir = os.path.abspath('./sql')
sql_file_name = 'infiniti_webpage_render.sql'

import_dir = os.path.abspath('./import')
import_name = 'Infiniti_Dealer_Master.xlsx'
 

# Get database connection 

def get_db_connection():
    host='host = gpdb.prod.cdk.com  dbname= fm01 '
    port = ' port = 5432'
    conn_string=''
    userfile_loc = userfile_dir + '/' + userfile_name
    print('userfile_loc:',  userfile_loc)
    try:
        userfile = open(userfile_loc,'r')
        count = 0  
        for line in userfile.readlines():  
            if count ==0: 
               # username = line.split(' ')[1].strip(' ').strip('\n')  -- use this when python file format needs
                username = line 
            if count ==1:
             #   password = line.split(' ')[1].strip(' ').strip('\n')  
                password = line 
                break
            count=+1
 
        user =str.join(' ', ('user=', username)) 
        passwd = str.join(' ', ('password=', password))
        conn_string =str.join(' ', (host, user, passwd, port)) 
   #     print('conn_string:', count, conn_string)   -- test
        userfile.close

    except IOError:
        print ("IO Error")

    except KeyboardInterrupt: #Raised when the user hits the interrupt key (normally Control-C or Delete). 
        print ("KeyboardInterrupt")

    finally:  
        return(conn_string)

## read sql script file
def get_sql_source():  
    sql_loc = sql_dir + '/' + sql_file_name
    print('sql_loc:',  sql_loc)
    sql_source = ""
    sql_source = open(sql_loc).read()
        
    return (sql_source) 

## get dealers who has page views from EDW page_render table 
def get_edw_dealer(): 
    
    print('SQL File Log')
    sql_source = get_sql_source()
    print('\n')
    print('EDW DBConnection Log')
    conn_string = get_db_connection()   
    print('\n')

    try:
        connection = psycopg2.connect(conn_string) 
        cursor = connection.cursor()  
        cursor.execute(sql_source)
        names = [ x[0] for x in cursor.description]
        records = cursor.fetchall() 
        print('records:', records) 

    finally:  
        df_edw =  DataFrame(records, columns = names)
        df_count= len(df_edw)
        print('EDW Record Counts: ', df_count)
        print('EDW 5 Rows:')
        print(df_edw.head(5)) 
        print('\n')

        if df_count ==0:
            print('No Infiniti new dealers from EDW') 
        
        out_loc = out_dir + '/' + out_edw 
        print('Save EDW SQL Output ', df_count,' Records to ', out_loc) 
        print('\n')
        df_edw.to_csv(out_loc, sep = '\t', encoding = 'utf-8')    ## save EDW output for QA
        cursor.close()
        connection.close()  
        return(df_edw)
 