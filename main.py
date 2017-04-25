
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

from dbconnection_package import MyDbConnection
 

 

sqldir = os.path.abspath('./sql')
sqlfile = 'infiniti_webpage_render.sql'   ## argv

import_dir = os.path.abspath('./import')
import_name = 'Infiniti_Dealer_Master.xlsx'  #argv



def main():
    print('Please input your username:')
    msg_username = input().strip()
    print('Please input your password:')
    msg_password = input().strip()

    host= 'host=gpdb.prod.cdk.com'
    dbname = 'dbname=fm01'
    username = str.join(' ', ('user=', msg_username))
    password = str.join(' ', ('password=', msg_password))
    port = 'port = 5432' 
    connstring =str.join(' ', (host, dbname, username, password, port)) 
     

# Get EDW data
    myDB = MyDbConnection()
    myDB.calledFromMain(sqldir, sqlfile, connstring)

    return 1

if __name__ == '__main__':
 
    sys.exit(main())
