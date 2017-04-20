
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

out_dir = os.path.abspath('./output') 
out_edw = 'edw_infiniti_webpage_render.csv'
 
output_name = 'infiniti_new_dealer_merge-{0:%Y}-{0:%m}-{0:%d}'.format(date.today()) + '.xlsx'


#RECIPIENTS = ['Beatrice.He@cdk.com']   ## test purpose 
RECIPIENTS = ['Beatrice.He@cdk.com', 'Olga.Davidov@cdk.com', 'Jon.Wales@cdk.com',  'Shanta.Ganguly@cdk.com', 'Crystal.Smithwick@cdk.com'  ]

print('Environment Log')
print('Message: Current working directory: %s' % os.getcwd())
print('Message: SQL directory: %s' % sql_dir)
#print('Message: SQL file: %s' % sys.argv[1])

print('Message: Import Excel working directory: %s' % import_dir)
print('Message: Import Excel name: %s' % import_name)

print('Message: Output directory: %s' % out_dir)
print('Message: Output name: %s' % output_name) 
print('\n')


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


## import Infiniti dealer file  Excel based on Susan's email attachment            
def get_import ():  
    print('Excel Import Log')
    import_loc = import_dir + '/' + import_name
    print('import_loc: ', import_loc)
    print('\n')
    
    df_excel = pd.read_excel(import_loc,  sheetname= 'Infiniti Master',
           header=1,  
           parse_cols = "A:F,O,P" )   

    df_excel.columns= [
            'Dealer Code',  
            'Region', 
            'Area',
            'District', 
            'District_name',
            'Dealer_name',
            'web_id' ,
            'Target_Live_Date']    ## only select these columns

    if len(df_excel) ==0:
        print('Infiniti Dealer File has no record')    

    # print the column names
    print('Excel Import counts: ', len(df_excel) )
    print('Excel Import fields: ', df_excel.columns)
    print('Excel Import 5 Rows: ') 
    print(df_excel.head(5))
    print('\n')
 
    return(df_excel)


# create region_new column to sync wiht EDW request
def new_region(row):
   if row['Region'] == 62 :
      return ('62 - South')
   if row['Region'] == 72 :
      return ('72 - East')
   if row['Region'] == 82 :
      return ('82 - North')
   if row['Region'] == 92 : 
      return ('92 - West')
  

## merge EDW and Excel to get final
def merge_edw_excel():
    print('EDW & Excel Merge Log')  
    df_edw   = get_edw_dealer()    
    df_excel = get_import() 
##    df_merge_result_tmp =df_excel   ## -- test purpose 

    df_merge_result_tmp = pd.merge(df_edw, df_excel, how='inner', on=['web_id'])    
    print('df_merge_result_tmp', df_merge_result_tmp)
    print(type(df_merge_result_tmp['Region']))

    print(type(df_merge_result_tmp['District']))
        
    if len(df_merge_result_tmp)==0: 
        return (df_merge_result_tmp)


# Hierarchy Display Name  Hier Type   Web Id                  Region     Zone    Area    Dealer Code OEM Owner   Is Active
# Infiniti                Tier 3      infiniti-competition    72 - East   NY    1 - NYC   70016       infiniti    Is Active


    df_merge_result_tmp['District_name'] = df_merge_result_tmp['District_name'].str.replace('Georgia/TN','GA TN')   ## manually update George to sync EDW
    df_merge_result_tmp['District_name'] = df_merge_result_tmp['District_name'].str.replace('/',' ')                ## replace '/' to space ' ' to sync EDW
 
    df_merge_result_tmp['Region_new']=df_merge_result_tmp.apply (lambda row: new_region(row),axis=1)                ## manually update region to sync EDW
    df_merge_result_tmp['Area_New'] =df_merge_result_tmp['District'].map(str)+' - '+df_merge_result_tmp['District_name']   ## manually update Area to sync EDW

    
    del df_merge_result_tmp['Region']

    df_merge_result_tmp = df_merge_result_tmp.rename(columns={'Region_new': 'Region'})
    df_merge_result_tmp = df_merge_result_tmp.rename(columns={'web_id': 'Web Id'})
    df_merge_result_tmp = df_merge_result_tmp.rename(columns={'Area': 'Zone'})
    df_merge_result_tmp = df_merge_result_tmp.rename(columns={'Area_New': 'Area'})
    df_merge_result_tmp['Hierarchy Display Name'] = 'Infiniti'
    df_merge_result_tmp['Hier Type'] = 'Tier 3'
    df_merge_result_tmp['OEM Owner'] = 'infiniti'
    df_merge_result_tmp['Is Active'] = 'Y' 
 
    del df_merge_result_tmp['District_name']
    del df_merge_result_tmp['Dealer_name']
    del df_merge_result_tmp['District']



 ##   df_merge_result = df_merge_result_tmp[['Hierarchy Display Name', 'Hier Type', 'Web Id', 'Region', 'Zone', 'Area', 'Dealer Code', 
 ##                                               'OEM Owner', 'Is Active', 'Target_Live_Date']]   -- test purpose 

    df_merge_result = df_merge_result_tmp[['Hierarchy Display Name', 'Hier Type', 'Web Id', 'Region', 'Zone', 'Area', 'Dealer Code', 
                                                'OEM Owner', 'Is Active', 'Target_Live_Date', 'total_page_views', 'total_visits', 'total_vdp_views']]    
     
    print('EDW & Excel Merge counts: ', len(df_merge_result) )
    print('EDW & Excel Merge 5 Rows: ') 
    print(df_merge_result.head(5))
    print('\n') 
    return (df_merge_result)


## figure out output file format
def write_output():   
    print('Write Output Log\n')
    df_merge_result = merge_edw_excel()   
    
    if len(df_merge_result) == 0:
        print('There is no new dealer') 
        return (0)

## figure out out put formating, twist df_merge_result result to combine some columns to create output columns
#   out_merge_result = df_merge_result  twist

    out_merge_result = df_merge_result 

    out_loc = out_dir + '/' + output_name
    print('out_dir:', out_loc)
    print('\n')
    print('Output has records: ', len(out_merge_result))   
    print('Merged DataFrame out_merge_result: ')
    print(out_merge_result)
    print('\n')

    writer = pd.ExcelWriter(out_loc, engine='xlsxwriter', date_format='YYYY mmmm',
                            datetime_format='YYYY mmmm') 
    out_merge_result.to_excel(writer, sheet_name='Infiniti_New_Dealer') 
    writer.save()
    return (1)



  #  out_merge_result.to_csv(out_loc, sep = '\t', encoding = 'utf-8')
    print('\n')
  #  return (1)

def send_mail_app1(addr_from, subject, body_message, file_to_attach):
    '''
    Purpose: Prepare and send email message with attachments
    Preconditions: recipients is a well-formed list, file to attach exists, permissions are sufficient.
    Postconditions: none.
    Returns: none.
    '''
    msg = MIMEMultipart()
    msg['From'] = addr_from
    msg['To'] = ', '.join(RECIPIENTS)
    msg['Subject'] = subject
    body = body_message 

    msg.attach(MIMEText(body, 'plain')) 
    part = MIMEBase('application', 'octet-stream')  
    part.set_payload(open(file_to_attach, "rb").read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment', filename=file_to_attach)
    msg.attach(part)

 #   server = smtplib.SMTP('dmlpsmtp.cdk.com') 
    server = smtplib.SMTP('ordpsmtp.cdk.com')   

    server.set_debuglevel(1)
    server.sendmail(addr_from, RECIPIENTS, msg.as_string())
    server.quit()


def send_mail_app0(addr_from, subject, body_message):
    '''
    Purpose: Prepare and send email message with attachments
    Preconditions: recipients is a well-formed list, file to attach exists, permissions are sufficient.
    Postconditions: none.
    Returns: none.
    '''
    msg = MIMEMultipart()
    msg['From'] = addr_from
    msg['To'] = ', '.join(RECIPIENTS)
    msg['Subject'] = subject
    body = body_message 
    msg.attach(MIMEText(body, 'plain'))

#   part = MIMEBase('application', 'octet-stream')
#   part.set_payload(open(file_to_attach, "rb").read())
#   encoders.encode_base64(part)
#   part.add_header('Content-Disposition', 'attachment', filename=file_to_attach)
#   msg.attach(part)

 #   server = smtplib.SMTP('dmlpsmtp.cdk.com') 
    server = smtplib.SMTP('ordpsmtp.cdk.com')   

    server.set_debuglevel(1)
    server.sendmail(addr_from, RECIPIENTS, msg.as_string())
    server.quit()


 
def send_email(output_flag):
    print('output_flag: ', output_flag)  
    try:
        if output_flag ==1: 
            print('Call-send_mail_app1')
            print('Message: Sending email with Attachment')
            os.chdir(out_dir)
            print('Message: Current working directory is: %s' % os.getcwd()) 
            print('\n')
            send_mail_app1('beatrice.he@cdk.com',
              'New Infiniti Dealers',
              'The attached file contains new dealers for Infiniti Hierarch. Please remove the field "A" when you upload data to online tool.',
              output_name)
            print('\n')

        if output_flag ==0:
            print('Call-send_mail_app0')
            print('Message: Sending email without Attachment')
            print('\n')
            send_mail_app0('beatrice.he@cdk.com',
              'There is No New Infiniti Dealer',
              'There is no new dealer for Infiniti Hierarch.' )
            print('\n')
        print('Message: Mail successfully sent')
    except ConnectionRefusedError as e:
        errno, strerror = e.args
        print('Error: Connection error {0}: {1}'.format(errno, strerror))
    except:
        print('Error: Unexpected error; check distribution, SMTP relay name, other settings:', sys.exc_info()[0])


def main():

    # Set up logging
    try: 
        output_flag=write_output()
        send_email(output_flag)
    except (SystemExit, KeyboardInterrupt):
        raise
 #   except Exception, e:
  #      logger.error('Failed to open file', exc_info=True) 
    return 1


if __name__ == '__main__':
    sys.exit(main())
