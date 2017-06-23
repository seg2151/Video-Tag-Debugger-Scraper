# -*- coding: utf-8 -*-
"""
Created on Wed May  3 09:43:50 2017

@author: SGarcia
"""

import requests
import yaml
import time
import pandas as pd
import win32com.client
from datetime import datetime
from impala.dbapi import connect
from impala.util import as_pandas


def find_http(domain):
    """Determines if URL is a full URL or root domain"""
    
    if not domain:
        return None
    elif len(domain) == 0:
        return ''
    elif 'http' in domain:
        return True
    else:
        return False
        
def shorten(string):
    try: 
        return string[:250]
    except:
        return None

def run():
    STAMP = '{:03.0f}'.format(datetime.timestamp(datetime.now()))
    
    CAPTURE_NAME = '{}_video_scraper'.format(STAMP)
    CAPTURE_SIZE = 1000
    CAPTURE_EXPIRY = 2
    
    #all pubs
    PAYLOAD = {"key":{"name":CAPTURE_NAME,"type":"TAG"},"logSize":CAPTURE_SIZE,"expireSeconds":CAPTURE_EXPIRY*3600,"expression":{"condition":"AND","rules":[{"field":"request_impression_type","operator":"EQUAL","value":"VIDEO"},{"field":"response_def_level_id","operator":"EQUAL","value":"DEF_LEVEL_15_UNAUTHORIZED"}],"valid":True}}
    #single pub
    #PAYLOAD = {"key":{"name":CAPTURE_NAME,"type":"RTS"},"logSize":CAPTURE_SIZE,"expireSeconds":CAPTURE_EXPIRY*3600,"expression":{"condition":"AND","rules":[{"field":"request_publisher_id","operator":"EQUAL","value":"560439"},{"field":"request_impression_type","operator":"EQUAL","value":"VIDEO"},{"field":"response_def_level_id","operator":"EQUAL","value":"DEF_LEVEL_12_PASSBACK"}],"valid":True}}
    r = requests.post('DEBUGGER HERE',json=PAYLOAD)
    print(r.status_code)
    
    time.sleep(30)
    
    g = requests.get('DEBUGGER LOGS HERE'.format(CAPTURE_NAME))
    print(g.status_code)
    
    d = g.json()
    
    requests.delete('DEBUGGER TYPE HERE'.format(CAPTURE_NAME))
    
    obj_dict = {}
    pub_dict = {}
    tag_dict = {}
    cwu_dict = {}
    dlr_dict = {}
    
    for i in range(len(d)):
        #print(i)
        try:
            obj_dict[i] = d[i]['request']['requestParams']['video']
        except KeyError:
            obj_dict[i] = None
        try:
            cwu_dict[i] = d[i]['request']['requestParams']['cwu']
        except KeyError:
            cwu_dict[i] = None
        pub_dict[i] = d[i]['request']['mpcBidRequest']['publisherId']
        tag_dict[i] = d[i]['request']['mpcBidRequest']['adTagId']
        dlr_dict[i] = d[i]['response']['reason']
        
    # for keys that are MISSING VALUES
    error_dict = {}
    #for parsing errors!
    parsing_error_dict = {}
    error_list = []
       
    for i in range(len(obj_dict)):
        sample = obj_dict[i]
        try:
            error_dict[i] = [i for i in yaml.load(sample) if yaml.load(sample)[i] == None]
        except:
            parsing_error_dict[i] = sample
            
    for i in error_dict:
        if len(error_dict[i]) > 0:
            for j in error_dict[i]:
                error_list.append(j)
        else:
            error_list.append('varied')
        
    error_series = pd.Series(error_list)
    #print(error_series.value_counts())
    
    keys = list(pub_dict.keys())
    values = list(pub_dict.values())
    
    pub_df = pd.DataFrame(data=pd.Series(values), index=pd.Series(keys), columns = ['PubID'])
    
    error_df = pd.DataFrame(pd.Series(error_dict),columns=['Missing_Field'])
    error_df['Missing_Field'] = error_df['Missing_Field'].apply(lambda x: ' '.join(x))

    tag_df = pd.DataFrame(list(tag_dict.values()),index=list(tag_dict.keys()),columns=['TagID'])
    
    obj_df = pd.DataFrame(list(obj_dict.values()),index=list(obj_dict.keys()),columns=['Video_Object'])
    
    dlr_df = pd.DataFrame(list(dlr_dict.values()),index=list(dlr_dict.keys()),columns=['DLR'])
    
    parse_copy = parsing_error_dict.copy()
    for i in parse_copy.keys():
        parse_copy[i] = 'error'
    
    parse_df = pd.DataFrame(data=list(parse_copy.values()),index=list(parse_copy.keys()),columns=['Parse_error'])
        
    cwu_df = pd.DataFrame(data=list(cwu_dict.values()),index=list(cwu_dict.keys()),columns=['CWU'])
    cwu_df['CWU'] = cwu_df['CWU'].apply(shorten)
        
    cwu_error_df = cwu_df['CWU'].apply(find_http)
    
    #query names
    ID_list = []
    for i in pub_df['PubID']:
        ID_list.append(i)
    ID_list = list(set(ID_list))
    if len(ID_list) > 1:
        ID_list = tuple(set(ID_list))
    else:    
        ID_list = '('+str(ID_list[0])+')'
    print('ID LIST IS:', ID_list)
    query = '''
    SELECT accountid,accountname
    FROM reference.masteraccount
    WHERE accountid in
    {}
    '''.format(ID_list)
    
    print('Connecting to host...')
    conn = connect(host = 'HOST HERE', port = PORT HERE)
    print('Connected')
    cursor = conn.cursor()
    print('Executing sql script...')
    cursor.execute(query)
    print('Creating dataframe...')
    df_cursor = as_pandas(cursor)
    df_cursor.columns = ['PubID','PubName']
    
    #query account owners
    query2 = '''
    SELECT m.accountid,
    own.firstname as AC_first,
    own.lastname as AC_last,
    own2.firstname as BD_first,
    own2.lastname as BD_last
    
    FROM reference.masteraccount m
    
    LEFT JOIN reference.accountcontact acc
    ON m.accountid = acc.accountid

    LEFT JOIN reference.accountowner own
    ON acc.accountmanagerid = own.id

    LEFT JOIN reference.accountowner own2
    ON acc.businessdevid = own2.id
    
    WHERE m.accountid in
    {}
    
    GROUP BY 1,2,3,4,5
    '''.format(ID_list)
    
    print('Connecting to host...')
    conn = connect(host = 'HOST HERE', port = PORT HERE)
    print('Connected')
    cursor = conn.cursor()
    print('Executing sql script...')
    cursor.execute(query2)
    print('Creating dataframe...')
    df_cursor2 = as_pandas(cursor)
    df_cursor2.columns = ['PubID','AM_First','AM_Last','BD_First','BD_Last']
    
    df = pd.merge(pub_df,df_cursor,how='left',on='PubID')
    df = pd.concat([df,tag_df,error_df,parse_df,cwu_df,cwu_error_df,obj_df,dlr_df],axis=1)
    df.columns = ['PubID', 'PubName', 'TagID', 'Missing_Field', 'Parse_error', 'CWU',
       'CorrectCWU', 'Video_Object','DLR']
    df = pd.merge(df,df_cursor2,how='left',on='PubID')
    return df

df = run()

for i in range(10):
    df_i = run()
    df = pd.concat([df,df_i])
    time.sleep(180)

df.sort_values(by='AM_First')
DATE = str(datetime.now()).split(' ')[0]
FILE_NAME = 'Video Tag Scrape {}.xlsx'.format(DATE)
FILEPATH = 'PATH_TO_FILE'.format(FILE_NAME)

writer = pd.ExcelWriter(FILEPATH,options={'strings_to_urls':False})
df.to_excel(writer,index=False)
writer.save()

def send_email(FILEPATH):
    
    olMailItem = 0x0
    obj = win32com.client.Dispatch("Outlook.Application")
    newMail = obj.CreateItem(olMailItem)
    newMail.Subject = "Misconfigured Tag Video {}".format(DATE)
    newMail.To = 'RECEIVERS HERE'
    newMail.body = '''
    Report instructions here
    '''
    newMail.Attachments.Add(Source=FILEPATH)
    
    newMail.Send()
    print('Sent!')

send_email(FILEPATH)