# -*- coding: utf-8 -*-
"""
Created on Sun Nov 25 14:48:03 2018

@author: MLOURE13
"""
import pandas as pd
import time as t

start_time=t.time()


class App_Succ_Rate(object):
    def __init__(self,app_id, cur_state, req_date, req_time):
        self.app_id=app_id
        self.cur_state=cur_state
        self.req_date=req_date
        self.req_time=req_time
        self.app_data={}
        self.app_data[str(self.cur_state)]=self.req_date             
        
    def __repr__(self):
        return repr([self.app_id, self.cur_state, self.req_date, self.req_time, self.app_data]) 
    
      
df=pd.read_table('class.txt', sep=',', header=0)
df['WK']=df['DATEREQUESTED'].map(lambda x : x[0:9]).map(lambda x: pd.datetime.strptime(x,'%d-%b-%y')).map(lambda x: pd.datetime.isocalendar(x)[1])
df['Date']=df['DATEREQUESTED'].map(lambda x : x[0:9]).map(lambda x: pd.datetime.strptime(x,'%d-%b-%y'))
df['Time']=df['DATEREQUESTED'].map(lambda x: x[9:18]).map(lambda x: x.replace('.',':')).map(lambda x: x.strip())
df.drop('DATEREQUESTED', axis=1, inplace=True)

''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
'''''''''''''''''''''''''''''''''''''''''' # Unique Lists'''''''''''''''''''''''''''''''''''''''''
''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
Unique_APP=df['APPID'].drop_duplicates(keep='first')
Unique_CS=df['CURRENTSTATE'].drop_duplicates(keep='first')
Unique_Days=df['Date'].drop_duplicates(keep='first')
Unique_Weeks=df['WK'].drop_duplicates(keep='first')
dur=(t.time()-start_time)

#h=App_Succ_Rate('G1265','REQ_START','10 DEC 2018','10:24:49')

def success_rate(Unique_APP, Unique_CS, df):
    start_time=t.time()
    app_count=0
    error_count=0
    suc_count=0
    start_count=0
    abort_count=0
    
    error_state=['ERROR', 'DOWNLOAD_APPLICATION_FAILURE', 'COMMS_ERROR']
    succe_state=['FINISHED', 'DOWNLOAD_APPLICATION_SUCCESS','INDICTED']
    start_state=['REQUESTED_START','REQUESTED_DOWNLOAD']
    abort_state=['ABORTED']
    
    for app in Unique_APP:
        for i in range(len(df)):
            if df['APPID'][i] == app:
                if str(df['CURRENTSTATE'][i]) in error_state:
                    error_count+= 1
                elif str(df['CURRENTSTATE'][i]) in succe_state:
                    suc_count+=1
                elif str(df['CURRENTSTATE'][i]) in start_state:
                    start_count+=1
                elif str(df['CURRENTSTATE'][i]) in abort_state:
                    abort_count+=1
                    
                app_count=app_count+1
                #print(app, count, df['APPID'][i])
        print(app, app_count, error_count, suc_count, start_count, abort_count)
        app_count=0
        error_count=0
        suc_count=0
        start_count=0
        abort_count=0
        dur=(t.time()-start_time)
        print('It took : ' + str(dur))                              
sr=success_rate(Unique_APP, Unique_CS, df)

#print(h)
print('It took: ' + str(dur) + ' seconds. ')
