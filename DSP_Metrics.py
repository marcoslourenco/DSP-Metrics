# -*- coding: utf-8 -*-
"""
Created on Sun Nov 25 14:48:03 2018
@author: MLOURE13
"""
import pandas as pd
import time as t
import threading 

start_time=t.time()


class App_Succ_Rate(object):
    def __init__(self,app_id, app_count):
        self.app_id=app_id
        self.app_count=app_count        
        #self.app_data[str(self.cur_state)]=self.req_date             
        
    def __repr__(self):
        return repr([self.app_id, self.app_count])
    
      
df=pd.read_table('class_big.txt', sep=',', header=0)
df['WK']=df['DATEREQUESTED'].map(lambda x : x[0:9]).map(lambda x: pd.datetime.strptime(x,'%d-%b-%y')).map(lambda x: pd.datetime.isocalendar(x)[1])
df['Date']=df['DATEREQUESTED'].map(lambda x : x[0:9]).map(lambda x: pd.datetime.strptime(x,'%d-%b-%y'))
df['Time']=df['DATEREQUESTED'].map(lambda x: x[9:18]).map(lambda x: x.replace('.',':')).map(lambda x: x.strip())
df.drop('DATEREQUESTED', axis=1, inplace=True)
Unique_APP=df['APPID'].drop_duplicates(keep='first')
Unique_CS=df['CURRENTSTATE'].drop_duplicates(keep='first')
Unique_Days=df['Date'].drop_duplicates(keep='first')
Unique_Weeks=df['WK'].drop_duplicates(keep='first')

def succ_rate(start_count, succ_count):
    if start_count>0 and succ_count<=start_count:
        return succ_count/start_count
    elif succ_count>start_count:
        return 0
    elif succ_count==0 and start_count==0:
        return float('NaN')
    else:
        return 0

def df_transformation(Unique_APP, Unique_CS, df):
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
    dfData = pd.DataFrame(columns=['APP Count','Start Count','Success Count', 'Error Count', 'Abort Count'])
    
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
        #print(app, app_count, error_count, suc_count, start_count, abort_count)
        dfData.loc[str(app), 'APP Count'] = int(app_count)
        dfData.loc[str(app), 'Start Count'] = int(start_count)
        dfData.loc[str(app), 'Success Count'] = int(suc_count)
        dfData.loc[str(app), 'Error Count'] = int(error_count)
        dfData.loc[str(app), 'Abort Count'] = int(abort_count)        
        dfData.loc[str(app), 'Success Rate']= float(succ_rate(start_count,suc_count))        
        #print(dfData)
        app_count=0
        error_count=0
        suc_count=0
        start_count=0
        abort_count=0
        dur=(t.time()-start_time)
    print('It took : ' + str(dur))
    return dfData

def df_SuccessRate_Weekly(Unique_APP, Unique_CS, df, UniqueWeeks):
    start_time=t.time()    
    error_count=0
    suc_count=0
    start_count=0
    abort_count=0
    
    error_state=['ERROR', 'DOWNLOAD_APPLICATION_FAILURE', 'COMMS_ERROR']
    succe_state=['FINISHED', 'DOWNLOAD_APPLICATION_SUCCESS','INDICTED']
    start_state=['REQUESTED_START','REQUESTED_DOWNLOAD']
    abort_state=['ABORTED']    
    dfDataWeekly = pd.DataFrame(columns=UniqueWeeks)
    #dfDataErrorAbort=pd.DataFrame(columns=UniqueWeeks)
    
    for wk in UniqueWeeks:
        for app in Unique_APP:
            for i in range(len(df)):
                if df['APPID'][i] == app:
                    if str(df['WK'][i])==str(wk) and str(df['CURRENTSTATE'][i]) in error_state:
                        error_count+= 1
                    elif str(df['WK'][i])==str(wk) and str(df['CURRENTSTATE'][i]) in succe_state:
                        suc_count+=1
                    elif str(df['WK'][i])==str(wk) and str(df['CURRENTSTATE'][i]) in start_state:
                        start_count+=1
                    elif str(df['WK'][i])==str(wk) and str(df['CURRENTSTATE'][i]) in abort_state:
                        abort_count+=1
                                             
        #print(app, app_count, error_count, suc_count, start_count, abort_count)                     
            dfDataWeekly.loc[str(app), wk] = float(succ_rate(start_count,suc_count))            
            print(app, wk)
            error_count=0
            suc_count=0
            start_count=0
            abort_count=0
            dur=(t.time()-start_time)
    print('It took : ' + str(dur))
    return dfDataWeekly

def success_rate_general(df):
    sorted_sr=df.sort_values(['APP Count', 'Start Count'], ascending=False)
    sum_app_count=df['APP Count'].sum()
    print(sum_app_count)
    return sorted_sr 

def error_analysis(se_succ_rate_summary, Unique_Weeks, df):
    dfErrorDataWeekly = pd.DataFrame(columns=['Error_Count', 'Abort_Count'])
    error_state=['ERROR', 'DOWNLOAD_APPLICATION_FAILURE', 'COMMS_ERROR']
    abort_state=['ABORTED']
    start_state=['REQUESTED_START','REQUESTED_DOWNLOAD']
    count_error=0
    count_abort=0
    count_start=0
    
    for wk in Unique_Weeks:
        for i in df['CURRENTSTATE']:
            if i in error_state:
                count_error+=1
            elif i in abort_state:
                count_abort+=1
            elif i in start_state:
                count_start+=1
        dfErrorDataWeekly.loc[wk, 'Error_Count' ] = count_error/count_start
        dfErrorDataWeekly.loc[wk, 'Abort_Count' ] = count_abort/count_start
        dfErrorDataWeekly.loc[wk, 'Start_Requests' ] = count_start
    
    dfErrorDataWeekly['Succ_Rate']= se_succ_rate_summary.values 
    return dfErrorDataWeekly
                
    
#concu=threading.Thread(target=df_SR, name='Thread1', args=(Unique_APP, Unique_CS, df))
#concu.start()
#concu.join()          
sr=df_transformation(Unique_APP, Unique_CS, df)
app_succ_rate_weekly_table=df_SuccessRate_Weekly(Unique_APP, Unique_CS, df, Unique_Weeks)
se=success_rate_general(sr)
succ_rate_summary=app_succ_rate_weekly_table.loc[:,Unique_Weeks].mean()
error_table= error_analysis(succ_rate_summary, Unique_Weeks, df )

dur=(t.time()-start_time)

print('It took: ' + str(dur) + ' seconds. ')
