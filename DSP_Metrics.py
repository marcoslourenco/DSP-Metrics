# -*- coding: utf-8 -*-
"""
Created on Sun Nov 25 14:48:03 2018
@author: MLOURE13
"""
import pandas as pd
import time as t
import DBOperations as dbo

start_time=t.time()

class AppLoad(object):
    def __init__(self,date_req, app_count):
        self.date_req = date_req
        self.app_count = app_count        
        #self.app_data[str(self.cur_state)]=self.req_date             
        
    def __repr__(self):
        return repr([self.date_req, self.app_count])
     
df=pd.read_table('class_FEWDAYS.txt', sep=',', header=0)
df['WK']=df['DATEREQUESTED'].map(lambda x : x[0:9]).map(lambda x: pd.datetime.strptime(x,'%d-%b-%y')).map(lambda x: pd.datetime.isocalendar(x)[1])
df['Date']=df['DATEREQUESTED'].map(lambda x : x[0:9])#.map(lambda x: pd.datetime.strptime(x,'%d-%b-%y'))
df['Time']=df['DATEREQUESTED'].map(lambda x: x[9:18]).map(lambda x: x.replace('.',':')).map(lambda x: x.strip())
df.drop('DATEREQUESTED', axis=1, inplace=True)
Unique_APP=df['APPID'].drop_duplicates(keep='first')
Unique_CS=df['CURRENTSTATE'].drop_duplicates(keep='first')
Unique_Days=df['Date'].drop_duplicates(keep='first')
#Unique_Weeks=df['WK'].drop_duplicates(keep='first')
    

def succ_rate(finished_count, error_count):
    if finished_count>0 and error_count>=0:
        return finished_count/(finished_count+error_count)
    elif finished_count==0 and error_count==0:
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
    
    error_state=['ERROR', 'DOWNLOAD_APPLICATION_FAILURE', 'COMMS_ERROR','DOWNLOAD_JNLP_FAILURE']
    succe_state=['FINISHED', 'DOWNLOAD_APPLICATION_SUCCESS','INDICTED', 'DOWNLOAD_JNLP_SUCCESS']
    start_state=['REQUESTED_START','REQUESTED_DOWNLOAD','DOWNLOAD_JNLP_REQUESTED']
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
        dfData.loc[str(app), 'Success Rate']= float(succ_rate(suc_count,error_count))        
        #print(dfData)
        app_count=0
        error_count=0
        suc_count=0
        start_count=0
        abort_count=0
        dur=(t.time()-start_time)
    print('It took : ' + str(dur))
    return dfData

def df_SuccessRate_Daily(Unique_APP, Unique_CS, df, Unique_Days):
    start_time=t.time()    
    error_count=0
    suc_count=0
    start_count=0
    abort_count=0
    
    error_state = ['ERROR', 'DOWNLOAD_APPLICATION_FAILURE', 'COMMS_ERROR','DOWNLOAD_JNLP_FAILURE']
    succe_state = ['FINISHED', 'DOWNLOAD_APPLICATION_SUCCESS','INDICTED', 'DOWNLOAD_JNLP_SUCCESS']
    start_state = ['REQUESTED_START','REQUESTED_DOWNLOAD','DOWNLOAD_JNLP_REQUESTED']
    abort_state = ['ABORTED']    
    dfDataWeekly = pd.DataFrame(columns=Unique_Days)
    #dfDataErrorAbort=pd.DataFrame(columns=UniqueWeeks)
    
    for wk in Unique_Days:        
        for app in Unique_APP:
            for i in range(len(df)):
                if df['APPID'][i] == app:
                    if str(df['Date'][i])==str(wk) and str(df['CURRENTSTATE'][i]) in error_state:
                        error_count+= 1
                    elif str(df['Date'][i])==str(wk) and str(df['CURRENTSTATE'][i]) in succe_state:
                        suc_count+=1
                    elif str(df['Date'][i])==str(wk) and str(df['CURRENTSTATE'][i]) in start_state:
                        start_count+=1
                    elif str(df['Date'][i])==str(wk) and str(df['CURRENTSTATE'][i]) in abort_state:
                        abort_count+=1
                                             
        #print(app, app_count, error_count, suc_count, start_count, abort_count)                     
            dfDataWeekly.loc[str(app), str(wk)] = float(succ_rate(suc_count,error_count))            
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


def error_analysis(se_succ_rate_summary, Unique_Days, df):
    dfErrorDataDaily = pd.DataFrame(columns=['% Errors', '% Aborts'])
    error_state=['ERROR', 'DOWNLOAD_APPLICATION_FAILURE', 'COMMS_ERROR','DOWNLOAD_JNLP_FAILURE']
    abort_state=['ABORTED']
    start_state= ['REQUESTED_START','REQUESTED_DOWNLOAD','DOWNLOAD_JNLP_REQUESTED','REQUESTED']
    finished_state=['FINISHED', 'DOWNLOAD_APPLICATION_SUCCESS','INDICTED', 'DOWNLOAD_JNLP_SUCCESS']
    
    count_error=0
    count_abort=0
    count_start=0
    count_finished=0
    
    for wk in Unique_Days:
        for i in range(len(df)):
            if str(wk)==str(df['Date'][i]) and str(df['CURRENTSTATE'][i]) in error_state:
                count_error+=1
            elif str(wk)==str(df['Date'][i]) and str(df['CURRENTSTATE'][i]) in abort_state:
                count_abort+=1
            elif str(wk)==str(df['Date'][i]) and str(df['CURRENTSTATE'][i]) in start_state:
                count_start+=1
            elif str(wk)==str(df['Date'][i]) and str(df['CURRENTSTATE'][i]) in finished_state:
                count_finished+=1
        errors=count_error/(count_finished+count_error)
        aborts=count_abort/(count_finished+count_error)
        dfErrorDataDaily.loc[str(wk), '% Errors'] = errors
        dfErrorDataDaily.loc[str(wk), '% Aborts'] = aborts
        dfErrorDataDaily.loc[str(wk), '% Finished/Started'] = count_finished/count_start
        dfErrorDataDaily.loc[str(wk), 'WK'] = str(df['WK'][i])        
        #newday=dbo.DBOps(str(wk), errors, aborts, count_start, '', 'GFDRS')
        #newday.get_day('07-AUG-18')
        #newday.get_all('GFDRS')
        
        count_error=0
        count_abort=0
        count_start=0
    
    dfErrorDataDaily['Succ_Rate']= se_succ_rate_summary.values 
    return dfErrorDataDaily
         
    
#concu=threading.Thread(target=df_SR, name='Thread1', args=(Unique_APP, Unique_CS, df))
#concu.start()
#concu.join()          

    
sr=df_transformation(Unique_APP, Unique_CS, df)
app_succ_rate_daily_table=df_SuccessRate_Daily(Unique_APP, Unique_CS, df, Unique_Days)
se=success_rate_general(sr)

succ_rate_summary=app_succ_rate_daily_table.loc[:,Unique_Days].mean()
error_table = error_analysis(succ_rate_summary, Unique_Days, df)

dur=(t.time()-start_time)

print('It took: ' + str(dur) + ' seconds. ')
