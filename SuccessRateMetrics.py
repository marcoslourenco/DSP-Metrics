# -*- coding: utf-8 -*-
"""
Created on Thu Dec 20 14:54:46 2018

@author: MLOURE13
"""
import pandas as pd
import time as t
import DBOperations as dbo


class SuccessRate(object):
    sid = 1
    def __init__(self, inputfile, system_name):
        self.inputfile=inputfile
        self.system_name=system_name.upper()
        # functions run
        #self.loadfile()
        #self.df_transformation()
        #self.error_analysis()
        #self.loadSuccRateDB()
        #self.load_DATA_DB()
        self.functions_run()
        self.sid=SuccessRate.sid
        SuccessRate.sid += 1

    def get_sid(self):
        return str(self.sid).zfill(3)

    def functions_run(self):
        if isinstance(self,SuccessRate):
            print(self)
            self.loadfile()
            self.df_transformation()
            self.error_analysis()
            self.loadSuccRateDB()
            self.load_DATA_DB()
        elif isinstance(self, DealerSuccRate):
            DealerSuccRate.loadfile()
            DealerSuccRate.df_transformation()
            DealerSuccRate.load_DATA_DB()

    def loadfile(self):
        print('Loaded: loadfile')
        start_time=t.time()
        self.df=pd.read_table(self.inputfile +'.txt', sep=',', header=0)
        self.df['WK']=self.df['DATEREQUESTED'].map(lambda x : x[0:9]).map(lambda x: pd.datetime.strptime(x,'%d-%b-%y')).map(lambda x: pd.datetime.isocalendar(x)[1])
        self.df['Date']=self.df['DATEREQUESTED'].map(lambda x : x[0:9])#.map(lambda x: pd.datetime.strptime(x,'%d-%b-%y'))
        self.df['Time']=self.df['DATEREQUESTED'].map(lambda x: x[9:18]).map(lambda x: x.replace('.',':')).map(lambda x: x.strip())
        self.df.drop('DATEREQUESTED', axis=1, inplace=True)
        self.df['Country']=self.df['DEALERCODE']
        self.df['Country']=self.df['Country'].fillna('NaN')
        self.df['Country']=self.df['Country'].map(lambda x: x[0:3])

        self.Unique_APP=self.df['APPID'].drop_duplicates(keep='first')
        self.Unique_CS=self.df['CURRENTSTATE'].drop_duplicates(keep='first')
        self.Unique_Days=self.df['Date'].drop_duplicates(keep='first')
        #self.Unique_Weeks=self.df['WK'].drop_duplicates(keep='first')
        self.UniqueVINs=self.df['VIN'].drop_duplicates(keep='first')
        self.UniqueDealers=self.df['DEALERCODE'].drop_duplicates(keep='first')
        self.UniqueCountry=self.df['Country'].drop_duplicates(keep='first')

        print('Finished: loadfile')
        dur=(t.time()-start_time)
        print('It took : ' + str(dur))
        return self.df

    def get_UniqueDays(self):
        return self.Unique_Days

    def succ_rate(self):
        if self.app_finished_count>0 and self.app_error_count>=0:
            return self.app_finished_count/(self.app_finished_count+self.app_error_count+self.app_abort_count)
        elif self.app_finished_count==0 and self.app_error_count==0:
            return float('NaN')
        else:
            return 0

    def succ_rate_daily(self):
        if self.finished_count>0 and self.error_count>=0:
            return self.finished_count/(self.finished_count+self.error_count+self.abort_count)
        elif self.finished_count==0 and self.error_count==0:
            return float('NaN')
        else:
            return 0

    def df_transformation(self):
        start_time=t.time()
        self.app_count=0
        self.app_error_count=0
        self.app_finished_count=0
        self.app_start_count=0
        self.app_abort_count=0

        print('Loading: df_transformation')
        error_state=['ERROR', 'DOWNLOAD_APPLICATION_FAILURE', 'COMMS_ERROR','DOWNLOAD_JNLP_FAILURE']
        succe_state=['FINISHED', 'DOWNLOAD_APPLICATION_SUCCESS','INDICTED', 'DOWNLOAD_JNLP_SUCCESS']
        start_state=['REQUESTED_START','REQUESTED_DOWNLOAD','DOWNLOAD_JNLP_REQUESTED', 'REQUESTED']
        abort_state=['ABORTED']
        self.dfData = pd.DataFrame(columns=['APP Count','Start Count','Success Count', 'Error Count', 'Abort Count'])

        for app in self.Unique_APP:
            for i in range(len(self.df)):
                if self.df['APPID'][i] == app:
                    if str(self.df['CURRENTSTATE'][i]) in error_state:
                        self.app_error_count+= 1
                    elif str(self.df['CURRENTSTATE'][i]) in succe_state:
                        self.app_finished_count+=1
                    elif str(self.df['CURRENTSTATE'][i]) in start_state:
                        self.app_start_count+=1
                    elif str(self.df['CURRENTSTATE'][i]) in abort_state:
                        self.app_abort_count+=1

                    self.app_count=self.app_count+1
                #print(app, app_count, error_count, suc_count, start_count, abort_count)
            self.dfData.loc[str(app), 'APP Count'] = int(self.app_count)
            self.dfData.loc[str(app), 'Start Count'] = int(self.app_start_count)
            self.dfData.loc[str(app), 'Success Count'] = int(self.app_finished_count)
            self.dfData.loc[str(app), 'Error Count'] = int(self.app_error_count)
            self.dfData.loc[str(app), 'Abort Count'] = int(self.app_abort_count)
            self.dfData.loc[str(app), 'Success Rate']= float(self.succ_rate())
            #### Test
            self.dfData.loc[str(app), 'Date']= str(self.df['Date'][i])

            #print(dfData)
            self.app_count=0
            self.app_error_count=0
            self.app_finished_count=0
            self.app_start_count=0
            self.app_abort_count=0
        dur=(t.time()-start_time)
        print('Finished: df_transformation')
        print('It took : ' + str(dur))
        return self.dfData

    def df_SuccessRate_Daily(self):

        start_time=t.time()
        self.error_count=0
        self.finished_count=0
        self.start_count=0
        self.abort_count=0

        print('Loading: df_SuccessRate_Daily')
        error_state = ['ERROR', 'DOWNLOAD_APPLICATION_FAILURE', 'COMMS_ERROR','DOWNLOAD_JNLP_FAILURE']
        succe_state = ['FINISHED', 'DOWNLOAD_APPLICATION_SUCCESS','INDICTED', 'DOWNLOAD_JNLP_SUCCESS']
        start_state = ['REQUESTED_START','REQUESTED_DOWNLOAD','DOWNLOAD_JNLP_REQUESTED', 'REQUESTED']
        abort_state = ['ABORTED']
        self.dfDataWeekly = pd.DataFrame(columns=self.Unique_Days)
        #dfDataErrorAbort=pd.DataFrame(columns=UniqueWeeks)

        for wk in self.Unique_Days:
            for app in self.Unique_APP:
                for i in range(len(self.df)):
                    if self.df['APPID'][i] == app:
                        if str(self.df['Date'][i])==str(wk) and str(self.df['CURRENTSTATE'][i]) in error_state:
                            self.error_count+= 1
                        elif str(self.df['Date'][i])==str(wk) and str(self.df['CURRENTSTATE'][i]) in succe_state:
                            self.finished_count+=1
                        elif str(self.df['Date'][i])==str(wk) and str(self.df['CURRENTSTATE'][i]) in start_state:
                            self.start_count+=1
                        elif str(self.df['Date'][i])==str(wk) and str(self.df['CURRENTSTATE'][i]) in abort_state:
                            self.abort_count+=1

        #print(app, app_count, error_count, suc_count, start_count, abort_count)
                self.dfDataWeekly.loc[str(app), str(wk)] = float(self.succ_rate_daily())
                #app_load=dbo.APP_Rate_DBOps(str(app),str(wk),float(self.succ_rate_daily()), self.system_name)
                #app_load.get_all()
                #print(app, wk)
                self.error_count=0
                self.finished_count=0
                self.start_count=0
                self.abort_count=0
                dur=(t.time()-start_time)
        print('It took : ' + str(dur))
        print('Finished: df_SuccessRate_Daily')
        self.dfDataWeekly.to_csv('ETIS_App_Daily_SR.txt',sep=',')
        print('File: App_Daily_SR.txt produced ...')
        return self.dfDataWeekly

    def success_rate_general(self):
        self.sorted_sr=self.df.sort_values(['APP Count', 'Start Count'], ascending=False)
        self.sum_app_count=self.df['APP Count'].sum()
        #print(sum_app_count)
        return self.sorted_sr

    def error_analysis(self):
        print('Loading: error_analysis')
        start_time=t.time()
        self.dfErrorDataDaily = pd.DataFrame(columns=['% Errors', '% Aborts'])
        error_state=['ERROR', 'DOWNLOAD_APPLICATION_FAILURE', 'COMMS_ERROR','DOWNLOAD_JNLP_FAILURE']
        abort_state=['ABORTED']
        start_state= ['REQUESTED_START','REQUESTED_DOWNLOAD','DOWNLOAD_JNLP_REQUESTED','REQUESTED']
        finished_state=['FINISHED', 'DOWNLOAD_APPLICATION_SUCCESS','INDICTED', 'DOWNLOAD_JNLP_SUCCESS']

        count_error_daily=0
        count_abort_daily=0
        count_start_daily=0
        count_finished_daily=0

        for wk in self.Unique_Days:
            for i in range(len(self.df)):
                if str(wk)==str(self.df['Date'][i]) and str(self.df['CURRENTSTATE'][i]) in error_state:
                    count_error_daily+=1
                elif str(wk)==str(self.df['Date'][i]) and str(self.df['CURRENTSTATE'][i]) in abort_state:
                    count_abort_daily+=1
                elif str(wk)==str(self.df['Date'][i]) and str(self.df['CURRENTSTATE'][i]) in start_state:
                    count_start_daily+=1
                elif str(wk)==str(self.df['Date'][i]) and str(self.df['CURRENTSTATE'][i]) in finished_state:
                    count_finished_daily+=1
            errors_daily=count_error_daily/(count_finished_daily+count_error_daily+count_abort_daily)
            aborts_daily=count_abort_daily/(count_finished_daily+count_error_daily+count_abort_daily)
            self.dfErrorDataDaily.loc[str(wk), '% Errors'] = errors_daily
            self.dfErrorDataDaily.loc[str(wk), '% Aborts'] = aborts_daily
            self.dfErrorDataDaily.loc[str(wk), '% Finished/Started'] = (count_finished_daily/count_start_daily)
            self.dfErrorDataDaily.loc[str(wk), 'WK'] = str(self.df['WK'][i])
            month_year=str(wk)
            month_year=month_year[3:9]
            self.dfErrorDataDaily.loc[str(wk), 'Month_Year'] = month_year
            #self.dfErrorDataDaily.loc[str(wk), 'WK'] = str(self.df['WK'][i])
            #month_year=str(self.df['Date'][i])
            #month_year=month_year[3:9]
            dbo.Volume_DB_Ops(str(self.system_name),str(wk), len(self.UniqueVINs), len(self.UniqueDealers))
            count_error_daily=0
            count_abort_daily=0
            count_start_daily=0

        app_succ_rate_daily_table=self.df_SuccessRate_Daily()
        Unique_Days=self.get_UniqueDays()
        succ_rate_summary=app_succ_rate_daily_table.loc[:,Unique_Days].mean()
        self.dfErrorDataDaily['Succ_Rate']= succ_rate_summary.values
        succ_rate_summary=app_succ_rate_daily_table.loc[:,Unique_Days].std()
        self.dfErrorDataDaily['Stand_Deviation']= succ_rate_summary.values
        succ_rate_summary=app_succ_rate_daily_table.loc[:,Unique_Days].median()
        self.dfErrorDataDaily['Median']= succ_rate_summary.values
        succ_rate_summary=app_succ_rate_daily_table.loc[:,Unique_Days].skew()
        self.dfErrorDataDaily['Skew']= succ_rate_summary.values
        succ_rate_summary=app_succ_rate_daily_table.loc[:,Unique_Days].kurt()
        self.dfErrorDataDaily['Kurtosis']= succ_rate_summary.values

        succ_rate_summary=app_succ_rate_daily_table.loc[:,Unique_Days].var()
        self.dfErrorDataDaily['Unbiased_Var']= succ_rate_summary.values

        succ_rate_summary=app_succ_rate_daily_table.loc[:,Unique_Days]
        self.dfErrorDataDaily['System']= self.system_name
        #dbo.Volume_DB_Ops(str(self.system_name),str(wk), len(self.UniqueVINs), len(self.UniqueDealers))

        print(self.dfErrorDataDaily)
        print('Finished: error_analysis')
        dur=(t.time()-start_time)
        print('It took : ' + str(dur))
        return self.dfErrorDataDaily

    def loadSuccRateDB(self):
        print('Loading: loadSuccRateDB')
        start_time=t.time()
        self.error_summ = self.dfErrorDataDaily
        for index, column in self.error_summ.iterrows():
            #print(index, *column)
            dbo.Succ_Rate_DBOps(index, *column) #####newday=dbo.Succ_Rate_DBOps(index, *column)
            #'newday.get_day('07-AUG-18')
            #newday.get_all()
        print('Finished: loadSuccRateDB')
        dur=(t.time()-start_time)
        print('It took : ' + str(dur))
        return print('Success Rates loaded to DB')

    def load_DATA_DB(self):
        print('Loaded: loadAPP_DATA_DB')
        start_time=t.time()
        self.app_summ = self.dfData
        for index, column in self.app_summ.iterrows():
            #print(index, *column)
            dbo.APP_DB_OPS(index, *column, str(self.system_name))
            #'newday.get_day('07-AUG-18')

        print('Finished: loadAPP_DATA_DB')
        dur=(t.time()-start_time)
        print('It took : ' + str(dur))
        return print('App data loaded to DB')

class DealerSuccRate(SuccessRate):
    sid = 1
    def __init__(self, inputfile, system_name):
        super().__init__(inputfile, system_name)
        #self.loadfile()
        #self.df_transformation()
        #self.load_DEALER_DATA_DB()

    def df_transformation(self):
        start_time=t.time()
        self.app_count=0
        self.app_error_count=0
        self.app_finished_count=0
        self.app_start_count=0
        self.app_abort_count=0
        DealerSuccRate.sid += 1

        print('Loaded: df_transformation for Dealers')
        error_state=['ERROR', 'DOWNLOAD_APPLICATION_FAILURE', 'COMMS_ERROR','DOWNLOAD_JNLP_FAILURE']
        succe_state=['FINISHED', 'DOWNLOAD_APPLICATION_SUCCESS','INDICTED', 'DOWNLOAD_JNLP_SUCCESS']
        start_state=['REQUESTED_START','REQUESTED_DOWNLOAD','DOWNLOAD_JNLP_REQUESTED', 'REQUESTED']
        abort_state=['ABORTED']
        self.dfData = pd.DataFrame(columns=['APP Count','Start Count','Success Count', 'Error Count', 'Abort Count'])

        for dealer in self.UniqueDealers:
            for i in range(len(self.df)):
                if self.df['DEALERCODE'][i] == dealer:
                    if str(self.df['CURRENTSTATE'][i]) in error_state:
                        self.app_error_count+= 1
                    elif str(self.df['CURRENTSTATE'][i]) in succe_state:
                        self.app_finished_count+=1
                    elif str(self.df['CURRENTSTATE'][i]) in start_state:
                        self.app_start_count+=1
                    elif str(self.df['CURRENTSTATE'][i]) in abort_state:
                        self.app_abort_count+=1

                    self.app_count=self.app_count+1
                #print(app, app_count, error_count, suc_count, start_count, abort_count)
            self.dfData.loc[str(dealer), 'APP Count'] = int(self.app_count)
            self.dfData.loc[str(dealer), 'Start Count'] = int(self.app_start_count)
            self.dfData.loc[str(dealer), 'Success Count'] = int(self.app_finished_count)
            self.dfData.loc[str(dealer), 'Error Count'] = int(self.app_error_count)
            self.dfData.loc[str(dealer), 'Abort Count'] = int(self.app_abort_count)
            self.dfData.loc[str(dealer), 'Success Rate']= float(self.succ_rate())
            #### Test
            self.dfData.loc[str(dealer), 'Date']= str(self.df['Date'][i])

            #print(dfData)
            self.app_count=0
            self.app_error_count=0
            self.app_finished_count=0
            self.app_start_count=0
            self.app_abort_count=0
        dur=(t.time()-start_time)
        print('Finished: df_transformation for Dealers')
        print('It took : ' + str(dur))
        return self.dfData

    def load_DATA_DB(self):
        print('Loaded: load_DEALER_DATA_DB')
        start_time=t.time()
        self.app_summ = self.dfData
        if self.system_name=='GFDRS':
            for index, column in self.app_summ.iterrows():
                #print(index, *column)
                dbo.DEALER_DB_OPS(index, *column, str(self.system_name))
                #'newday.get_day('07-AUG-18')
            print('Finished: load_DEALER_DATA_DB')
            dur=(t.time()-start_time)
            print('It took : ' + str(dur))
        else:
            print('ETIS: No Dealer data for ETIS')
        return print('load_DEALER_DATA_DB')

    def error_analysis(self):
        print('Dealer: error_analysis')
    def df_SuccessRate_Daily(self):
        print('Dealer: df_SuccessRate_Daily')
    def loadSuccRateDB(self):
        print('Dealer: loadSuccRateDB')

class CountrySuccRate(DealerSuccRate):
    sid = 1
    def __init__(self, inputfile, system_name):
        super().__init__(inputfile, system_name)
        #self.loadfile()
        #self.df_transformation()
        #self.load_DEALER_DATA_DB()

    def df_transformation(self):
        start_time=t.time()
        self.app_count=0
        self.app_error_count=0
        self.app_finished_count=0
        self.app_start_count=0
        self.app_abort_count=0
        DealerSuccRate.sid += 1

        print('Loaded: df_transformation for Country')
        error_state=['ERROR', 'DOWNLOAD_APPLICATION_FAILURE', 'COMMS_ERROR','DOWNLOAD_JNLP_FAILURE']
        succe_state=['FINISHED', 'DOWNLOAD_APPLICATION_SUCCESS','INDICTED', 'DOWNLOAD_JNLP_SUCCESS']
        start_state=['REQUESTED_START','REQUESTED_DOWNLOAD','DOWNLOAD_JNLP_REQUESTED', 'REQUESTED']
        abort_state=['ABORTED']
        self.dfData = pd.DataFrame(columns=['APP Count','Start Count','Success Count', 'Error Count', 'Abort Count'])

        for country in self.UniqueCountry:
            for i in range(len(self.df)):
                if self.df['Country'][i] == country:
                    if str(self.df['CURRENTSTATE'][i]) in error_state:
                        self.app_error_count+= 1
                    elif str(self.df['CURRENTSTATE'][i]) in succe_state:
                        self.app_finished_count+=1
                    elif str(self.df['CURRENTSTATE'][i]) in start_state:
                        self.app_start_count+=1
                    elif str(self.df['CURRENTSTATE'][i]) in abort_state:
                        self.app_abort_count+=1

                    self.app_count=self.app_count+1
                #print(app, app_count, error_count, suc_count, start_count, abort_count)
            self.dfData.loc[str(country), 'APP Count'] = int(self.app_count)
            self.dfData.loc[str(country), 'Start Count'] = int(self.app_start_count)
            self.dfData.loc[str(country), 'Success Count'] = int(self.app_finished_count)
            self.dfData.loc[str(country), 'Error Count'] = int(self.app_error_count)
            self.dfData.loc[str(country), 'Abort Count'] = int(self.app_abort_count)
            self.dfData.loc[str(country), 'Success Rate']= float(self.succ_rate())
            #### Test
            self.dfData.loc[str(country), 'Date']= str(self.df['Date'][i])

            #print(dfData)
            self.app_count=0
            self.app_error_count=0
            self.app_finished_count=0
            self.app_start_count=0
            self.app_abort_count=0
        dur=(t.time()-start_time)
        print('Finished: df_transformation for Country')
        print('It took : ' + str(dur))
        return self.dfData

    def load_DATA_DB(self):
        print('Loaded: load_COUNTRY_DATA_DB')
        start_time=t.time()
        self.app_summ = self.dfData
        if self.system_name=='GFDRS':
            for index, column in self.app_summ.iterrows():
                #print(index, *column)
                dbo.COUNTRY_DB_OPS(index, *column, str(self.system_name))
                #'newday.get_day('07-AUG-18')
            print('Finished: load_COUNTRY_DATA_DB')
            dur=(t.time()-start_time)
            print('It took : ' + str(dur))
        else:
            print('ETIS: No Country data for ETIS')
        return print('load_COUNTRY_DATA_DB')

gfdrs=SuccessRate('GFDRS_DATA', 'GFDRS')
gfdrs_dealers=DealerSuccRate('GFDRS_DATA', 'GFDRS')
gfdrs_country=CountrySuccRate('GFDRS_DATA', 'GFDRS')

