# -*- coding: utf-8 -*-
"""
Created on Sun Nov 25 14:48:03 2018

@author: MLOURE13
"""
import pandas as pd
import numpy as np

class DataAggregation:
    def __init__(self, dfWeeks, dfApps, dfCS, dfDays, dfCompleteSet ):
        self.dfWeeks=dfWeeks
        self.dfApps=dfApps
        self.dfCS=dfCS
        self.dfDays=dfDays
        self.dfCompleteSet=dfCompleteSet        
        example=['haloooo']
        example1=['tudo bem?']
        example3=example+example1
        print(example3)
    
    def hj(self):
        self.dfWeeks=1
        return self.dfWeeks
        
    
df=pd.read_table('class.txt', sep=',', header=0)
df['WK']=df['DATEREQUESTED'].map(lambda x : x[0:9]).map(lambda x: pd.datetime.strptime(x,'%d-%b-%y')).map(lambda x: pd.datetime.isocalendar(x)[1])
df['Date']=df['DATEREQUESTED'].map(lambda x : x[0:9]).map(lambda x: pd.datetime.strptime(x,'%d-%b-%y'))
df['Time']=df['DATEREQUESTED'].map(lambda x: x[9:18]).map(lambda x: x.replace('.',':')).map(lambda x: x.strip())
df.drop('DATEREQUESTED', axis=1, inplace=True)
Unique_APP=df['APPID'].drop_duplicates(keep='first')
Unique_CS=df['CURRENTSTATE'].drop_duplicates(keep='first')
Unique_Days=df['Date'].drop_duplicates(keep='first')
Unique_Weeks=df['WK'].drop_duplicates(keep='first')

#print(example)

gf=DataAggregation(Unique_Weeks, Unique_APP, Unique_CS, Unique_Days, df)
print(gf.hj())

#for i in range(len(Unique_Weeks)):
#Unique_Weeks.to_csv('sian_unique_weeks.txt', sep='|', index=False)
#np.savetxt('sian_unique_weeks.txt', Unique_CS, delimiter='|')
        
    #print(Unique_Weeks[i])
    #print(df['WK'][i])
    #print(df['APPID'][i])