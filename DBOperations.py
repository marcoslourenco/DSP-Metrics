# -*- coding: utf-8 -*-
"""
Created on Wed Dec 19 15:19:21 2018

@author: MLOURE13
"""
import sqlite3 as sq

class Succ_Rate_DBOps(object):
    def __init__(self,date_req, errors, aborts, start_finished, week_req , month_year, succ_rate, stand_dev, median_r, skew_r, kurt_r, unb_var, system_name):
        self.date_req=date_req
        self.errors=errors
        self.aborts=aborts
        self.start_finished=start_finished
        self.week_req=week_req
        self.month_year=month_year
        self.succ_rate=succ_rate
        self.stand_dev=stand_dev
        self.median_r=median_r
        self.skew_r=skew_r
        self.kurt_r=kurt_r
        self.unb_var=unb_var
        self.system_name=system_name        
        
        if self.system_name.upper()=='GFDRS':
            self.conn = sq.connect('GFDRS_SUCC_RATE.db')
            #self.conn = sq.connect('GFDRS_SR.db')
            c=self.conn.cursor()
            #c.execute('''CREATE TABLE GFDRS_SUCC_RATE_DAILY_SUMMARY_TABLE (date text, errors real, aborts real, start_finished integer, wk integer, month_year text, succ_rate real, stand_dev real, median_r real, skew_r real, kurt_r real, unb_var real, system_name text  )''')
            c.execute('''INSERT INTO GFDRS_SUCC_RATE_DAILY_SUMMARY_TABLE VALUES (:date, :errors, :aborts, :start_finished, :wk, :month_year , :succ_rate, :stand_dev, :median_r, :skew_r, :kurt_r, :unb_var, :system_name)''',{'date': self.date_req, 'errors': self.errors, 
                      'aborts': self.aborts, 'start_finished': self.start_finished, 'wk': self.week_req ,'month_year': self.month_year, 'succ_rate': self.succ_rate, 'stand_dev': self.stand_dev, 'median_r': self.median_r, 'skew_r':self.skew_r, 'kurt_r':self.kurt_r, 'unb_var': self.unb_var, 'system_name':system_name })
            self.conn.commit()
            print('GFDRS Table and DB created/updated')
            
            #c.execute('''SELECT * FROM error_table WHERE date=:date''',{'date': self.date_req})
            #print(c.fetchall())
            
        elif self.system_name.upper()=='ETIS':
            #self.conn = sq.connect(':memory:')
            self.conn = sq.connect('ETIS_SUCC_RATE.db')
            c=self.conn.cursor()
            c.execute('''CREATE TABLE ETIS_SUCC_RATE_DAILY_SUMMARY_TABLE (date text, errors real, aborts real, start_finished integer, wk integer, month_year text, succ_rate real, stand_dev real, median_r real, skew_r real, kurt_r real, unb_var real, system_name text  )''')
            c.execute('''INSERT INTO ETIS_SUCC_RATE_DAILY_SUMMARY_TABLE VALUES (:date, :errors, :aborts, :start_finished, :wk, :month_year , :succ_rate, :stand_dev, :median_r, :skew_r, :kurt_r, :unb_var, :system_name)''',{'date': self.date_req, 'errors': self.errors, 
                      'aborts': self.aborts, 'start_finished': self.start_finished, 'wk': self.week_req ,'month_year': self.month_year, 'succ_rate': self.succ_rate, 'stand_dev': self.stand_dev, 'median_r': self.median_r, 'skew_r':self.skew_r, 'kurt_r':self.kurt_r, 'unb_var': self.unb_var, 'system_name':system_name })
            
            self.conn.commit()
            print('ETIS Table and DB created/updated')   
            #c.execute('''SELECT * FROM error_table WHERE date=:date''',{'date': self.date_req})
            #print(c.fetchall())
                    
       
    def update(self, suc_rate):
        with self.conn:
            c=self.conn.cursor()
            c.execute('''UPDATE error_table SET suc_rate =:succ_rate''', )
            self.conn.commit()
        
    def get_day(self, date_re):
        with self.conn:
            c=self.conn.cursor()
            c.execute('''SELECT * FROM error_table WHERE date=:date''',{'date': date_re})
            print(c.fetchall())
            #c.execute('''SELECT * FROM error_table WHERE date=:date''',{'date': self.date_req})
            return c.fetchall()
        
    def get_all(self, system_name='GFDRS'):
        with self.conn:
            if self.system_name.upper()=='GFDRS':
                c=self.conn.cursor()
                c.execute('''SELECT * FROM GFDRS_SUCC_RATE_DAILY_SUMMARY_TABLE''')
                print(c.fetchall())
                #c.execute('''SELECT * FROM error_table WHERE date=:date''',{'date': self.date_req})
                return c.fetchall()
            elif self.system_name.upper()=='ETIS':
                c=self.conn.cursor()
                c.execute('''SELECT * FROM ETIS_SUCC_RATE_DAILY_SUMMARY_TABLE''')
                print(c.fetchall())
                #c.execute('''SELECT * FROM error_table WHERE date=:date''',{'date': self.date_req})
                return c.fetchall()
            
    

class APP_Rate_DBOps(object):
    def __init__(self,app_ref, dia, suc_rate,system_name):
        self.app_ref=app_ref
        self.dia=dia
        self.suc_rate=suc_rate 
        self.system_name=system_name
               
        if self.system_name.upper()=='GFDRS':
            self.conn = sq.connect(':memory:')
            c=self.conn.cursor()
            try:
                c.execute('''CREATE TABLE GFDRS_APP_SR (date text) ''')
                #c.execute('''INSERT INTO GFDRS_APP_SR VALUES (: app_ref, :date)''', {'app_ref': self.dia , 'date': self.suc_rate })
                c.execute('''ALTER TABLE GFDRS_APP_SR ADD COLUMN self.app_ref ''')
                self.conn.commit()
                c.execute('''UPDATE GFDRS_APP_SR SET self.app_ref=self.suc_rate''' )
                #c.execute('''INSERT INTO GFDRS_APP_SR VALUES (:date, :self.app_ref) ''', { 'date': self.dia, self.app_ref: self.suc_rate })
                self.conn.commit()
                print('GFDRS TABLE' + str(self.app_ref) + 'created')   
                #c.execute('''SELECT * FROM error_table WHERE date=:date''',{'date': self.date_req})
                #print(c.fetchall())
            except:
                c.execute('''INSERT INTO GFDRS_APP_SR VALUES ( :date, :self.app_ref) ''', {'date': self.dia, self.app_ref: self.suc_rate })
                self.conn.commit()
                print('GFDRS TABLE : ' + str(self.app_ref) + ' inserted')
                
                
            
    def get_all(self, system_name='GFDRS'):
        with self.conn:
            if self.system_name.upper()=='GFDRS':
                c=self.conn.cursor()
                c.execute('''SELECT * FROM GFDRS_APP_SR''')
                print(c.fetchall())
                #c.execute('''SELECT * FROM error_table WHERE date=:date''',{'date': self.date_req})
                return c.fetchall()
        
                
        
           
               