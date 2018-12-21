# -*- coding: utf-8 -*-
"""
Created on Wed Dec 19 15:19:21 2018

@author: MLOURE13
"""
import sqlite3 as sq

class DBOps(object):
    def __init__(self,date_req, errors, aborts, starts, succ_rate, system_name='GFDRS'):
        self.date_req=date_req
        self.errors=errors
        self.aborts=aborts
        self.starts=starts
        self.succ_rate=succ_rate
        
        if system_name.upper()=='GFDRS':
            self.conn = sq.connect(':memory:')
            c=self.conn.cursor()
            c.execute('''CREATE TABLE GFDRS_SUCC_RATE_DAILY_SUMMARY_TABLE (date text, errors real, aborts real, starts integer, succ_rate real)''')
            c.execute('''INSERT INTO GFDRS_SUCC_RATE_DAILY_SUMMARY_TABLE VALUES (:date, :errors, :aborts, :starts, :succ_rate )''',{'date': self.date_req, 'errors': self.errors, 'aborts': self.aborts, 'starts': self.starts, 'succ_rate': self.succ_rate})
            self.conn.commit()
            print('GFDRS Table and DB created')   
            #c.execute('''SELECT * FROM error_table WHERE date=:date''',{'date': self.date_req})
            #print(c.fetchall()) 
        elif system_name.upper()=='ETIS':
            self.conn = sq.connect(':memory:')
            c=self.conn.cursor()
            c.execute('''CREATE TABLE ETIS_SUCC_RATE_DAILY_SUMMARY_TABLE (date text, errors real, aborts real, starts integer, succ_rate real)''')
            c.execute('''INSERT INTO ETIS_SUCC_RATE_DAILY_SUMMARY_TABLE VALUES (:date, :errors, :aborts, :starts, :succ_rate )''',{'date': self.date_req, 'errors': self.errors, 'aborts': self.aborts, 'starts': self.starts, 'succ_rate': self.succ_rate})
            self.conn.commit()
            print('ETIS Table and DB created')   
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
            if system_name.upper()=='GFDRS':
                c=self.conn.cursor()
                c.execute('''SELECT * FROM GFDRS_SUCC_RATE_DAILY_SUMMARY_TABLE''')
                print(c.fetchall())
                #c.execute('''SELECT * FROM error_table WHERE date=:date''',{'date': self.date_req})
                return c.fetchall()
            elif system_name.upper()=='ETIS':
                c=self.conn.cursor()
                c.execute('''SELECT * FROM ETIS_SUCC_RATE_DAILY_SUMMARY_TABLE''')
                print(c.fetchall())
                #c.execute('''SELECT * FROM error_table WHERE date=:date''',{'date': self.date_req})
                return c.fetchall()
                
        
           
               