# -*- coding: utf-8 -*-
"""
Created on Wed Dec 19 15:19:21 2018

@author: MLOURE13
"""
import sqlite3 as sq

class DBOps(object):
    def __init__(self,date_req, errors, aborts, starts, succ_rate):
        self.date_req=date_req
        self.errors=errors
        self.aborts=aborts
        self.starts=starts
        self.succ_rate=succ_rate
        
        self.conn = sq.connect(':memory:')
        c=self.conn.cursor()
        c.execute('''CREATE TABLE error_table (date text, errors real, aborts real, starts integer, succ_rate real)''')
        c.execute('''INSERT INTO error_table VALUES (:date, :errors, :aborts, :starts, :succ_rate )''',{'date': self.date_req, 'errors': self.errors, 'aborts': self.aborts, 'starts': self.starts, 'succ_rate': self.succ_rate})
        self.conn.commit()
        print('table and DB created')   
        #c.execute('''SELECT * FROM error_table WHERE date=:date''',{'date': self.date_req})
        #print(c.fetchall())           
       
    def get_day(self, date_re):
        with self.conn:
            c=self.conn.cursor()
            c.execute('''SELECT * FROM error_table WHERE date=:date''',{'date': date_re})
            print(c.fetchall())
            #c.execute('''SELECT * FROM error_table WHERE date=:date''',{'date': self.date_req})
            return c.fetchall()
        
    def get_all(self):
        with self.conn:
            c=self.conn.cursor()
            c.execute('''SELECT * FROM error_table''')
            print(c.fetchall())
            #c.execute('''SELECT * FROM error_table WHERE date=:date''',{'date': self.date_req})
            return c.fetchall()
        
    
               