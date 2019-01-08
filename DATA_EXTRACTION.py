# -*- coding: utf-8 -*-
"""
Created on Sat Dec 22 21:27:13 2018

@author: MLOURE13
"""

import sqlite3 as sq


conn = sq.connect('GFDRS_SUCC_RATE.db')
with conn:
    c=conn.cursor()
    #c.execute('''SELECT * FROM GFDRS_SUCC_RATE_DAILY_SUMMARY_TABLE WHERE date=:date''',{'date':'07-AUG-18'})
    c.execute('''SELECT * FROM GFDRS_SUCC_RATE_DAILY_SUMMARY_TABLE''')
    print(c.fetchall())

#conn = sq.connect('ETIS_SUCC_RATE.db')
#conn = sq.connect('GFDRS_SR.db')
#c=conn.cursor()
#c.execute('''CREATE TABLE ETIS_SUCC_RATE_DAILY_SUMMARY_TABLE (date text, errors real, aborts real, start_finished integer, wk integer, month_year text, succ_rate real, stand_dev real, median_r real, skew_r real, kurt_r real, unb_var real, system_name text  )''')
#conn.commit()
