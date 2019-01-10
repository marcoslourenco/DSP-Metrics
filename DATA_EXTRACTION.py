# -*- coding: utf-8 -*-
"""
Created on Sat Dec 22 21:27:13 2018

@author: MLOURE13
"""

import sqlite3 as sq


conn = sq.connect('GFDRS_VOL.db')
with conn:
    c=conn.cursor()
    #c.execute('''SELECT * FROM GFDRS_SUCC_RATE_DAILY_SUMMARY_TABLE WHERE date=:date''',{'date':'07-AUG-18'})
    c.execute('''SELECT * FROM GFDRS_VOL_DAILY_TABLE''')
    print(c.fetchall())

#conn = sq.connect('ETIS_SUCC_RATE.db')
#conn = sq.connect('ETIS_VOL.db')
#c=conn.cursor()
#c.execute('''CREATE TABLE ETIS_APP_DAILY_TABLE (appid text, app_count integer, start_count integer, success_count integer, error_count integer, abort_count integer, success_rate float, date_app text, system_name text   )''')
#conn.commit()
