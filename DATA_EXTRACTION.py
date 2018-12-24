# -*- coding: utf-8 -*-
"""
Created on Sat Dec 22 21:27:13 2018

@author: MLOURE13
"""

import sqlite3 as sq

conn = sq.connect('GFDRS_SR.db')
with conn:
    c=conn.cursor()
    c.execute('''SELECT * FROM GFDRS_SUCC_RATE_DAILY_SUMMARY_TABLE WHERE date=:date''',{'date':'15-OCT-18'})
    print(c.fetchall())
