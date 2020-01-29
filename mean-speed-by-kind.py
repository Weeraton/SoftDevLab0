#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul  3 17:24:59 2018

@author: ratha
"""

import MySQLdb
import numpy as np
import pandas as pd
import math
from datetime import date, datetime, timedelta
import os
from dateutil import parser
import logging
#import csv
#import codecs
#import sqlite3

################ INIT

dbpass=""

day0 = date.today() - timedelta(1)
xday0 = day0.strftime('%Y-%m-%d')


logging.basicConfig(filename="/root/gps34/log/mean-speed-by-kind-{0}.log".format(day0.strftime('%Y-%m-%d')),level=logging.DEBUG)
#logging.basicConfig(filename="log/mean-speed-by-kind-{0}.log".format(day0.strftime('%Y-%m-%d')),level=logging.DEBUG)

########## MQTT

def logmsg(msg):
    try:
        print(msg)
    except:
        pass
    
    try:
        logging.info(msg)
    except:
        pass
    

############### FUNCTIONS

def type_desc(code):
    tx = {"1":"รถโดยสารประจำทาง" ,
        "3":"รถโดยสารไม่ประจำทาง" ,
        "4":"รถโดยสารส่วนบุคคล" ,
        "5":"บรรทุกส่วนบุคคล" ,
        "6":"รถบรรทุกไม่ประจำทาง" ,
        "7":"รถบรรทุกไม่ประจำทาง" ,
        "8":"รถบรรทุกส่วนบุคคล" ,
        "9":"รถบรรทุกส่วนบุคคล" }  
    
    if code in tx:
        return tx[code]
    else:
        return ""
        
def kind_desc(code):
    tx = { "01":"รถโดยสารปรับอากาศพิเศษ" ,
        "02":"รถโดยสารปรับอากาศชั้น2" ,
        "03":"รถโดยสารธรรมดา" ,
        "04":"รถโดยสาร 2 ชั้นปรับอากาศพิเศษ" ,
        "11":"กระบะบรรทุก" ,
        "12":"ตู้บรรทุก" ,
        "13":"บรรทุกของเหลว" ,
        "14":"บรรทุกวัตถุอันตราย" ,
        "15":"บรรทุกเฉพาะกิจ",
        "19":"ลากจูง" ,
        "95":"รถโดยสารพ่วง" ,
        "96":"รถโดยสารระหว่างประเทศ" ,
        "97":"รถโดยสารเฉพาะกิจ" ,
        "99":"ไม่สามารถระบุได้" 
        }
    
    if code in tx:
        return tx[code]
    else:
        return "" 
    
    
############### DB
        

db = MySQLdb.connect(host="10.103.0.51",    # pma.dlt.transcodeglobal.com
                     user="gps-behv",         # your username
                     passwd=dbpass,  # your password
                     db="gps",        # name of the data base
                     charset='utf8')

############## GET CAR INFO
#xlog = pd.DataFrame(columns=["unit_id","ts","lat","lon", "speed"])



sql = "SELECT stat_date, type_code, kind_code, round(avg(avg_speed),0) as avg_speed, round(avg(sd_speed),2) as sd_speed FROM  stat_daily_veh_speed where stat_date='"+xday0+"' GROUP BY stat_date, type_code, kind_code"
df = pd.read_sql_query(sql, db)

df["type_desc"] = df["type_code"].apply(type_desc)
df["kind_desc"] = df["kind_code"].apply(kind_desc)

df = df[["stat_date","type_code","type_desc","kind_code","kind_desc","avg_speed","sd_speed"]]

df.to_csv("/var/www/html/vehspeed/meanspeed/veh_speed.csv", index=False, mode='a', header=False, encoding='utf-8-sig')
