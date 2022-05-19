import pymysql
import numpy as np
import datetime
import os
import pandas as pd
import copy

#connect db
db = pymysql.connect(host='localhost',user='root',passwd='a23187',port=3306,db='ic',charset='utf8')
cursor = db.cursor()

# import data from table 1min
# only select 01 02 03 06
cursor.execute("select *  from 1min where symbol like '%01' or symbol like '%02' or symbol like '%03' or symbol like '%06'")
data = cursor.fetchall()
db.close()


# tradetime
time_set = set()
for i in data:
    time_set.add(i[0])

time_list = list(time_set)

# mk dict data
#dict_data = dict.fromkeys(time_list,[]) all keys id is same !!!
dict_data = dict.fromkeys(time_list)
for key in time_list:
    dict_data[key] = []

for i in data:
    # symbol,continue_stat,open,close,high,low,position,volume
    dict_data[i[0]].append(i[1:-1])

# remove bad data
rm_time = set()
for key in dict_data.keys():
    if(len(dict_data[key])!=4):
        rm_time.add(key)

for key in rm_time:
    del dict_data[key]

valid_time = time_set - rm_time

# save dict_data
np.save('01begin.npy',dict_data)
