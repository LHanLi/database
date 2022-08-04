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
cursor.execute("select *  from 1min")
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
    dict_data[i[0]].append(list(i[1:-1]))
    
# remove bad data
rm_time = set()
for key in dict_data.keys():
    if(len(dict_data[key])!=4):
        rm_time.add(key)

for key in rm_time:
    print(key)
    del dict_data[key]

# continue stat
for t in time_list:
    symbols = [i[0] for i in dict_data[t]]
    symbols.sort()
    for i in dict_data[t]:
        if(i[0] == symbols[0]):
            i[1] = '当月连续'
        if(i[0] == symbols[1]):
            i[1] = '下月连续'
        if(i[0] == symbols[2]):
            i[1] = '下季连续'
        if(i[0] == symbols[3]):
            i[1] = '隔季连续'

# save dict_data
np.save('ic_ok.npy',dict_data)
