import pymysql
import numpy
import datetime
import os

# read data in data/  and insert to database ic table 1min







#connect db
db = pymysql.connect(host='localhost',user='root',passwd='a23187',port=3306,db='ic',charset='utf8')
cursor = db.cursor()

'''
# create table ic01-12
tables = ["IC%02d"%(i+1) for i in range(12)]
for i in tables:
#    drop_table = "DROP TABLE IF EXISTS %s"%i
    cursor.execute(drop_table)
    create_table = "CREATE TABLE %s (date INT,open float, close float, high float, low float)"%i
    cursor.execute(create_table)
'''
#create table 1mim
drop_table = "DROP TABLE IF EXISTS 1min"
cursor.excete(drop_table)
create_table = "CREATE TABLE 1min (tradetime datetime,symbol char(10), continue_stat varchar(20), open float, close float, high float, low float,position float,volume float,insert_time datetime)"
cursor.execute(create_table)

wants = ["IC%02d"%(i+1) for i in range(12)]


#read source data file
os.chdir('data')
file_list = [f for f in os.listdir() if os.path.isdir(f)]

for f in file_list:
    os.chdir(f)
    temp = [f for f in os.listdir() if 'MIN01' in f]
    name = temp[0]
#    file = open(name,'r',encoding='utf-8')

    try:
        file = open(name,'r')
        file.readline().strip('\n').split(',')
    except:
        file = open(name,'r',encoding='utf-8')
    print(name)
#    data_field = file.readline().strip('\n').split(',')
    need_field = [1,2,15,3,4,5,6,9,10]                                      # 0 is date
#    print("saving",[data_field[i] for i in need_field])
    number = 0
    for line in file:
        temp = line.strip('\n').split(',')
        tag = temp[2][:2]+temp[2][-2:]                       #trading product  2 is symbol
        if(tag in wants and temp[2][2].isdigit()):          #exclude icm
            temp = [temp[i] for i in need_field]
            insert = "INSERT INTO 1min (tradetime,symbol,continue_stat,open,close,high,low,position,volume,insert_time) VALUES ('%s','%s','%s',%.4f,%.4f,%.4f,%.4f,%.4f,%.4f,'%s')"%(temp[0],temp[1],temp[2],float(temp[3]),float(temp[4]),float(temp[5]),float(temp[6]),float(temp[7]),float(temp[8]),datetime.datetime.now())
#            print(insert)
            number += 1
            cursor.execute(insert)
            db.commit()
    file.close()
    print('success: %d '%number)
    os.chdir('../')


'''
# show table 1min
cursor.execute("select * from 1min")
data = cursor.fetchone()
print(data)
'''


db.close()
