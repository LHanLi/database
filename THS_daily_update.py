from iFinDPy import *
import datetime
import pymysql
from jqdatasdk import *
import pandas as pd

# connect db
db = pymysql.connect(host='localhost',user='root',passwd='a23187',port=3306,db='THS',charset='utf8')
cursor = db.cursor()
# connect iFinD
THS_iFinDLogin('gj5212','234160')

today = datetime.date.today()
today = str(today)

# update total

# import tradedate
date = THS_Date_Query('212001','mode:1,dateType:0,period:D,dateFormat:0','2010-01-04',today)
date = date.data
date = date.split(',')
#date = [datetime.datetime.strptime(i,'%Y-%m-%d') for i in date]

# insert mysql
#drop_table = "DROP TABLE IF EXISTS trade_date"
#cursor.execute(drop_table)
#create_table = "CREATE TABLE trade_date (Date datetime,insert_time datetime)"
#cursor.execute(create_table)
date = list(reversed(date))
print('update trade_date...')
for i in date:
    isexist = "select 1 as isExist from trade_date where Date = '%s'"%(i)
    if(cursor.execute(isexist)):
        break;
    insert = "INSERT INTO trade_date (Date, insert_time) VALUES ('%s','%s')"%(i,datetime.datetime.now())
    print(i)
    cursor.execute(insert)
    db.commit()

# login joinquant
auth('15305333613','a23187')
df = get_all_securities(types=['futures'], date=None)
change_code = {'XSGE':'SHF','XDCE':'DCE','XZCE':'CZC','XINE':'SHF','CCFX':'CFE'}

# days
select = 'select * from trade_date;'
cursor.execute(select)
days = cursor.fetchall()
days = list(days)
days.sort()
days = list(reversed(days))
# insert mysql
#drop_table = "DROP TABLE IF EXISTS trading_secucode"
#cursor.execute(drop_table)
#create_table = "CREATE TABLE trading_secucode (Date datetime,THScode char(20),name varchar(30),insert_time datetime)"
#cursor.execute(create_table)

print('update trading_secucode...')
for i in days:
    today = i[0]
    print(today)
    isexist = "select 1 as isExist from trading_secucode where Date = '%s'"%(today)
    if(cursor.execute(isexist)):
        break;
    df1 = df[(df['start_date']<today)&(df['end_date']>today)]
    for j in df1.itertuples():
        code_joint = j[0]
        if('8888' in code_joint or '9999' in code_joint):
            continue
        point_loc = code_joint.rfind('.')
        code_ths = change_code[code_joint[point_loc+1:]]
        kind = code_joint[:point_loc]
        if(code_ths == 'CZC'):
            kind = kind[:2]+kind[3:]
        code_ths = kind + '.' + code_ths
        name = j[1]
        print(today,code_ths)
        insert = "INSERT INTO trading_secucode (Date, THScode, name, insert_time) VALUES ('%s','%s','%s','%s')"%(today,code_ths,name,datetime.datetime.now())
        cursor.execute(insert)
        db.commit()
        
 
'''
# security from THS
THS
# mk security table
drop_table = "DROP TABLE IF EXISTS security"
cursor.execute(drop_table)
create_table = "CREATE TABLE security (Date datetime,THScode char(20),name varchar(30),insert_time datetime)"
cursor.execute(create_table)
# import security
trade_market = ['091001','091002','091003','091004','091027']
for day in date:
    query_str = [day + ';%s;monthcontract'%i for i in trade_market]
    for i in query_str:
        print(i)
        security = THS_DP('block',i,'date:Y,thscode:Y,security_name:Y').data
        if(type(security) == pd.core.frame.DataFrame):
            for row in security.itertuples():
                insert = "INSERT INTO security (Date, THScode, name, insert_time) VALUES ('%s','%s','%s','%s')"%(getattr(row,'DATE'),getattr(row,'THSCODE'),getattr(row,'SECURITY_NAME'),datetime.datetime.now())
                cursor.execute(insert)
                db.commit()
'''

# mk HQ
#drop_table = "DROP TABLE IF EXISTS trading_daily_price"
#cursor.execute(drop_table)
#create_table = "CREATE TABLE trading_daily_price (date datetime, code char(20), preclose float, open float, high float, low float, close float, change_ratio float, volume float, amount float, open_interest float, position_change float, insert_time datetime)"
#cursor.execute(create_table)
print('update trading_daily_price...')
for i in days:
    today = i[0]
    print(today)
    isexist = "select 1 as isExist from trading_daily_price where date = '%s'"%(today)
    if(cursor.execute(isexist)):
        break;
    select = "select THScode from trading_secucode where Date = '%s';"%today
    cursor.execute(select)
    codes = cursor.fetchall()
    for j in codes:
        code = j[0]
        query_date = today.strftime('%Y-%m-%d')
        print(query_date,code)
        temp = THS_HQ(code,'preClose,open,high,low,close,changeRatio,volume,amount,openInterest,positionChange','',query_date,query_date).data
        try:
            insert = "INSERT INTO trading_daily_price (date, code,preclose,open,high,low,close,change_ratio,volume,amount,open_interest,position_change,insert_time) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"%(today,code,temp['preClose'][0],temp['open'][0],temp['high'][0],temp['low'][0],temp['close'][0],temp['changeRatio'][0],temp['volume'][0],temp['amount'][0],temp['openInterest'][0],temp['positionChange'][0],datetime.datetime.now())
            insert = insert.replace("'None'",'null')
        except:
            print(today,code,'no data')
#            insert = insert = "INSERT INTO trading_daily_price (date, code,preclose,open,high,low,close,change_ratio,volume,amount,open_interest,position_change,insert_time) VALUES ('%s','%s','null','null','null','null','null','null','null','null','null','null','null')"%(today,code)
            continue
        cursor.execute(insert)
        db.commit()

db.close()
THS_iFinDLogout()



