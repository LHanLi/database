from iFinDPy import *
import datetime
import pymysql
from jqdatasdk import *
import pandas as pd
import argparse

# read date from terminal     '2022-05-30'
parser = argparse.ArgumentParser()
parser.add_argument('date', type=str, help='input a date')
date = parser.parse_args().date

#date = datetime.date.today()

# There are three table in THS database, trade_date, trading_security, trading_daily_price;
def daily_commodity_update(date='2022-05-30'):
    # connect db
    db = pymysql.connect(host='localhost',user='root',passwd='a23187',port=3306,db='THS',charset='utf8')
    cursor = db.cursor()

    # connect iFinD
    #THS_iFinDLogin('gj5212','234160')
    THS_iFinDLogin('ifind6521','751676')


    print('update trade_date...')
    #update
    insert = "REPLACE INTO trade_date_1 (Date, insert_time) VALUES ('%s','%s')"%(date,datetime.datetime.now())
    print(date)
    cursor.execute(insert)
    db.commit()


#    # login joinquant
#    auth('15305333613','a23187')
#    df = get_all_securities(types=['futures'], date=None)
#    change_code = {'XSGE':'SHF','XDCE':'DCE','XZCE':'CZC','XINE':'SHF','CCFX':'CFE'}
#
#    print('update trading_secucode...')
#    print(date)
#    # secu trade in date
#    df1 = df[(df['start_date']<date)&(df['end_date']>date)]
#    for j in df1.itertuples():
#        code_joint = j[0]
#        if('8888' in code_joint or '9999' in code_joint):
#            continue
#        point_loc = code_joint.rfind('.')
#        code_ths = change_code[code_joint[point_loc+1:]]
#        kind = code_joint[:point_loc]
#        # THS's CZC code number is only 3 digit 
#        if(code_ths == 'CZC'):
#            kind = kind[:2]+kind[3:]
#        code_ths = kind + '.' + code_ths
#        name = j[1]
#        print(date,code_ths)
#        insert = "REPLACE INTO trading_secucode_1 (Date, THScode, name, insert_time) VALUES ('%s','%s','%s','%s')"%(date,code_ths,name,datetime.datetime.now())
#        cursor.execute(insert)
#    db.commit()
#    
    # security from THS

    print('update trading_secucode...')
## import security
    trade_market = ['091001','091002','091003','091004','091027']
    query_str = [date + ';%s;monthcontract'%i for i in trade_market]
    for i in query_str:
        print(i)
        security = THS_DP('block',i,'date:Y,thscode:Y,security_name:Y').data
        if(type(security) == pd.core.frame.DataFrame):
            for row in security.itertuples():
                insert = "REPLACE INTO trading_secucode_00 (Date, THScode, name, insert_time) VALUES ('%s','%s','%s','%s')"%(getattr(row,'DATE'),getattr(row,'THSCODE'),getattr(row,'SECURITY_NAME'),datetime.datetime.now())
                cursor.execute(insert)
            db.commit()


    print('update trading_daily_price...')
    # get all trade secu
    select = "select THScode from trading_secucode_00 where Date = '%s';"%date
    cursor.execute(select)
    codes = cursor.fetchall()
    for j in codes:
    # CFE data is not supported in THS
        if('CFE' not in j[0]):
            code = j[0]
            print(date,code)
            temp = THS_HQ(code,'preClose,open,high,low,close,changeRatio,volume,amount,openInterest,positionChange','',date,date).data 
            try:
                insert = "REPLACE INTO trading_daily_price_1 (date, code,preclose,open,high,low,close,change_ratio,volume,amount,open_interest,position_change,insert_time) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"%(date,code,temp['preClose'][0],temp['open'][0],temp['high'][0],temp['low'][0],temp['close'][0],temp['changeRatio'][0],temp['volume'][0],temp['amount'][0],temp['openInterest'][0],temp['positionChange'][0],datetime.datetime.now())
                insert = insert.replace("'None'",'null')
            except:
                print(date,code,'no data')
                continue
            cursor.execute(insert)
    db.commit()
    db.close()
    THS_iFinDLogout()
    
daily_commodity_update(date)
