import os
from sqlite3 import TimestampFromTicks
import requests
import json
import ccxt
from datetime import datetime
from pytz import utc
import time
import numpy as np
import pandas as pd
import talib

apiKey='填上你自己的'
secret='填上你自己的'

exchange=ccxt.okx({
#     #代理部分
#     'proxies':{
#     'http':'socks5h://127.0.0.1:7891',
#     'https':'socks5h://127.0.0.1:7891'
#     },
    #api登陆
    'apiKey':apiKey,
    'secret':secret    
    }) 
#币种
exchange.symbol='ETH/USDT'
#交易所该币种交易最小数量精度
exchange.AmountPrecision=4
#交易所该币种价格最小精度
exchange.PricePrecision=4

def main():
    getDatas('15m','2021-09-28 08:00:00','2022-09-28 08:00:00')
    mergeDatas('15m')

def getData(timeFrame, strTime1, strTime2, fileNameIndex=-1):
    name_attribute = ['UTC','O','H','L','C','V']
    if fileNameIndex==-1:
        time1 = round(datetime.strptime(strTime1, '%Y-%m-%d %H:%M:%S').timestamp()*1000)
    else:
        dfPrevious = pd.read_csv('./data_%s/data_%d.csv'%(timeFrame,fileNameIndex),encoding='utf-8')
        lastTimeSpan = dfPrevious['UTC'].iloc[-1]
        time1 = round(lastTimeSpan)
    fileNameIndex += 1
    time2 = round(datetime.strptime(strTime2, '%Y-%m-%d %H:%M:%S').timestamp()*1000)
    while(time1<time2):
        data = exchange.fetch_ohlcv(exchange.symbol,since = time1,limit=1000)
        dfCurrent = pd.DataFrame(columns=name_attribute,data=data)
        time1 = round(dfCurrent['UTC'].iloc[-1])
        dfCurrent.to_csv('./data_%s/data_%d.csv'%(timeFrame,fileNameIndex),encoding='utf-8')
        fileNameIndex += 1
        time.sleep(10)

def getTime(timeSpan):
    tre_timeArray = time.localtime(timeSpan/1000)
    return time.strftime("%Y-%m-%d %H:%M:%S", tre_timeArray)

def getDatas(timeFrame, strTime1, strTime2):
    rootdir='data_%s'%(timeFrame)
    file_names=[]
    for parent, dirnames, filenames in os.walk(rootdir): 
      file_names=filenames
    if(len(filenames)==0):
        return getData(timeFrame, strTime1, strTime2)

    max =-1
    for file_name in file_names:
        strFile = os.path.splitext(file_name)[0]
        index = int(strFile[5:])
        if(index > max):
            max = index
    return getData(timeFrame, strTime1, strTime2, max)

def mergeDatas(timeFrame):
    dfFrames=[]
    rootdir='data_%s'%(timeFrame)
    file_names=[]
    for parent, dirnames, filenames in os.walk(rootdir): 
      file_names=filenames
    for file_name in file_names:
        filepath = rootdir+ "/" +  file_name
        dfFrame = pd.read_csv(filepath)
        dfFrames.append(dfFrame)
    df = pd.concat(dfFrames,)
    df = df.sort_values(by=["UTC"])
    df.to_csv('./data_%s.csv'%(timeFrame),encoding='utf-8')

def getTestData(timeFrame):
    return pd.read_csv('data_%s.csv'%(timeFrame))
#main()



