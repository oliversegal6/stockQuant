# -*- coding: utf-8 -*-
from mongoConn import MongoConn
import json
import tushare as ts
import mongoService
import traceback
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import logging
logger = logging.getLogger(__name__)
logger.setLevel(level = logging.INFO)
handler = logging.FileHandler("log.txt")
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def insertAllStocksOfToday():
    df = ts.get_today_all()
    insertAndPrintResult('todaydata', df)

def insertStocksByConceptClassified():
    df = ts.get_concept_classified()
    insertAndPrintResult('stocksByConcept', df)

def insertStocksByIndustryClassified():
    df = ts.get_concept_classified()
    insertAndPrintResult('stocksByIndustry', df)

def insertTop10Holders():
    mongoService.drop_collection('top10Holders')
    mongoService.drop_collection('stockHoldersChanges')
    res = mongoService.find('stocksBasics', {})
    count = 1
    for row in res:
        try:
            logger.info('count %d' %count)
            logger.info('code is %s' %row['code'])
            df = ts.top10_holders(row['code'], 2018, 1)
            df[0]['code'] = row['code']
            df[1]['code'] = row['code']
            logger.info('Top10 Holders of %s' %(row['code']))
            insertAndPrintResult('stockHoldersChanges', df[0])
            insertAndPrintResult('top10Holders', df[1])
            count+=1
        except Exception:
            logger.error(traceback.format_exc())

def insertStocksBasics():
    df = ts.get_stock_basics()
    refreshAndPrintResultResetIndex('stocksBasics', df)

def refreshAllHistData(start, end):
    mongoService.drop_collection('stocksHistData')
    insertAllHistData(start, end)

def insertAllHistData(start, end):
    res = mongoService.find('stocksBasics', {})

    count = 1
    with ThreadPoolExecutor(max_workers=20) as pool:
        for row in res:
            logger.info('count %d' %count)
            pool.submit(insertSpecificHistData, row['code'], start, end)
            count+=1
    #for row in res:
     #   logger.info('count %d' %count)
     #   logger.info('code is %s' %row['code'])
     #   insertSpecificHistData(row['code'], start, end)
     #   count+=1
    
    insertSpecificHistData('sh', start, end)#获取上证指数k线数据，其它参数与个股一致，下同
    insertSpecificHistData('sz', start, end)#获取深圳成指k线数据
    insertSpecificHistData('hs300', start, end)#获取沪深300指数k线数据
    insertSpecificHistData('sz50', start, end)#获取上证50指数k线数据
    insertSpecificHistData('zxb', start, end)#获取中小板指数k线数据
    insertSpecificHistData('cyb', start, end)#获取创业板指数k线数据

def insertSpecificHistData(code, startDate, endDate):
    logger.info('code is %s, start date: %s, end date: %s' %(code, startDate, endDate))
    df = ts.get_hist_data(code, start=startDate, end=endDate)
    if(df is None):
        logger.info('Can not find hist data for %s' %code)
        return

    df = df.sort_index()
    df['code'] = code
    #年线是250日均线，半年线是120日均线，月线是30日均线
    ma_list = [30,60,120,250]
    for ma in ma_list:
        df['ma' + str(ma)] = pd.Series(df.close).rolling(window=ma).mean()
        df['v_ma' + str(ma)] = pd.Series(df.volume).rolling(window=ma).mean()
        
    insertAndPrintResultResetIndex('stocksHistData', df)

def insertFundHoldings():
    df = ts.fund_holdings(2017, 4)
    df1 = ts.fund_holdings(2018, 1)
    insertAndPrintResult('fundHoldings', df)

    insertAndPrintResult('fundHoldings', df1)


def insertAndPrintResultResetIndex(tableName, data):
    insertAndPrintResult(tableName, data.reset_index())

def insertAndPrintResult(tableName, data):
    mongoService.insert(tableName, json.loads(data.to_json(orient='records')))
    #res=mongoService.find(tableName,{})
    #for k in res:
        #print(k)

def refreshAndPrintResultResetIndex(tableName, data):
    refreshAndPrintResult(tableName, data.reset_index())

def refreshAndPrintResult(tableName, data):
    mongoService.drop_collection(tableName)
    mongoService.insert(tableName, json.loads(data.to_json(orient='records')))
    res=mongoService.find(tableName,{})
    for k in res:
        print(k)

if __name__ == "__main__":
    #insertAllStocksOfToday()

    #insertStocksByConceptClassified()
    #insertStocksByIndustryClassified()
    #insertStocksBasics()
    #insertFundHoldings()
    #print(ts.__version__)
    refreshAllHistData('2015-05-14', '2018-05-16')
    #insertTop10Holders()
    #df = ts.get_hist_data('300253')
   
    #logger.info(df)
    #df = ts.get_hist_data('300253')
    #df = ts.get_stock_basics()
    #df.to_csv('./300253.csv')
    #df1 = df.reset_index()
    