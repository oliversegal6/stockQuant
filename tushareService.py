# -*- coding: utf-8 -*-
from mongoConn import MongoConn
import json
import sys
import tushare as ts
import mongoService
import traceback
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor
import logging
logger = logging.getLogger(__name__)
logger.setLevel(level = logging.INFO)
#handler = logging.FileHandler("log.txt")
handler = logging.FileHandler("/tmp/log.txt")
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def refreshAllStocksOfToday():
    logger.info("refreshAllStocksOfToday...")
    df = ts.get_today_all()
    refreshAndPrintResult('todaydata', df)

def refreshStocksByConceptClassified():
    logger.info("refreshStocksByConceptClassified...")
    df = ts.get_concept_classified()
    refreshAndPrintResult('stocksByConcept', df)

def refreshStocksByIndustryClassified():
    logger.info("refreshStocksByIndustryClassified...")
    df = ts.get_concept_classified()
    refreshAndPrintResult('stocksByIndustry', df)

def refreshStocksBasics():
    logger.info("refreshStocksBasics...")
    df = ts.get_stock_basics()
    refreshAndPrintResultResetIndex('stocksBasics', df)

def refreshFundHoldings():
    logger.info("refreshFundHoldings...")
    df1 = ts.fund_holdings(2018, 1)
    refreshAndPrintResult('fundHoldings', df1)


def refreshTop10Holders():
    logger.info("refreshTop10Holders...")
    mongoService.drop_collection('top10Holders')
    mongoService.drop_collection('stockHoldersChanges')
    res = mongoService.find('stocksBasics', {})
    count = 1
    for row in res:
        try:
            logger.info('count %d' %count)
            logger.info('code is %s' %row['code'])
            df = ts.top10_holders(row['code'], 2018, 3)
            df[0]['code'] = row['code']
            df[1]['code'] = row['code']
            logger.info('Top10 Holders of %s' %(row['code']))
            insertAndPrintResult('stockHoldersChanges', df[0])
            insertAndPrintResult('top10Holders', df[1])
            count+=1
        except Exception:
            logger.error(traceback.format_exc())

def refreshAllHistData(start, end):
    logger.info("refreshAllHistData...")
    mongoService.drop_collection('stocksHistData')
    insertAllHistData(start, end, "False")

def refreshAll():
    refreshAllStocksOfToday()
    refreshStocksByConceptClassified()
    refreshStocksByIndustryClassified()
    refreshStocksBasics()
    refreshFundHoldings()
    refreshTop10Holders()
    refreshAllHistData('2018-05-16', time.strftime("%Y-%m-%d"))

def insertAllHistData(start, end, isOneDay):
    logger.info('Start insertAllHistData')
    res = mongoService.find('stockBasic', {})

    count = 1
    for row in res:
       logger.info('count %d' %count)
       #logger.info('code is %s' %row['ts_code'])
       insertSpecificHistData(row['ts_code'], start, end, isOneDay)
       count+=1
    
    logger.info('End of insertAllHistData')
    #insertSpecificHistData('sh', start, end)#获取上证指数k线数据，其它参数与个股一致，下同
    #insertSpecificHistData('sz', start, end)#获取深圳成指k线数据
    #insertSpecificHistData('hs300', start, end)#获取沪深300指数k线数据
    #insertSpecificHistData('sz50', start, end)#获取上证50指数k线数据
    #insertSpecificHistData('zxb', start, end)#获取中小板指数k线数据
    #insertSpecificHistData('cyb', start, end)#获取创业板指数k线数据

def insertSpecificHistData(code, startDate, endDate, isOneDay):
    logger.info('code is %s, start date: %s, end date: %s' %(code, startDate, endDate))
    pro = ts.pro_api()
    df = ts.pro_bar(pro_api=pro, ts_code=code, adj='qfq', start_date=startDate, end_date=endDate, ma=[5, 10, 20, 60, 120, 250])
    if(df is None):
        logger.info('Can not find hist data for %s' %code)
        return

    df = df.sort_index()
    df['code'] = code
    #年线是250日均线，半年线是120日均线，月线是30日均线
    #ma_list = [30,60,120,250]
    #for ma in ma_list:
    #    df['ma' + str(ma)] = pd.Series(df.close).rolling(window=ma).mean()
    #    df['v_ma' + str(ma)] = pd.Series(df.amount).rolling(window=ma).mean()
    if(isOneDay == "True"):
        logger.info("Only save today's data, remove rest of data")
        indexNotEndDate = df[(df.index != endDate)].index.tolist()
        df = df.drop(indexNotEndDate);
    
    insertAndPrintResult('stocksHistData_' + endDate[0:4], df)
    

def insertAndPrintResultResetIndex(tableName, data):
    insertAndPrintResult(tableName, data.reset_index())

def insertAndPrintResult(tableName, data):
    jsonStr = data.to_json(orient='records');
    if(jsonStr is None):
        logger.info('Cant insert to db as data is empty for %s' %jsonStr)
        return
    mongoService.insert(tableName, json.loads(jsonStr))

def refreshAndPrintResultResetIndex(tableName, data):
    refreshAndPrintResult(tableName, data.reset_index())

def refreshAndPrintResult(tableName, data):
    mongoService.drop_collection(tableName)
    mongoService.insert(tableName, json.loads(data.to_json(orient='records')))
    res=mongoService.find(tableName,{})
    #for k in res:
    #    print(k)

if __name__ == "__main__":
    logger.info(ts.__version__)
    ts.set_token('5dfbbdf5953c683a061952a4a6c7eae376dc2a892ee3ce5ed4117d64')
    #pro = ts.pro_api()
    #insertAllStocksOfToday()

    #insertStocksByConceptClassified()
    #insertStocksByIndustryClassified()
    # #insertStocksBasics()
    #insertFundHoldings()
    #print(ts.__version__)
    #refreshAllHistData('20150514', '20190111')
    startDate = sys.argv[1] # 20190227
    endDate = sys.argv[2] # 20190307
    isOneDay = sys.argv[3]
    insertAllHistData(startDate, endDate, isOneDay)
    #refreshStocksBasics()
    #refreshTop10Holders()
    #insertTop10Holders()
    #df = ts.get_hist_data('300253')
   
    #logger.info(df)
    #df = ts.get_hist_data('300253')
    #df = ts.get_stock_basics()
    #df.to_csv('./300253.csv')
    #df1 = df.reset_index()
    
