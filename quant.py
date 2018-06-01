#!/usr/bin/python  
# coding: UTF-8  
import tushare as ts  
import mongoService
import logging


logger = logging.getLogger("mainModule.sub")

def getAllStocksBasics():

    stocksBasics = mongoService.find('stocksBasics', {})
    stocksCache = {}
    for row in stocksBasics:
        stocksCache[row['code']] = row
    
    return stocksCache

def getAlltop10Holders():

    stocksBasics = mongoService.find('top10Holders', {})
    top10Holders = {}
    for row in stocksBasics:
        if(row['code'] not in top10Holders):
            top10Holders[row['code']] = {}
        top10Holders[row['code']][row['name']] = row
    
    return top10Holders

def getStocksHistData(date):
    res = mongoService.find('stocksHistData', {'date':date})
    stocksHistCache = {}
    for row in res:
        stocksHistCache[row['code']] = row

    return stocksHistCache

def getAllHoldersContainsHuijin(stocksCache, top10Holders, initStocks):

    filteredStocks = {}
    for key in initStocks.keys():
        if(key not in top10Holders):
            #print("code not exist in stocks Hist %s" %(key))
            continue

        holders = top10Holders[key]
        for holdersKey in holders.keys():
            if(holdersKey in ['中央汇金资产管理有限责任公司', '中国证券金融股份有限公司']):
                if(float(holders[holdersKey]['h_pro']) < 1):
                    continue

                filteredStocks[key] = initStocks[key]
                filteredStocks[key]['holderName']=holders[holdersKey]['name']
                filteredStocks[key]['h_pro']=holders[holdersKey]['h_pro']
                filteredStocks[key]['hold']=holders[holdersKey]['hold']
                #logger.info('代码:%s, 名字:%s, 当前价:%s,市净率:%s,市赢率:%s, 5日线:%s, 20日线:%s, 半年线:%s,半年线:%s, 2年前价:%s, 基金名:%s, 持股比:%s, 持股量:%s', *(filteredStocks[key]['code'],stocksCache[key]['name'], filteredStocks[key]['close'], filteredStocks[key]['pb'], filteredStocks[key]['pe'], filteredStocks[key]['ma5'], filteredStocks[key]['ma20'], filteredStocks[key]['ma120'],filteredStocks[key]['ma250'], filteredStocks[key]['closePrice3Year'], filteredStocks[key]['holderName'], filteredStocks[key]['h_pro'], filteredStocks[key]['hold']))
                break

    return filteredStocks

def getAllPriceLowerThanMa20(stocksCache, stocksHist, initStocks):

    filteredStocks = {}
    for key in initStocks.keys():
        if(key not in stocksHist):
            #print("code not exist in stocks Hist %s" %(key))
            continue

        hist = stocksHist[key]
        if(0.95 < hist['close']/hist['ma20'] < 1.05 and initStocks[key]['esp'] > 0):
            filteredStocks[key] = initStocks[key]
            filteredStocks[key]['esp']=initStocks[key]['esp']
            filteredStocks[key]['pb']=initStocks[key]['pb']
            filteredStocks[key]['pe']=initStocks[key]['pe']
            filteredStocks[key]['close']=hist['close']
            filteredStocks[key]['ma20']=hist['ma20']
            logger.debug('代码:%s, 名字:%s, close price %s, ma20 is %s, 每股收益:%s', *(hist['code'],stocksCache[hist['code']]['name'], hist['close'], hist['ma20'], initStocks[key]['esp']))
    
    return filteredStocks

def getAllMa5HigherThanMa20(stocksCache, stocksHist, initStocks):

    filteredStocks = {}
    for key in initStocks.keys():
        if(key not in stocksHist):
            #print("code not exist in stocks Hist "  + key)
            continue

        hist = stocksHist[key]
        if(hist['ma5'] > hist['ma20']):
            #filteredStocks[hist['code']]={'code':hist['code'],'close':hist['ma5'], 'ma20':hist['ma20']}
            filteredStocks[key] = initStocks[key]
            filteredStocks[key]['ma5']=hist['ma5']
            filteredStocks[key]['ma20']=hist['ma20']
            filteredStocks[key]['ma30']=hist['ma30']
            filteredStocks[key]['ma120']=hist['ma120']
            filteredStocks[key]['ma250']=hist['ma250']
            #print("code is %s, name is %s, ma5 is %s, ma20 is %s" %(hist['code'],stocksCache[hist['code']]['name'], hist['ma5'], hist['ma20']))
            logger.debug('code is %s, name is %s, ma5 is %s, ma20 is %s', *(hist['code'],stocksCache[hist['code']]['name'], hist['ma5'], hist['ma20']))
    
    return filteredStocks

def getPriceLower300bpsThan3YearAgo(stocksCache, stocksHist, stocksHist3Year, initStocks):

    filteredStocks = {}
    for key in initStocks.keys():
        if(key not in stocksHist or key not in stocksHist3Year):
            logger.debug("code not exist in stocks Hist "  + key)
            continue

        hist = stocksHist[key]
        hist3Year = stocksHist3Year[key]
        if(hist['close']/hist3Year['close'] < 0.7):
            #filteredStocks[hist['code']]={'code':hist['code'],'price':hist['close'], 'price3Year':hist3Year['close']}
            filteredStocks[key] = initStocks[key]
            filteredStocks[key]['close']=hist['close']
            filteredStocks[key]['closePrice3Year']=hist3Year['close']
            del filteredStocks[key]['_id']
            #print("code is %s, name is %s, close price %s, ma5 is %s, ma20 is %s, price3Year is %s" %(hist['code'],stocksCache[hist['code']]['name'], hist['close'], hist['ma5'], hist['ma20'], hist3Year['close']))
            #logger.info('代码:%s, 名字:%s, 当前价:%s,市净率:%s,市赢率:%s, 5日线:%s, 20日线:%s, 半年线:%s,半年线:%s, 2年前价:%s, 汇金持股比:%s, 汇金持股量:%s', *(hist['code'],stocksCache[hist['code']]['name'], hist['close'], filteredStocks[key]['pb'], filteredStocks[key]['pe'], hist['ma5'], hist['ma20'], filteredStocks[key]['ma120'],filteredStocks[key]['ma250'], hist3Year['close'], filteredStocks[key]['h_pro'], filteredStocks[key]['hold']))
    
    return filteredStocks

if __name__ == '__main__':  
    STOCK = '600000'       ##浦发银行  
    #parse(STOCK) 
    getAllPriceLowerThanMa20(getAllStocksBasics(), getStocksHistData('2018-05-14'), getAllStocksBasics()) 