import quant as qt
import tushareService as tss  
import mongoService as mgs  
from flask import Flask, Blueprint
from flask_restplus import Resource, Api
from flask_restplus import fields, marshal_with
import json
import pandas as pd
import time
import logging
from logging.handlers import RotatingFileHandler
logger = logging.getLogger("mainModule")
logger.setLevel(level = logging.INFO)
#定义一个RotatingFileHandler，最多备份3个日志文件，每个日志文件最大1K
rHandler = RotatingFileHandler("log.txt",maxBytes = 1000*1024,backupCount = 3)
rHandler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s-%(name)s-%(levelname)s-%(message)s')
rHandler.setFormatter(formatter)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(formatter)

logger.addHandler(rHandler)
logger.addHandler(console)
#from flask.ext.restful import Api, Resource

app = Flask(__name__)
#api = Api(app, version='1.0', title='StockMining API',description='StockMining API',)
blueprint = Blueprint('api', __name__, url_prefix='/api')
api = Api(blueprint, version='1.0', title='StockMining API', doc='/doc/')
app.register_blueprint(blueprint)

@api.route('/AllPriceLowerThanMa20')
class AllPriceLowerThanMa20(Resource):
    @api.doc(id='getAllPriceLowerThanMa20')
    def get(self):
        stocksBasics = qt.getAllStocksBasics()
        stocksHist = qt.getStocksHistData('2018-05-14')
        top10Holders = qt.getAlltop10Holders()
        stocksHist2Year = qt.getStocksHistData('2016-06-14')
        f1 = qt.getAllPriceLowerThanMa20(stocksBasics, stocksHist, stocksBasics)
        f2 = qt.getAllMa5HigherThanMa20(stocksBasics, stocksHist, f1)
        f3 = qt.getPriceLower300bpsThan3YearAgo(stocksBasics, stocksHist,stocksHist2Year, f2)
        filteredRes = qt.getAllHoldersContainsHuijin(stocksBasics, top10Holders, f3)

        res = []
        for key in filteredRes.keys():
            res.append((filteredRes[key]['code'],filteredRes[key]['name'], filteredRes[key]['close'], filteredRes[key]['pb'], filteredRes[key]['pe'], filteredRes[key]['ma5'], filteredRes[key]['ma20'], filteredRes[key]['ma120'],filteredRes[key]['ma250'], filteredRes[key]['closePrice3Year'], filteredRes[key]['holderName'], filteredRes[key]['h_pro'], filteredRes[key]['hold']))
        
        dataframe = pd.DataFrame.from_records(res, columns=['代码','名字','当前价','市净率','市赢率','5日线','20日线','半年线','半年线','2年前价','基金名','持股比','持股量']);
        dataframe.sort_values(by = ['持股比'],axis = 0,ascending = False) 
        dataframe.set_index('代码') 
        #dataframe.reset_index()
        #return dataframe
        pd.set_option('display.max_rows',500)
        pd.set_option('display.max_columns',500)
        pd.set_option('display.width',1000)
        
        logger.info(dataframe)
        return filteredRes

#api.add_resource(AllPriceLowerThanMa20, '/AllPriceLowerThanMa20')
@api.route('/insertAllHistData')
class insertAllHistData(Resource):
    @api.doc(id='insertAllHistData')
    def get(self):
        tss.insertAllHistData('2018-05-16', time.strftime("%Y-%m-%d"))

@api.route('/refreshAllHistData')
class refreshAllHistData(Resource):
    @api.doc(id='refreshAllHistData')
    def get(self):
        tss.refreshAllHistData('2015-05-16', time.strftime("%Y-%m-%d"))

@api.route('/refreshAll')
class refreshAll(Resource):
    @api.doc(id='refreshAll')
    def get(self):
        tss.refreshAll()


resource_fields = {
    'value':   fields.String
}

# 定义命名空间
ns = api.namespace('Childrens', description='Children operations')
 
children = api.model('Children', {
     'id': fields.Integer(readOnly=True, description='The Children unique identifier'),
     'name': fields.String(required=True, description='The Children name')
})
 
@ns.route('/')
class ChildrenList(Resource):
    @ns.doc('createChildren')
    @ns.expect(children)
    @ns.marshal_with(children, code=201)
    def post(self):
        logger.info('insert Children')
        mgs.insert('children',api.payload)

@ns.route('/<string:query>')
@ns.response(404, 'Children not found')
@ns.param('query', 'The Children identifier')
class Children(Resource):
    def get(self, query):
        logger.info('get Children' + query)
        return mgs.find('children',json.loads(query))

if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True)