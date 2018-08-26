import quant as qt
import tushareService as tss  
import mongoService as mgs  
from functools import wraps
from flask import Flask, Blueprint, request, make_response
from flask_restplus import Resource, Api, reqparse, fields, marshal_with
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
blueprint = Blueprint('api', __name__)
#blueprint = Blueprint('api', __name__, url_prefix='/api')
api = Api(blueprint, version='1.0', title='StockMining API', doc='/doc/')
app.register_blueprint(blueprint)

@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization,session_id')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS,HEAD')
    # 这里不能使用add方法，否则会出现 The 'Access-Control-Allow-Origin' header contains multiple values 的问题
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response

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


# 定义命名空间
ns = api.namespace('ToBeBetter', description='ToBeBetter operations')
parser = reqparse.RequestParser()
parser.add_argument('query', type=str, required=True, help='query for this resource')
children = api.model('Children', {
     'id': fields.Integer(readOnly=True, description='The Children unique identifier'),
     'name': fields.String(required=True, description='The Children name')
})

@ns.route('/children')
class Children(Resource):
    @ns.doc('createChildren')
    @ns.expect(children)
    @ns.marshal_with(children, code=201)
    def post(self):
        logger.info('insert Children')
        mgs.upsert_one('children',api.payload)

    @ns.param('query', 'The Children identifier')
    def get(self):
        args = parser.parse_args()
        query = args["query"]
        logger.info('get Children' + query)
        return mgs.find('children',json.loads(query))

    @ns.param('query', 'The Children identifier')
    def delete(self, query):
        args = parser.parse_args()
        query = args["query"]
        logger.info('delete Children' + query)
        return mgs.delete('children',json.loads(query))

target = api.model('Target', {
     'id': fields.Integer(readOnly=True, description='The Target unique identifier'),
     'name': fields.String(required=True, description='The Target name'),
     'desc': fields.String(required=True, description='The Target description')
})
@ns.route('/target')
class Target(Resource):
    @ns.doc('createTarget')
    @ns.expect(target)
    @ns.marshal_with(target, code=201)
    def post(self):
        logger.info('insert target')
        mgs.upsert_one('target',api.payload)

    @ns.param('query', 'The target identifier')
    def get(self):
        args = parser.parse_args()
        query = args["query"]
        logger.info('get target' + query)
        return mgs.find('target',json.loads(query))

    @ns.param('query', 'The target identifier')
    def delete(self, query):
        args = parser.parse_args()
        query = args["query"]
        logger.info('delete target' + query)
        return mgs.delete('target',json.loads(query))

target = api.model('Score', {
     'id': fields.Integer(readOnly=True, description='The Score unique identifier'),
     'childId': fields.Integer(required=True, description='childId'),
     'targetId': fields.Integer(required=True, description='The Target Id'),
     'score': fields.Float(required=True, description='Score'),
     'date': fields.Date(required=True, description='The Score Date')

})
@ns.route('/score')
class Score(Resource):
    @ns.doc('createScore')
    @ns.expect(target)
    @ns.marshal_with(target, code=201)
    def post(self):
        logger.info('insert score')
        mgs.upsert_one('score',api.payload)

    @ns.param('query', 'The score identifier')
    def get(self):
        args = parser.parse_args()
        query = args["query"]
        logger.info('get score' + query)
        return mgs.find('score',json.loads(query))

    @ns.param('query', 'The score identifier')
    def delete(self, query):
        args = parser.parse_args()
        query = args["query"]
        logger.info('delete score' + query)
        return mgs.delete('score',json.loads(query))

'''
@ns.route('/<string:query>')
@ns.response(404, 'Children not found')
@ns.param('query', 'The Children identifier')
class Children(Resource):
    def get(self, query):
        logger.info('get Children' + query)
        return mgs.find('children',json.loads(query))

    def delete(self, query):
        logger.info('delete Children' + query)
        return mgs.delete('children',json.loads(query))
'''

if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True)