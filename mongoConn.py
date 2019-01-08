# -*- coding: utf-8 -*-
import pymongo
import sys
import traceback    
import logging  # 引入logging模块
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')  # logging.basicConfig函数对日志的输出格式及方式做相关配置

MONGODB_CONFIG = {
    'host': '172.17.0.1',
    'port': 27017,
    'db_name': 'stockminingnew',
    'username': None,
    'password': None
}

class MongoConn(object):

    def __init__(self):
        # connect db
        try:
            logging.info('connect db...')
            self.conn = pymongo.MongoClient(MONGODB_CONFIG['host'], MONGODB_CONFIG['port'])
            self.db = self.conn[MONGODB_CONFIG['db_name']]  # connect db
            self.username=MONGODB_CONFIG['username']
            self.password=MONGODB_CONFIG['password']           
            if self.username and self.password:
                self.connected = self.db.authenticate(self.username, self.password)
            else:
                self.connected = True
        except Exception:
            logging.info(traceback.format_exc())
            logging.info('Connect Statics Database Fail.')
            sys.exit(1)   