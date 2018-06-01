# -*- coding: utf-8 -*-
from mongoConn import MongoConn
import json
import tushare as ts

if __name__ == "__main__":
    if __name__ == "__main__":
    file_path = './enterprise_all.txt'
    company_list = []
    with open(file_path, "r") as in_file:
        for line in in_file:
            dic={}
            dic['_id']=line.strip()
            dic['name']=line.strip()
            company_list.append(dic)
    upsert_mary('mytest',company_list)

    datas = [
        {'_id':8, 'data':88},        
        {'_id':9, 'data':99},
        {'_id':36, 'data':3366}      
    ]    

    #插入,'_id' 的值必须不存在，否则报错
    insert('mytest', datas)
    #插入    
    data={'_id':6, 'data':66}
    save('mytest',data)
    #更新数据
    update('mytest',{'_id':8},{'$set':{'data':'888'}}, False, False)
    #更新或插入    
    data={'_id':36, 'data':'dsd'}
    upsert_one('mytest',data)  
    #查找。相对于 select _id from mytest
    res=select_colum('mytest',{},'_id')
    for k in res:
        for key, value in k.iteritems():
            print key,":",value
    #查找。相对于 select * from mytest
    res=find('mytest',{})
    for k in res:
        for key, value in k.iteritems():
            print key,":",value,
        print 
    #查找。相对于 select * from mytest limit 1
    res=find_one('mytest',{})
    for k in res:
        print k,':',res[k]
        
    conn = MongoConn()
    df = ts.get_tick_data('600848',date='2014-12-22')

    conn.db['tickdata'].insert(json.loads(df.to_json(orient='records')))
    #插入数据，'mytest'是上文中创建的表名
    #my_conn.db['mytest'].insert(datas)
    #查询数据，'mytest'是上文中创建的表名
    df = ts.get_today_all()
    my_conn.db['todaydata'].insert(json.loads(df.to_json(orient='records')))

    res=my_conn.db['todaydata'].find({})
    for k in res:
        print(k)