import configparser as cp
import datetime as dt
import os
import re
import time

from sqlalchemy import create_engine
import mysql.connector
import numpy as np
import pandas as pd

from DataReader import DBreader

class DataProcessor:

    def __init__(self):
        cfp = cp.ConfigParser()
        cfp.read('.\loginfo.ini')
        self._loginfo = dict(cfp.items('login'))

    def _getConn_(self,dbname):
        conn = mysql.connector.connect(**self._loginfo)
        conn.cursor().execute('USE {0};'.format(dbname))
        print('connected to {0}'.format(dbname))
        return conn

    def timetrans(self,seconds):
        bjtime = time.localtime(seconds)
        return time.strftime("%Y%m%d %H:%M:%S",bjtime)

    def make_features(self,exelines,dbname='hbc_train'):
        start = time.time()
        conn = self._getConn_(dbname=dbname)
        cursor = conn.cursor()
        cursor.execute('DROP TABLE IF EXISTS featuresAll')
        cursor.execute('SELECT {0}userid,province,age FROM features'.format('' if 'test' in dbname else 'response,'))
        alltable = pd.DataFrame(cursor.fetchall(),columns=['response','userid','province','age'])
        for exeline in exelines:
            cursor.execute(exeline)
            colname = re.findall(pattern='(?<=AS )[\w]*(?= FROM)',string=exeline)[0]
            print(colname)
            newtb = pd.DataFrame(cursor.fetchall(),columns=['userid',colname])
            # alltable = alltable.join(other=newtb,on='userid',how='left')
            alltable = pd.merge(left=alltable,right=newtb,how='left',on='userid')
        # process table
        cursor.execute('SELECT province FROM features')
        provdict = {}
        for num,prov in enumerate(set([c[0] for c in cursor.fetchall()])):
            provdict[prov] = num
        conn.close()
        alltable = alltable.fillna(0)
        alltable['province'] = alltable['province'].map(lambda x: -1 if x=='nan' else provdict[x])
        alltable['age'] = alltable['age'].map(lambda x:-1 if x=='nan' else int(x.strip('后')))
        alltable['bought1'] = alltable['bought1']>0
        alltable['bought0'] = alltable['bought0']>0
        alltable['act59'] = alltable['maxact']>=5
        alltable['ordrate24'] = alltable['ordnum']/alltable['numact24']
        alltable['ordrate59'] = alltable['ordnum']/alltable['numact59']
        alltable['ordrateall'] = alltable['ordnum']/alltable['numactall']
        engine  = create_engine('mysql+mysqlconnector://root:Wqxl7309@localhost:3306/{0}?charset=utf8'.format(dbname))
        alltable.to_sql(name='featuresall',con=engine,if_exists='replace',index=False)
        print(alltable.shape)
        print(time.time()-start)

    def _executeDB_(self,dbname,exeline):
        start = time.time()
        conn = self._getConn_(dbname=dbname)
        cursor = conn.cursor()
        print(exeline)
        cursor.execute(exeline)
        conn.commit()
        print('finished with {0} seconds'.format(time.time()-start))

if __name__=='__main__':
    obj = DataProcessor()

    # exeline = ' '.join(['CREATE TABLE features AS',
    #                     'SELECT l.*,r.lstacttime FROM userprofile AS l',
    #                     'LEFT JOIN (SELECT userid,MAX(actionTime) AS lstacttime FROM action GROUP BY userid) AS r',
    #                     'ON l.userid=r.userid'])

    # exeline = ' '.join(['CREATE TABLE features AS',
    #                     'SELECT tb.ordertype AS response,l.*,r.lstacttime FROM userprofile AS l',
    #                     'LEFT JOIN (SELECT userid,MAX(actionTime) AS lstacttime FROM action GROUP BY userid) AS r',
    #                     'ON l.userid=r.userid',
    #                     'LEFT JOIN orderfuture AS tb ON tb.userid=l.userid'])


    # exeline = ' '.join(['CREATE TABLE temphist AS',
    #                     'SELECT tb1.*,tb2.lstacttime,tb3.rating,tb3.tags,tb3.commentsKeyWords FROM orderHistory AS tb1',
    #                     'LEFT JOIN features AS tb2 ON tb1.userid=tb2.userid',
    #                     'LEFT JOIN usercomment AS tb3 ON tb1.orderid=tb3.orderid'])

    # exeline = ' '.join(['CREATE TABLE tempact AS',
    #                     'SELECT l.*,r.lstacttime FROM action AS l',
    #                     'LEFT JOIN features AS r',
    #                     'ON l.userid=r.userid'])

    # obj._executeDB_(dbname='hbc_test',exeline=exeline)
    # obj._executeDB_(dbname='hbc_train',exeline=exeline)

    exelines = ['SELECT userid,COUNT(*) AS bought1 FROM temphist WHERE ordertype=1 GROUP BY userid',
                'SELECT userid,COUNT(*) AS bought0 FROM temphist WHERE ordertype=0 AND (rating IS NOT NULL OR tags IS NOT NULL OR commentsKeyWords IS NOT NULL) GROUP BY userid',
                'SELECT userid,COUNT(*) AS ordnum FROM temphist GROUP BY userid',

                'SELECT userid,AVG(rating) AS avgrt FROM temphist WHERE rating IS NOT NULL GROUP BY userid',
                'SELECT userid,COUNT(*) AS cmtnum FROM temphist WHERE tags IS NOT NULL OR commentsKeyWords IS NOT NULL GROUP BY userid',

                'SELECT userid,MAX(actiontype) AS maxact FROM tempact GROUP BY userid',
                'SELECT userid,COUNT(*) AS numactall FROM tempact GROUP BY userid',
                'SELECT userid,COUNT(*) AS numact24 FROM tempact WHERE actiontype>=2 AND actiontype<=4 GROUP BY userid',
                'SELECT userid,COUNT(*) AS numact59 FROM tempact WHERE actiontype>=5 AND actiontype<=9 GROUP BY userid',

                # 'SELECT userid,COUNT(*) AS numact1 FROM tempact WHERE actiontype=1 GROUP BY userid',
                # 'SELECT userid,COUNT(*) AS numact2 FROM tempact WHERE actiontype=2 GROUP BY userid',
                # 'SELECT userid,COUNT(*) AS numact3 FROM tempact WHERE actiontype=3 GROUP BY userid',
                # 'SELECT userid,COUNT(*) AS numact4 FROM tempact WHERE actiontype=4 GROUP BY userid',
                # 'SELECT userid,COUNT(*) AS numact5 FROM tempact WHERE actiontype=5 GROUP BY userid',
                # 'SELECT userid,COUNT(*) AS numact6 FROM tempact WHERE actiontype=6 GROUP BY userid',
                # 'SELECT userid,COUNT(*) AS numact7 FROM tempact WHERE actiontype=7 GROUP BY userid',
                # 'SELECT userid,COUNT(*) AS numact8 FROM tempact WHERE actiontype=8 GROUP BY userid',
                # 'SELECT userid,COUNT(*) AS numact9 FROM tempact WHERE actiontype=9 GROUP BY userid',
                #
                # 'SELECT userid,AVG(lstacttime-actiontime)/60 AS avgtact1 FROM tempact WHERE actiontype=1 GROUP BY userid',
                # 'SELECT userid,AVG(lstacttime-actiontime)/60 AS avgtact2 FROM tempact WHERE actiontype=2 GROUP BY userid',
                # 'SELECT userid,AVG(lstacttime-actiontime)/60 AS avgtact3 FROM tempact WHERE actiontype=3 GROUP BY userid',
                # 'SELECT userid,AVG(lstacttime-actiontime)/60 AS avgtact4 FROM tempact WHERE actiontype=4 GROUP BY userid',
                # 'SELECT userid,AVG(lstacttime-actiontime)/60 AS avgtact5 FROM tempact WHERE actiontype=5 GROUP BY userid',
                # 'SELECT userid,AVG(lstacttime-actiontime)/60 AS avgtact6 FROM tempact WHERE actiontype=6 GROUP BY userid',
                # 'SELECT userid,AVG(lstacttime-actiontime)/60 AS avgtact7 FROM tempact WHERE actiontype=7 GROUP BY userid',
                # 'SELECT userid,AVG(lstacttime-actiontime)/60 AS avgtact8 FROM tempact WHERE actiontype=8 GROUP BY userid',
                # 'SELECT userid,AVG(lstacttime-actiontime)/60 AS avgtact9 FROM tempact WHERE actiontype=9 GROUP BY userid',
                #
                # 'SELECT userid,STD(lstacttime-actiontime)/60 AS stdtact1 FROM tempact WHERE actiontype=1 GROUP BY userid',
                # 'SELECT userid,STD(lstacttime-actiontime)/60 AS stdtact2 FROM tempact WHERE actiontype=2 GROUP BY userid',
                # 'SELECT userid,STD(lstacttime-actiontime)/60 AS stdtact3 FROM tempact WHERE actiontype=3 GROUP BY userid',
                # 'SELECT userid,STD(lstacttime-actiontime)/60 AS stdtact4 FROM tempact WHERE actiontype=4 GROUP BY userid',
                # 'SELECT userid,STD(lstacttime-actiontime)/60 AS stdtact5 FROM tempact WHERE actiontype=5 GROUP BY userid',
                # 'SELECT userid,STD(lstacttime-actiontime)/60 AS stdtact6 FROM tempact WHERE actiontype=6 GROUP BY userid',
                # 'SELECT userid,STD(lstacttime-actiontime)/60 AS stdtact7 FROM tempact WHERE actiontype=7 GROUP BY userid',
                # 'SELECT userid,STD(lstacttime-actiontime)/60 AS stdtact8 FROM tempact WHERE actiontype=8 GROUP BY userid',
                # 'SELECT userid,STD(lstacttime-actiontime)/60 AS stdtact9 FROM tempact WHERE actiontype=9 GROUP BY userid',
                #
                # 'SELECT userid,MIN(lstacttime-actiontime)/60 AS mintact1 FROM tempact WHERE actiontype=1 GROUP BY userid',
                # 'SELECT userid,MIN(lstacttime-actiontime)/60 AS mintact2 FROM tempact WHERE actiontype=2 GROUP BY userid',
                # 'SELECT userid,MIN(lstacttime-actiontime)/60 AS mintact3 FROM tempact WHERE actiontype=3 GROUP BY userid',
                # 'SELECT userid,MIN(lstacttime-actiontime)/60 AS mintact4 FROM tempact WHERE actiontype=4 GROUP BY userid',
                # 'SELECT userid,MIN(lstacttime-actiontime)/60 AS mintact5 FROM tempact WHERE actiontype=5 GROUP BY userid',
                # 'SELECT userid,MIN(lstacttime-actiontime)/60 AS mintact6 FROM tempact WHERE actiontype=6 GROUP BY userid',
                # 'SELECT userid,MIN(lstacttime-actiontime)/60 AS mintact7 FROM tempact WHERE actiontype=7 GROUP BY userid',
                # 'SELECT userid,MIN(lstacttime-actiontime)/60 AS mintact8 FROM tempact WHERE actiontype=8 GROUP BY userid',
                # 'SELECT userid,MIN(lstacttime-actiontime)/60 AS mintact9 FROM tempact WHERE actiontype=9 GROUP BY userid',
                #
                # 'SELECT userid,MAX(lstacttime-actiontime)/60 AS maxtact1 FROM tempact WHERE actiontype=1 GROUP BY userid',
                # 'SELECT userid,MAX(lstacttime-actiontime)/60 AS maxtact2 FROM tempact WHERE actiontype=2 GROUP BY userid',
                # 'SELECT userid,MAX(lstacttime-actiontime)/60 AS maxtact3 FROM tempact WHERE actiontype=3 GROUP BY userid',
                # 'SELECT userid,MAX(lstacttime-actiontime)/60 AS maxtact4 FROM tempact WHERE actiontype=4 GROUP BY userid',
                # 'SELECT userid,MAX(lstacttime-actiontime)/60 AS maxtact5 FROM tempact WHERE actiontype=5 GROUP BY userid',
                # 'SELECT userid,MAX(lstacttime-actiontime)/60 AS maxtact6 FROM tempact WHERE actiontype=6 GROUP BY userid',
                # 'SELECT userid,MAX(lstacttime-actiontime)/60 AS maxtact7 FROM tempact WHERE actiontype=7 GROUP BY userid',
                # 'SELECT userid,MAX(lstacttime-actiontime)/60 AS maxtact8 FROM tempact WHERE actiontype=8 GROUP BY userid',
                # 'SELECT userid,MAX(lstacttime-actiontime)/60 AS maxtact9 FROM tempact WHERE actiontype=9 GROUP BY userid',
                ]
    obj.make_features(exelines=exelines)