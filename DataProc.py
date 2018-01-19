import configparser as cp
import datetime as dt
import os
import time

import mysql.connector
import numpy as np
import pandas as pd

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


    # exeline = ' '.join(['CREATE TABLE temphist AS',
    #                     'SELECT tb1.*,tb2.lstacttime,tb3.rating,tb3.tags,tb3.commentsKeyWords FROM orderHistory AS tb1',
    #                     'LEFT JOIN features AS tb2 ON tb1.userid=tb2.userid',
    #                     'LEFT JOIN usercomment AS tb3 ON tb1.orderid=tb3.orderid'])

    # exeline = ' '.join(['CREATE TABLE tempact AS',
    #                     'SELECT l.*,r.lstacttime FROM action AS l',
    #                     'LEFT JOIN features AS r',
    #                     'ON l.userid=r.userid'])

    exeline = ' '.join(['CREATE TABLE featuresAll AS',
                        'SELECT tb1.userid,tb1.province,tb2.OrdNumSelect,tb3.OrdNumAllMon1,tb4.OrdNumAllMon3,tb5.OrdNumAllMon6,tb6.OrdNumAllMon12,tb7.OrdNumAllMon,tb8.AvgRating,'+
                        'tb9.ActNum1Mon1,tb10.ActNum2To4Mon1,tb11.ActNum5To9Mon1,tb12.ActNum1Mon3,tb13.ActNum2To4Mon3,tb14.ActNum5To9Mon3',
                        'FROM features AS tb1',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS OrdNumSelect FROM temphist WHERE ordertype=1 GROUP BY userid ) AS tb2 ON tb1.userid=tb2.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS OrdNumAllMon1 FROM temphist WHERE lstacttime<=ordertime+3600*24*30 GROUP BY userid ) AS tb3 ON tb1.userid=tb3.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS OrdNumAllMon3 FROM temphist WHERE lstacttime<=ordertime+3600*24*90 GROUP BY userid ) AS tb4 ON tb1.userid=tb4.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS OrdNumAllMon6 FROM temphist WHERE lstacttime<=ordertime+3600*24*180 GROUP BY userid ) AS tb5 ON tb1.userid=tb5.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS OrdNumAllMon12 FROM temphist WHERE lstacttime<=ordertime+3600*24*360 GROUP BY userid ) AS tb6 ON tb1.userid=tb6.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS OrdNumAllMon FROM temphist GROUP BY userid ) AS tb7 ON tb1.userid=tb7.userid',
                        'LEFT JOIN (SELECT userid,AVG(rating) AS AvgRating FROM temphist WHERE rating IS NOT NULL GROUP BY userid) AS tb8 ON tb1.userid=tb8.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS ActNum1Mon1 FROM tempact WHERE actiontype=1 AND lstacttime<=actiontime+3600*24*30 GROUP BY userid ) AS tb9 ON tb1.userid=tb9.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS ActNum2To4Mon1 FROM tempact WHERE actiontype>=2 AND actiontype<=4 AND lstacttime<=actiontime+3600*24*30 GROUP BY userid ) AS tb10 ON tb1.userid=tb10.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS ActNum5To9Mon1 FROM tempact WHERE actiontype>=5 AND actiontype<=9 AND lstacttime<=actiontime+3600*24*30 GROUP BY userid ) AS tb11 ON tb1.userid=tb11.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS ActNum1Mon3 FROM tempact WHERE actiontype=1 AND lstacttime<=actiontime+3600*24*90 GROUP BY userid ) AS tb12 ON tb1.userid=tb12.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS ActNum2To4Mon3 FROM tempact WHERE actiontype>=2 AND actiontype<=4 AND lstacttime<=actiontime+3600*24*90 GROUP BY userid ) AS tb13 ON tb1.userid=tb13.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS ActNum5To9Mon3 FROM tempact WHERE actiontype>=5 AND actiontype<=9 AND lstacttime<=actiontime+3600*24*90 GROUP BY userid ) AS tb14 ON tb1.userid=tb14.userid',
                        ])

    # obj._executeDB_(dbname='hbc_test',exeline=exeline)
    obj._executeDB_(dbname='hbc_train',exeline=exeline)

    # print(obj.timetrans(1494239682+3600))