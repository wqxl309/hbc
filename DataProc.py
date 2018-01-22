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

    exeline = ' '.join(['CREATE TABLE featuresAll AS',
                        'SELECT tb1.response,tb1.userid,tb1.province,rts.AvgRating,'+
                        'tb2.OrdNumSelect,tb3.OrdNumAllMon1,tb4.OrdNumAllMon3,tb5.OrdNumAllMon6,tb6.OrdNumAllMon12,tb7.OrdNumAllMon,na.numactall,'+
                        'tb7.OrdNumAllMon/na.numactall AS ordrate,tb2.OrdNumSelect/na.numactall AS selrate,'+

                        'da1.avgtdact1,da2.avgtdact2,da3.avgtdact3,da4.avgtdact4,da5.avgtdact5,da6.avgtdact6,da7.avgtdact7,da8.avgtdact8,da9.avgtdact9,'+
                        'ma1.mintdact1,ma2.mintdact2,ma3.mintdact3,ma4.mintdact4,ma5.mintdact5,ma6.mintdact6,ma7.mintdact7,ma8.mintdact8,ma9.mintdact9,'+
                        'na1.numact1,na2.numact2,na3.numact3,na4.numact4,na5.numact5,na6.numact6,na7.numact7,na8.numact8,na9.numact9,'+
                        'm1na1.m1numact1,m1na2.m1numact2,m1na3.m1numact3,m1na4.m1numact4,m1na5.m1numact5,m1na6.m1numact6,m1na7.m1numact7,m1na8.m1numact8,m1na9.m1numact9,'+
                        'm3na1.m3numact1,m3na2.m3numact2,m3na3.m3numact3,m3na4.m3numact4,m3na5.m3numact5,m3na6.m3numact6,m3na7.m3numact7,m3na8.m3numact8,m3na9.m3numact9',

                        'FROM features AS tb1',
                        'LEFT JOIN (SELECT userid,AVG(rating) AS AvgRating FROM temphist WHERE rating IS NOT NULL GROUP BY userid) AS rts ON tb1.userid=rts.userid',

                        'LEFT JOIN (SELECT userid,COUNT(*) AS OrdNumSelect FROM temphist WHERE ordertype=1 GROUP BY userid ) AS tb2 ON tb1.userid=tb2.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS OrdNumAllMon1 FROM temphist WHERE lstacttime<=ordertime+3600*24*30 GROUP BY userid ) AS tb3 ON tb1.userid=tb3.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS OrdNumAllMon3 FROM temphist WHERE lstacttime<=ordertime+3600*24*90 GROUP BY userid ) AS tb4 ON tb1.userid=tb4.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS OrdNumAllMon6 FROM temphist WHERE lstacttime<=ordertime+3600*24*180 GROUP BY userid ) AS tb5 ON tb1.userid=tb5.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS OrdNumAllMon12 FROM temphist WHERE lstacttime<=ordertime+3600*24*360 GROUP BY userid ) AS tb6 ON tb1.userid=tb6.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS OrdNumAllMon FROM temphist GROUP BY userid ) AS tb7 ON tb1.userid=tb7.userid',

                        'LEFT JOIN (SELECT userid,COUNT(*) AS m1numact1 FROM tempact WHERE actiontype=1 AND lstacttime<=actiontime+3600*24*30 GROUP BY userid ) AS m1na1 ON tb1.userid=m1na1.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS m1numact2 FROM tempact WHERE actiontype=2 AND lstacttime<=actiontime+3600*24*30 GROUP BY userid ) AS m1na2 ON tb1.userid=m1na2.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS m1numact3 FROM tempact WHERE actiontype=3 AND lstacttime<=actiontime+3600*24*30 GROUP BY userid ) AS m1na3 ON tb1.userid=m1na3.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS m1numact4 FROM tempact WHERE actiontype=4 AND lstacttime<=actiontime+3600*24*30 GROUP BY userid ) AS m1na4 ON tb1.userid=m1na4.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS m1numact5 FROM tempact WHERE actiontype=5 AND lstacttime<=actiontime+3600*24*30 GROUP BY userid ) AS m1na5 ON tb1.userid=m1na5.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS m1numact6 FROM tempact WHERE actiontype=6 AND lstacttime<=actiontime+3600*24*30 GROUP BY userid ) AS m1na6 ON tb1.userid=m1na6.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS m1numact7 FROM tempact WHERE actiontype=7 AND lstacttime<=actiontime+3600*24*30 GROUP BY userid ) AS m1na7 ON tb1.userid=m1na7.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS m1numact8 FROM tempact WHERE actiontype=8 AND lstacttime<=actiontime+3600*24*30 GROUP BY userid ) AS m1na8 ON tb1.userid=m1na8.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS m1numact9 FROM tempact WHERE actiontype=9 AND lstacttime<=actiontime+3600*24*30 GROUP BY userid ) AS m1na9 ON tb1.userid=m1na9.userid',

                        'LEFT JOIN (SELECT userid,COUNT(*) AS m3numact1 FROM tempact WHERE actiontype=1 AND lstacttime<=actiontime+3600*24*90 GROUP BY userid ) AS m3na1 ON tb1.userid=m3na1.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS m3numact2 FROM tempact WHERE actiontype=2 AND lstacttime<=actiontime+3600*24*90 GROUP BY userid ) AS m3na2 ON tb1.userid=m3na2.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS m3numact3 FROM tempact WHERE actiontype=3 AND lstacttime<=actiontime+3600*24*90 GROUP BY userid ) AS m3na3 ON tb1.userid=m3na3.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS m3numact4 FROM tempact WHERE actiontype=4 AND lstacttime<=actiontime+3600*24*90 GROUP BY userid ) AS m3na4 ON tb1.userid=m3na4.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS m3numact5 FROM tempact WHERE actiontype=5 AND lstacttime<=actiontime+3600*24*90 GROUP BY userid ) AS m3na5 ON tb1.userid=m3na5.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS m3numact6 FROM tempact WHERE actiontype=6 AND lstacttime<=actiontime+3600*24*90 GROUP BY userid ) AS m3na6 ON tb1.userid=m3na6.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS m3numact7 FROM tempact WHERE actiontype=7 AND lstacttime<=actiontime+3600*24*90 GROUP BY userid ) AS m3na7 ON tb1.userid=m3na7.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS m3numact8 FROM tempact WHERE actiontype=8 AND lstacttime<=actiontime+3600*24*90 GROUP BY userid ) AS m3na8 ON tb1.userid=m3na8.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS m3numact9 FROM tempact WHERE actiontype=9 AND lstacttime<=actiontime+3600*24*90 GROUP BY userid ) AS m3na9 ON tb1.userid=m3na9.userid',

                        'LEFT JOIN (SELECT userid,COUNT(*) AS numactall FROM tempact GROUP BY userid ) AS na ON tb1.userid=na.userid',

                        'LEFT JOIN (SELECT userid,COUNT(*) AS numact1 FROM tempact WHERE actiontype=1 GROUP BY userid ) AS na1 ON tb1.userid=na1.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS numact2 FROM tempact WHERE actiontype=2 GROUP BY userid ) AS na2 ON tb1.userid=na2.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS numact3 FROM tempact WHERE actiontype=3 GROUP BY userid ) AS na3 ON tb1.userid=na3.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS numact4 FROM tempact WHERE actiontype=4 GROUP BY userid ) AS na4 ON tb1.userid=na4.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS numact5 FROM tempact WHERE actiontype=5 GROUP BY userid ) AS na5 ON tb1.userid=na5.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS numact6 FROM tempact WHERE actiontype=6 GROUP BY userid ) AS na6 ON tb1.userid=na6.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS numact7 FROM tempact WHERE actiontype=7 GROUP BY userid ) AS na7 ON tb1.userid=na7.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS numact8 FROM tempact WHERE actiontype=8 GROUP BY userid ) AS na8 ON tb1.userid=na8.userid',
                        'LEFT JOIN (SELECT userid,COUNT(*) AS numact9 FROM tempact WHERE actiontype=9 GROUP BY userid ) AS na9 ON tb1.userid=na9.userid',

                        'LEFT JOIN (SELECT userid,AVG(lstacttime-actiontime) AS avgtdact1 FROM tempact WHERE actiontype=1 GROUP BY userid ) AS da1 ON tb1.userid=da1.userid',
                        'LEFT JOIN (SELECT userid,AVG(lstacttime-actiontime) AS avgtdact2 FROM tempact WHERE actiontype=2 GROUP BY userid ) AS da2 ON tb1.userid=da2.userid',
                        'LEFT JOIN (SELECT userid,AVG(lstacttime-actiontime) AS avgtdact3 FROM tempact WHERE actiontype=3 GROUP BY userid ) AS da3 ON tb1.userid=da3.userid',
                        'LEFT JOIN (SELECT userid,AVG(lstacttime-actiontime) AS avgtdact4 FROM tempact WHERE actiontype=4 GROUP BY userid ) AS da4 ON tb1.userid=da4.userid',
                        'LEFT JOIN (SELECT userid,AVG(lstacttime-actiontime) AS avgtdact5 FROM tempact WHERE actiontype=5 GROUP BY userid ) AS da5 ON tb1.userid=da5.userid',
                        'LEFT JOIN (SELECT userid,AVG(lstacttime-actiontime) AS avgtdact6 FROM tempact WHERE actiontype=6 GROUP BY userid ) AS da6 ON tb1.userid=da6.userid',
                        'LEFT JOIN (SELECT userid,AVG(lstacttime-actiontime) AS avgtdact7 FROM tempact WHERE actiontype=7 GROUP BY userid ) AS da7 ON tb1.userid=da7.userid',
                        'LEFT JOIN (SELECT userid,AVG(lstacttime-actiontime) AS avgtdact8 FROM tempact WHERE actiontype=8 GROUP BY userid ) AS da8 ON tb1.userid=da8.userid',
                        'LEFT JOIN (SELECT userid,AVG(lstacttime-actiontime) AS avgtdact9 FROM tempact WHERE actiontype=9 GROUP BY userid ) AS da9 ON tb1.userid=da9.userid',

                        'LEFT JOIN (SELECT userid,MIN(lstacttime-actiontime) AS mintdact1 FROM tempact WHERE actiontype=1 GROUP BY userid ) AS ma1 ON tb1.userid=ma1.userid',
                        'LEFT JOIN (SELECT userid,MIN(lstacttime-actiontime) AS mintdact2 FROM tempact WHERE actiontype=2 GROUP BY userid ) AS ma2 ON tb1.userid=ma2.userid',
                        'LEFT JOIN (SELECT userid,MIN(lstacttime-actiontime) AS mintdact3 FROM tempact WHERE actiontype=3 GROUP BY userid ) AS ma3 ON tb1.userid=ma3.userid',
                        'LEFT JOIN (SELECT userid,MIN(lstacttime-actiontime) AS mintdact4 FROM tempact WHERE actiontype=4 GROUP BY userid ) AS ma4 ON tb1.userid=ma4.userid',
                        'LEFT JOIN (SELECT userid,MIN(lstacttime-actiontime) AS mintdact5 FROM tempact WHERE actiontype=5 GROUP BY userid ) AS ma5 ON tb1.userid=ma5.userid',
                        'LEFT JOIN (SELECT userid,MIN(lstacttime-actiontime) AS mintdact6 FROM tempact WHERE actiontype=6 GROUP BY userid ) AS ma6 ON tb1.userid=ma6.userid',
                        'LEFT JOIN (SELECT userid,MIN(lstacttime-actiontime) AS mintdact7 FROM tempact WHERE actiontype=7 GROUP BY userid ) AS ma7 ON tb1.userid=ma7.userid',
                        'LEFT JOIN (SELECT userid,MIN(lstacttime-actiontime) AS mintdact8 FROM tempact WHERE actiontype=8 GROUP BY userid ) AS ma8 ON tb1.userid=ma8.userid',
                        'LEFT JOIN (SELECT userid,MIN(lstacttime-actiontime) AS mintdact9 FROM tempact WHERE actiontype=9 GROUP BY userid ) AS ma9 ON tb1.userid=ma9.userid',
                        ])

    # obj._executeDB_(dbname='hbc_test',exeline=exeline)
    obj._executeDB_(dbname='hbc_train',exeline='DROP TABLE featuresall')
    obj._executeDB_(dbname='hbc_train',exeline=exeline)

    # print(obj.timetrans(1494239682+3600))