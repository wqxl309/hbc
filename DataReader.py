import configparser as cp
import datetime as dt
import os
import time

import mysql.connector
import numpy as np
import pandas as pd

from PathInfo import Info,LogMark

class DBreader:

    def __init__(self):
        cfp = cp.ConfigParser()
        cfp.read('.\loginfo.ini')
        self._loginfo = dict(cfp.items('login'))

    def _getConn_(self,dbname):
        conn = mysql.connector.connect(**self._loginfo)
        conn.cursor().execute('USE {0};'.format(dbname))
        print('connected to {0}'.format(dbname))
        return conn

    def csv2db(self,trainORtest,name):
        def strtrans(x):
            x = str(x)
            x = x.strip()
            x = x.replace('\\','')
            return '\''+x+'\''
        def listtrans(x):
            x = str(x)
            x = x.replace('[','')
            x = x.replace(']','')
            x = x.replace('\'','')
            return '\''+x+'\''
        dbname = '_'.join(['hbc',trainORtest])
        conn = self._getConn_(dbname=dbname)
        cursor = conn.cursor()
        datapath = os.path.join('.\data',trainORtest)
        file = os.path.join(datapath,''.join([name,'_',trainORtest,'.csv']))
        data = pd.read_csv(file)
        colinfo = Info.tableinfo[name]['cols']
        for cl in colinfo:
            print(cl)
            if 'TEXT' in colinfo[cl]:
                data[cl] = data[cl].map(strtrans)
        if name=='userComment':
            data['commentsKeyWords'] = data['commentsKeyWords'].map(listtrans)
        data = data.values
        # for dumi in range(data.shape[0]):
        #     if '60' in data[dumi,3]:
        #         print(data[dumi,:])
        self.update_db(conn=conn,
                       dbname=dbname,
                       data=data,
                       tablename=name,
                       colinfo=colinfo,
                       prmkey=Info.tableinfo[name]['prmkey'],
                       if_exist='replace'
                       )


    def update_db(self,conn,dbname,data,tablename,colinfo,prmkey=None,if_exist='nothing',chunksize=1000):
        """ 将 单张表格 数据更新至 指定数据库
            data : np.array of size obsnum*colnum
            colinfo : 列名：列类型的dict
        """
        cursor = conn.cursor()
        # data info
        obsnum = data.shape[0]
        colnames = list(colinfo.keys())
        colnum = len(colnames)
        # saved tables
        cursor.execute('SHOW TABLES;')
        temptbs = cursor.fetchall()
        savedtables = [tb[0] for tb in temptbs if tb[0]!='trddates'] if temptbs else temptbs
        hastable = tablename in savedtables
        if hastable and (if_exist=='nothing'): # 数据表已存在且不会替换
            print('{0}table {1} already in database {2}'.format(LogMark.info,tablename,dbname))
            insertdata = False
        elif (hastable and if_exist=='replace') or (not hastable): # 需要创建新表格
            if hastable: # 需要先删除原表格
                cursor.execute('DROP TABLE {0}'.format(tablename))
                print('{0}table {1} dropped from database {2}'.format(LogMark.info,tablename,dbname))
            ############# 创建表格 #############
            colstr = '('+','.join(['{0} {1}'.format(cn,colinfo[cn]) for cn in colinfo])
            prmkey = ',PRIMARY KEY (' + ','.join(prmkey) + '))' if prmkey else ')'
            egn = 'ENGINE=InnoDB DEFAULT CHARSET=utf8'
            createline = ' '.join(['CREATE TABLE {0} '.format(tablename),colstr,prmkey,egn])
            try:
                cursor.execute(createline)
                print('{0}create table {1} successfully in database {2}'.format(LogMark.info,tablename,dbname))
            except mysql.connector.Error as e:
                print('{0}create table {1} failed in database {2},err : {3}'.format(LogMark.error,tablename,dbname,str(e)))
                raise e
            insertdata = True
        elif hastable and if_exist=='append':
            insertdata = True
        else:
            raise BaseException('if_exist value {0} error'.format(if_exist))
        ############# 插入表格 #############
        if insertdata:
            insertline  = 'INSERT INTO {0} ('.format(tablename) + ','.join(colnames) + ') VALUES '
            try:
                st = time.time()
                chunknum = int(np.ceil(obsnum/chunksize))
                for ck in range(chunknum):
                    head = ck*chunksize
                    tail = min((ck+1)*chunksize,obsnum)
                    chunkdata = data[head:tail,:]
                    toinsert = ','.join([''.join(['(',','.join(['{'+'{0}'.format(i)+'}' for i in range(colnum)]),')']).format(*rowdata) for rowdata in chunkdata])
                    exeline = ''.join([insertline,toinsert])
                    try:
                        cursor.execute(exeline)
                    except BaseException as e:
                        print(chunkdata)
                        raise e
            except BaseException as e:
                if (hastable and if_exist=='replace') or (not hastable): # 需要创建新表格的情况下
                    cursor.execute('DROP TABLE {0}'.format(tablename))  # 如果更新失败需要确保表格删除
                    print('{0}table {1} dropped from database {2}'.format(LogMark.info,tablename,dbname))
                print('{0}update table {1} failed in database {2}, line No.{3} ,err : {4}'.format(LogMark.error,tablename,dbname,ck,str(e)))
                raise e
            else:
                conn.commit()
                print('{0}table updated {1} successfully in database {2} with {3} seconds'.format(LogMark.info,tablename,dbname,time.time()-st))

if __name__=='__main__':
    obj = DBreader()
    obj.csv2db('train','action')
    # print("['很','好','很','主动','中']")