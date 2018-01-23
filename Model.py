import os
mingw_path = r'C:\My Programs\mingw64\mingw64\bin'
os.environ['PATH'] = mingw_path + ';' + os.environ['PATH']
from xgboost import DMatrix
from xgboost import XGBRegressor
from xgboost import XGBClassifier

import configparser as cp
import mysql.connector
import numpy as np
import pandas as pd
import sklearn.metrics as skm


class BaseModel:

    def __init__(self):
        cfp = cp.ConfigParser()
        cfp.read('.\loginfo.ini')
        self._loginfo = dict(cfp.items('login'))

    def calcIndicators(self,pred,real):
        return skm.roc_auc_score(real, pred)

    def ModelTrain(self,dbname='hbc_train'):
        # 数据仍需进一步构建
        conn = mysql.connector.connect(**self._loginfo)
        cursor = conn.cursor()
        cursor.execute('USE {0};'.format(dbname))
        print('connected to {0}'.format(dbname))


        cursor.execute('SELECT * FROM featuresall')
        rawdata = pd.DataFrame(cursor.fetchall())

        trainData = rawdata.sample(frac=0.8)
        testData = rawdata.iloc[list(set(rawdata.index.values)-set(trainData.index.values))]

        mdlparas =  {'learning_rate' : 0.1,
                     'n_estimators': 500,
                     'min_child_weight': 5,
                     'objective': 'binary:logistic'}

        model = XGBClassifier(**mdlparas)
        print(trainData.shape)
        model.fit(X=trainData.loc[:,2:].values,y=trainData.loc[:,0].values)
        predb = model.predict_proba(testData.loc[:,2:].values)
        print(predb)
        print(self.calcIndicators(pred=predb[:,1],real=testData.loc[:,0].values))

if __name__=='__main__':
    mdl = BaseModel()
    mdl.ModelTrain()