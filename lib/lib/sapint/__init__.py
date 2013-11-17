# -*- coding: utf-8 -*-
#作者：王卫生
#时间：2013-3-31
#使用SAPINT 文件
#测试python 与SAP NCO之间的调用
print 'package sapint imported'
import sys
reload(sys)
sys.setdefaultencoding("UTF-8")

__version__ = '1.0.0'
__author__ = 'Vincent wang<vincentwwsheng@gmail.com>'

__all__ = ['BackUpConfig','ShareFunction']

import BackUpConfig;
import SharedFunction;

from datetime import *

from com.sap.conn.jco.ext import Environment;
from com.sap.conn.jco import JCoDestinationManager;

from com.sap.conn.jco import AbapException
from com.sap.conn.jco import JCoException

from BackUpConfig import BackupDestinationConfiguration;




print datetime.now(),"sap initial...."
#初始化配置文件
config = BackupDestinationConfiguration();

#注册配置
Environment.registerDestinationDataProvider(config);
    
    
class SAPException(Exception):
    """ 定义一个SAP出错     """
    def __init__(self,value):
        self.value = value
    def __str__(self):
        return repr(self.value)

#根据名称返回连接实例。
def GetDestination(destinationName):
    try:
        return JCoDestinationManager.getDestination(destinationName)
    except AbapException, abex:
        print abex.messageText + " " + abex.key
        raise e
    except JCoException,e :
        print  e.key + e.message
        raise e
    except Exception,e:
        print e
        print sys.exc_info()
        raise e
        #return sys.exc_info()

#调用前应该检查SAP是否可用。
def CheckSap(destName):
    #设置目标系统的标识，根据标识返回SAP实例。
    #destName = 'AIP'
    destination = GetDestination(destName)
    
    if destination == None:
        print datetime.now(), destName ,"is not available,now returning.........."
        return False
    
    #测试目标实例是否可以正常工作。
    try:
        destination.ping()
        
        print datetime.now(),("Distination " + destName + " works")
        return True
    except JCoException,e:
        print e.key + e.message
        print datetime.now(),("Distination " + destName + " failed")
        return False
    except Exception,e:
        print e
        print sys.exc_info()
        return False


#把rfctable转换成jython数组,根据RFC表的列数，动态生成一个数组
def RfcTableToList(rfcTable):
    return SharedFunction.RfcTableToList(rfcTable);


def RfcTableToArray(rfcTable):
    list = SharedFunction.RfcTableToList(rfcTable)
    return SharedFunction.EscapeList(list);

def RfcTableToJson(rfcTable):
    list = SharedFunction.RfcTableToList(rfcTable)
    headers = SharedFunction.GetRfcTableHeader(rfcTable)
    return SharedFunction.CombineHeaderAndContent(headers,list);

if __name__ == "__main__":
    print "Hei,Judy"
