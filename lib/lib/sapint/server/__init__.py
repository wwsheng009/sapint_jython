# -*- coding: utf-8 -*-
#作者：王卫生
#时间：2013-3-31
#使用SAPINT 文件
#测试python 与SAP NCO之间的调用

__version__ = '1.0.0'
__author__ = 'Vincent wang<vincentwwsheng@gmail.com>'

__all__ = ['ServerConfig',]

import ServerConfig;

import sys
from datetime import *

from com.sap.conn.jco.ext import Environment;
from com.sap.conn.jco import JCoDestinationManager;
from com.sap.conn.jco.server import JCoServer;
from com.sap.conn.jco.server import JCoServerFactory;
from com.sap.conn.jco import AbapException
from com.sap.conn.jco import JCoException

from ServerConfig import SapServerConfiguration;




print datetime.now(),"sap server initial...."
#初始化配置文件
config = SapServerConfiguration();

#注册配置
if False == Environment.isServerDataProviderRegistered():
    Environment.registerServerDataProvider(config);
else:
    print 'Configuration already registered'
    
#根据名称返回连接实例。
def GetServer(serverName):
    try:
        return JCoServerFactory.getServer(serverName)
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

# #调用前应该检查SAP是否可用。
# def CheckSapServer(serverName):
#     #设置目标系统的标识，根据标识返回SAP实例。
#     #destName = 'AIP'
#     server = GetServer(serverName)
#     
#     if server == None:
#         print datetime.now(), serverName ,"is not available,now returning.........."
#         return False
#     
#     #测试目标实例是否可以正常工作。
#     try:
#         server.
#         
#         print datetime.now(),("Server " + serverName + " works")
#         return True
#     except JCoException,e:
#         print e.key + e.message
#         print datetime.now(),("Server " + serverName + " failed")
#         return False
#     except Exception,e:
#         print e
#         print sys.exc_info()
#         return False

if __name__ == "__main__":
    print "Hei,Judy"
