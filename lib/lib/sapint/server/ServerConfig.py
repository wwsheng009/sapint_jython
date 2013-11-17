# -*- coding: utf-8 -*-
import sys;
#设置JCO的JAR包。
#如果只是本地运行，那就手动设置sapjco3的包的路径
#sys.path.append("E:\\jython\\Lib\\sapjco3.jar");
#把Jco包放在LIB下。

import java.util.HashMap;
import java.util.Properties;
from com.sap.conn.jco import JCoDestination;
from com.sap.conn.jco import JCoDestinationManager;
from com.sap.conn.jco import JCoException;
from com.sap.conn.jco.ext import DataProviderException;
from com.sap.conn.jco.ext import ServerDataEventListener;
from com.sap.conn.jco.ext import ServerDataProvider;

#from com.sap.conn.jco import *

#这个例将会被JCO调用。
class SapServerConfiguration (ServerDataProvider):
    def getServerProperties(self,name):
        if name == "BYD_SAND":
            connectProperties = java.util.Properties();
            connectProperties.setProperty(ServerDataProvider.JCO_REP_DEST,"BYD_SAND");
            connectProperties.setProperty(ServerDataProvider.JCO_GWHOST,"192.168.100.90");
            connectProperties.setProperty(ServerDataProvider.JCO_GWSERV,"sapgw00");
            connectProperties.setProperty(ServerDataProvider.JCO_PROGID,"miniServer");
            connectProperties.setProperty(ServerDataProvider.JCO_SAPROUTER,"");
            connectProperties.setProperty(ServerDataProvider.JCO_CONNECTION_COUNT,"1");
            return connectProperties
        if name == "AIP":
            connectProperties = java.util.Properties();
            connectProperties.setProperty(ServerDataProvider.JCO_REP_DEST,"AIP");
            connectProperties.setProperty(ServerDataProvider.JCO_GWHOST,"");
            connectProperties.setProperty(ServerDataProvider.JCO_GWSERV,"sapgw07");
            connectProperties.setProperty(ServerDataProvider.JCO_PROGID,"miniServer");
            connectProperties.setProperty(ServerDataProvider.JCO_SAPROUTER,"");
            connectProperties.setProperty(ServerDataProvider.JCO_CONNECTION_COUNT,"1");
            return connectProperties
        
    def ChangeEventsSupported(self):
        return 0