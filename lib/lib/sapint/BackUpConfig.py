# -*- coding: utf-8 -*-
import sys;
#设置JCO的JAR包。
#如果只是本地运行，那就手动设置sapjco3的包的路径
#sys.path.append("E:\\jython\\Lib\\sapjco3.jar");
#把Jco包放在LIB下。
print sys.path

import java.util.HashMap;
import java.util.Properties;
from com.sap.conn.jco import JCoDestination;
from com.sap.conn.jco import JCoDestinationManager;
from com.sap.conn.jco import JCoException;
from com.sap.conn.jco.ext import DataProviderException;
from com.sap.conn.jco.ext import DestinationDataEventListener;
from com.sap.conn.jco.ext import DestinationDataProvider;

#from com.sap.conn.jco import *

#这个例将会被JCO调用。
class BackupDestinationConfiguration (DestinationDataProvider):
    def getDestinationProperties(self,name):
        if name == "LH205":
            connectProperties = java.util.Properties();
            connectProperties.setProperty(DestinationDataProvider.JCO_ASHOST,"192.168.0.205");
            connectProperties.setProperty(DestinationDataProvider.JCO_SYSNR,"00");
            connectProperties.setProperty(DestinationDataProvider.JCO_CLIENT,"500");
            connectProperties.setProperty(DestinationDataProvider.JCO_USER,"wwsheng");
            connectProperties.setProperty(DestinationDataProvider.JCO_PASSWD,"wwsheng");
            connectProperties.setProperty(DestinationDataProvider.JCO_LANG,"zh");
            connectProperties.setProperty(DestinationDataProvider.JCO_SAPROUTER,"" );
            return connectProperties
        if name == "AIP":
            connectProperties = java.util.Properties();
            connectProperties.setProperty(DestinationDataProvider.JCO_ASHOST,"");
            connectProperties.setProperty(DestinationDataProvider.JCO_SYSNR,"07");
            connectProperties.setProperty(DestinationDataProvider.JCO_CLIENT,"800");
            connectProperties.setProperty(DestinationDataProvider.JCO_USER,"wwsheng");
            connectProperties.setProperty(DestinationDataProvider.JCO_PASSWD,"wwsheng");
            connectProperties.setProperty(DestinationDataProvider.JCO_LANG,"zh");
            #connectProperties.setProperty(DestinationDataProvider.JCO_SAPROUTER,"" );
            return connectProperties
        if name == "CRMC":
            connectProperties = java.util.Properties();
            connectProperties.setProperty(DestinationDataProvider.JCO_ASHOST,"192.168.114.21");
            connectProperties.setProperty(DestinationDataProvider.JCO_SYSNR,"00");
            connectProperties.setProperty(DestinationDataProvider.JCO_CLIENT,"206");
            connectProperties.setProperty(DestinationDataProvider.JCO_USER,"aipadm");
            connectProperties.setProperty(DestinationDataProvider.JCO_PASSWD,"123Passw0rd");
            connectProperties.setProperty(DestinationDataProvider.JCO_LANG,"zh");
            #connectProperties.setProperty(DestinationDataProvider.JCO_SAPROUTER,"" );
            return connectProperties
        if name == "BYD":
            connectProperties = java.util.Properties();
            connectProperties.setProperty(DestinationDataProvider.JCO_ASHOST,"192.168.100.11");
            connectProperties.setProperty(DestinationDataProvider.JCO_SYSNR,"00");
            connectProperties.setProperty(DestinationDataProvider.JCO_CLIENT,"800");
            connectProperties.setProperty(DestinationDataProvider.JCO_USER,"ABAPCONST002");
            connectProperties.setProperty(DestinationDataProvider.JCO_PASSWD,"520jj.com.cn");
            connectProperties.setProperty(DestinationDataProvider.JCO_LANG,"zh");

            #connectProperties.setProperty(DestinationDataProvider.JCO_SAPROUTER,"" );
            return connectProperties
        if name == "BYD_SAND":
            connectProperties = java.util.Properties();
            connectProperties.setProperty(DestinationDataProvider.JCO_ASHOST,"192.168.100.90");
            connectProperties.setProperty(DestinationDataProvider.JCO_SYSNR,"00");
            connectProperties.setProperty(DestinationDataProvider.JCO_CLIENT,"800");
            connectProperties.setProperty(DestinationDataProvider.JCO_USER,"wwsheng");
            connectProperties.setProperty(DestinationDataProvider.JCO_PASSWD,"123456");
        
            #connectProperties.setProperty(DestinationDataProvider.JCO_SAPROUTER,"" );
            return connectProperties
    def ChangeEventsSupported(self):
        return 0