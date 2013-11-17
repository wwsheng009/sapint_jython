# -*- coding: utf-8 -*-

import sys
# sys.path.append('D:\\jython\\lib\\sapjco3.jar')
reload(sys)
sys.setdefaultencoding('utf-8')
import sapint
import sapint.server

from sapint.table.CReadTable import CReadTable

# from sapint.db.sqlite import cptable
from sapint.db.SapTableToSqlite import SapTableToSqlite
from sapint.db.SapTableToMysql import SapTableToMysql

from com.sap.conn.jco import JCo;
from com.sap.conn.jco import JCoCustomRepository;
from com.sap.conn.jco import JCoDestinationManager;
from com.sap.conn.jco import JCoException;
from com.sap.conn.jco import JCoFunction;
from com.sap.conn.jco import JCoFunctionTemplate;
from com.sap.conn.jco import JCoListMetaData;
from com.sap.conn.jco import JCoMetaData;
from com.sap.conn.jco import JCoTable;
from com.sap.conn.jco.server import DefaultServerHandlerFactory;
from com.sap.conn.jco.server import JCoServer;
from com.sap.conn.jco.server import JCoServerContext;
from com.sap.conn.jco.server import JCoServerContextInfo;
from com.sap.conn.jco.server import JCoServerErrorListener;
from com.sap.conn.jco.server import JCoServerExceptionListener;
from com.sap.conn.jco.server import JCoServerFactory;
from com.sap.conn.jco.server import JCoServerFunctionHandler;
from com.sap.conn.jco.server import JCoServerState;
from com.sap.conn.jco.server import JCoServerStateChangedListener;
from com.sap.conn.jco.server import JCoServerTIDHandler;

connectionHandler = None

class ReadTableHandler(JCoServerFunctionHandler):
    """sap read table function handler"""
#     def __init__(self,pClient):
#         self.Client = pClient
#         """we need the client """
        
#     @classmethod
#     @staticmethod
#     @classmethod
    def handleRequest(self, serverCtx, function):
        try:
            print '----------------------------------------------------------------'
            print 'call              : ', function.getName()
            print "ConnectionId      : ", serverCtx.getConnectionID()
            print "SessionId         : ", serverCtx.getSessionID()
            print "TID               : ", serverCtx.getTID()
            print "repository name   : ", serverCtx.getRepository().getName()
            print "is in transaction : ", serverCtx.isInTransaction()
            print "is stateful       : ", serverCtx.isStatefulSession()
            print "----------------------------------------------------------------"
            print "gwhost: ", serverCtx.getServer().getGatewayHost()
            print "gwserv: ", serverCtx.getServer().getGatewayService()
            print "progid: ", serverCtx.getServer().getProgramID()
            print "----------------------------------------------------------------"
            print "attributes  : "
            print serverCtx.getConnectionAttributes()
            
            print "----------------------------------------------------------------"
            print "CPIC conversation ID: ", serverCtx.getConnectionAttributes().getCPICConversationID()
            print "----------------------------------------------------------------"
            
            __tableName = function.getImportParameterList().getValue('QUERY_TABLE')
            
            __table1 = function.getTableParameterList().getTable("ET_FIELDS");
            __tableDef = sapint.RfcTableToList(__table1)
#             print 'table fields',__tableDef
            __table = CReadTable('')
            __table.processRetriveData(function)
            __fields = __table.GetFields()
            __data = __table.GetResult()

            
            
            
            print 'Push data to database from sap,table: {0},fields count: {1}, data count: {2}'.format(__tableName,len(__fields),len(__data))
#             adapterSqlite = SapTableToSqlite()
#             adapterSqlite.tableName = __tableName
#             adapterSqlite.PushDataBySAP(__tableName, __fields, __data, __tableDef, True, True)
            
            mysqldb = SapTableToMysql()
            mysqldb.tableName = __tableName
            mysqldb.PushDataBySAP(__tableName, __fields, __data, __tableDef, True, True)
            
            print 'processed ok!'
        except Exception, e:
            print 'error occurs:'
            print sys.exc_info()
            print e
    
            
     
class StateChangedListener(JCoServerStateChangedListener):
    def serverStateChangeOccurred(self, server, oldState, newState):
        print  "Server state changed from " + oldState.toString() + " to " + newState.toString() + " on server with program id " + server.getProgramID()
    
class ThrowableListener(JCoServerErrorListener,JCoServerExceptionListener):
    def serverErrorOccurred(self,jcoServer,connectionId,serverCtx,error):
        print ">>> Error occured on " , jcoServer.getProgramID() , " connection " , connectionId
        error.printStackTrace()
    def serverExceptionOccurred(self,jcoServer, connectionId, serverCtx, error):
        print ">>> Error occured on " + jcoServer.getProgramID() + " connection " + connectionId
        error.printStackTrace();
         
class TIDState:
    CREATED = 0
    EXECUTED = 1
    COMMITTED = 2
    ROLLED_BACK = 3
    CONFIRMED = 4
    
class TIDHandler(JCoServerTIDHandler):
    availableTIDs = {}
    def checkTID(self,serverCtx,tid):
        print "TID Handler: checkTID for " + tid
        state = self.availableTIDs.get(tid)
        if state == None:
            self.availableTIDs[tid] = TIDState.CREATED
            return True
        if state == TIDState.CREATED or state == TIDState.ROLLED_BACK:
            return True
        return False
    def commit(self,serverCtx,tid):
        print "TID Handler: commit for " + tid
        self.availableTIDs[tid] = TIDState.COMMITTED
        
    def rollback(self,serverCtx,tid):
        print "TID Handler: rollback for " + tid
        self.availableTIDs[tid] = TIDState.ROLLED_BACK
    def confirmTID(self,serverCtx,tid):
        print "TID Handler: confirmTID for " + tid
        
        del self.availableTIDs[tid]
    def execute(self,serverCtx):
        tid = serverCtx.getTID()
        if tid != None:
            print "TID Handler: execute for " + tid
            self.availableTIDs[tid] = TIDState.EXECUTED
            
    
        
def startReadTableServer(pServer):
    try:
        server = sapint.server.GetServer(pServer)
        
        connectionHandler = ReadTableHandler()
        factory = DefaultServerHandlerFactory.FunctionHandlerFactory()
        factory.registerHandler("ZVI_RFC_READ_TABLE", connectionHandler)
        server.setCallHandlerFactory(factory)
        
        slistener = StateChangedListener()
        server.addServerStateChangedListener(slistener)
        
        eListener = ThrowableListener()
        server.addServerErrorListener(eListener)
        server.addServerExceptionListener(eListener)

        myTIDHandler = TIDHandler();
        server.setTIDHandler(myTIDHandler);
        
        server.start();
        
        print "The program can be stoped using <ctrl>+<c>"
    except Exception, e:
        print e
        
    
if __name__ == "__main__":
    print 'begin to test the server'
    startReadTableServer("AIP")
#     server = sapint.server.GetServer("AIP")
#     print server.getState()
#     i_cmd = 1
#     while True:
#         s = raw_input('Input [{0:d}] '.format(i_cmd))
    
