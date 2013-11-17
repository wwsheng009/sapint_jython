# -*- coding: utf-8 -*-
from com.ziclix.python.sql import DataHandler


class MySQLDataHandler(DataHandler):
    """statment的类型是java.sql.Statement
        mysql 扩展处理
    """
    
    def __init__(self, handler):
        self.handler = handler
        print 'Inside DataHandler'
    def getPyObject(self, set, col, datatype):
        return self.handler.getPyObject(set, col, datatype)
    def getJDBCObject(self, object, datatype):
        print "handling prepared statement"
        return self.handler.getJDBCObject(object, datatype)
    def preExecute(self, stmt):
        print "calling pre-execute to alter behavior"
        return self.handler.preExecute(stmt)
    def getRowId(self, stmt):
        """自定义读取最后行项目ID的逻辑，返回值"""
        print 'get rowid method name'
        result = stmt.executeQuery('SELECT LAST_INSERT_ID();')
        result.first()
        return result.getInt(1)
    
