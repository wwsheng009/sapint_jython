# -*- coding: utf-8 -*-
# coding=utf-8
import sys
reload(sys)
sys.setdefaultencoding("UTF-8")
print sys.getdefaultencoding()
import sapint
from sapint.table.CReadTable import CReadTable
from datetime import *
from com.ziclix.python.sql import zxJDBC
import json

class SapTableToDb():
    def __init__(self):
        self.tableName = 'mara'
        self.sapClient = 'AIP'
        self.sapServer = 'AIP'
        
        self.fieldsIn = []
        self.fieldsOut = []
        self.tableDef = []
        self.dataOut = []
        
        self.Delimeter = ''
        self.NoData = ''


        self.insertSQL = ''
        self.createSQL = ''
        self.dropSQL = ''
        self.cleanSQL = ''
        
        self.createTableType = '1'

    def CopySAPTable(self,pFields, pOption, pRows,pForce,pAppend):
        """复制SAP表到MYSQL数据库，在读取表的字段清单时使用RFC 函数DDIF_FIELDINFO_GET
                    字段的信息会更完整，但是速度比较慢
            pDestName ,SAP连接
            pTable,表名
            pFields,字段列表
            pOption,条件列表
            pRows,字段条数限制
            pDelimmiter,分隔符
            pForce,重建数据库表
            pAppend,只附加数据，并不是清空
        """
        print ''
        print datetime.now(),'Begin to copy table {0} from sap {1}'.format(self.tableName,self.sapClient)
    #     read the table fields info
        self.readTableContent(pFields, pOption, pRows)
        tableExist = self.checkTableExist()
        if pForce == True or tableExist == False:
            self.dropTable()
            self.createTable()
        else:
            if pAppend !=True:
               self.cleanTable()

        self.insertData()
            
    
    def PushDataBySAP(self,pTable,pFields,pData,pTableDef,pForce,pAppend):
        self.tableName = pTable
        self.dataOut = pData
        self.fieldsOut = pFields
        self.tableDef = pTableDef

        print datetime.now(),'table {0} pushed by sap'.format(self.tableName)
    #     read the table fields info
        tableExist = self.checkTableExist()
        if self.tableDef == [] and tableExist == False:
            raise Exception('the table is not exist and tablestruct is empty')
        
        if pForce == True or tableExist == False:
            self.dropTable()
            self.createTable()
        else:
            if pAppend !=True:
                self.cleanTable()
        
        self.insertData()
    
    def batchCopy(self,pDestName,pTableList,pDelimiter):
        for t in pTableList:
    #         CopySAPTable(pDestName, t,[],[],1000,pDelimiter,True)
    #         CopySAPTable(pDestName, t,[],[],None,pDelimiter,True)
    #         CopySAPTable('AIP', 'mara',[],[],10,None,True)
            CopySAPTable('AIP', 'marc',[],[],10,True,True)
    def readTableFields(self):
        try:
            table = CReadTable(self.sapClient)
            table.TableName = self.tableName
            self.tableDef = table.GetFieldInfo()
        
        except Exception,e :
    #         traceback.print_exc()
            print e
            print sys.exc_info()
            raise e
            # return sys.exc_info() 
    
    def readTableContent(self, pFields, pOption, pRows):
#         result = {}
        self.fieldsIn = pFields
        try:
            table = CReadTable(self.sapClient)
            table.TableName = self.tableName
            table.Delimiter = self.Delimeter
            table.RowCount = pRows
            table.NoData = self.NoData
            
            table.FunctionName = 'ZVI_RFC_READ_TABLE'
            
            for f in pFields:
                table.AddField(f)
            for o in pOption:
                table.AddCriteria(o)
                
            table.Run();
            
            self.fieldsOut = table.GetFields()
            self.dataOut = table.GetResult()
            self.tableDef = table.GetFieldsFull()
            print 'table definition :',self.tableDef
            
        except Exception,e:
            print sys.exc_info()
            raise e


    def execute(self,stmt):
        """执行SQL语句，并返回第一行数据"""
        print datetime.now(),  'execute sql statment: ',stmt
        ret = None
        conn = self.getDbConnection()
        with conn:
            with conn.cursor() as c:
                try:
                    c.execute(stmt)
                    if c.rowcount > 0:
                        ret = c.fetchone()
                except Exception, msg:
                    c.close()
                    conn.rollback()
                    conn.close()
                    print msg
                    raise msg
            
        conn.commit()
        conn.close()
        return ret
    

    def insertData(self):
        """
                    往数据库插入内表数据，在连接MYSQL数据库时，一定要把参数useUnicode=yes&characterEncoding=UTF-8加入，
                    否则插入中文时会出否乱码
        """
        if self.dataOut == None: 
            print datetime.now(),  'Nothing to insert'
#             sys.exit(0)
        if self.dataOut == []:
            print datetime.now(),  'Nothing to insert'
#             sys.exit(0)

        self.getInsertStatment()
        if self.insertSQL == '':
            print datetime.now()
            raise Exception('Failed to crate insertSQL')
#             sys.exit(0)
        conn = self.getDbConnection()
        
        with conn:
            with conn.cursor() as c:
                for d in self.dataOut:
                    try:
                        c.executemany(self.insertSQL, d)
                    except Exception, msg:
                        c.close()
                        print 'Inserting data is ........',d
                        print sys.exc_info()
                        conn.rollback()
                        conn.close()
                        print msg
                        raise msg
            
        conn.commit()
        conn.close()
        print datetime.now(),  'Insert compelete!','lines: ',len(self.dataOut),self.insertSQL


    def selectAllData(self):
        """读取所有的表数据"""
        conn = getDbConnection()
        stmt = getSelectAllStatument()
        with conn:
            with conn.cursor() as c:
                try:
                    c.execute(stmt)
                    if c.rowcount > 0:
                        print 'rows:',c.rowcount
                    else:
                        print 'no rows selected'
                    return c.fetchall()
                except Exception, msg:
                    conn.rollback()
                    conn.close()
                    print msg
                    print sys.exc_info()
                    raise msg
        conn.commit()
        conn.close()
    


    def checkTableExist(self):
        stmt = self.getCheckExistStatment()
        """SQLITE 检查数据库表是否已经存在"""
        print datetime.now(),'check if the table {0} is exist'.format(self.tableName)
        re = self.execute(stmt)
        if re is None:
            print '{0} is not exist'.format(self.tableName)
            return False
        else:
            print '{0} is exist'.format(self.tableName)
            return True

    def dropTable(self):
        stmt = self.getDropStatment()
        self.execute(stmt)
        print datetime.now(),  'Table:{0} dropped!!'.format(self.tableName)
    
    def cleanTable(self):
        if self.checkTableExist() == False:
            return
        stmt = self.getCleanStatment()
        self.execute(stmt)
        print datetime.now(),  'Table:{0} cleanned!!'.format(self.tableName)

    def createTable(self):
        createSQL = self.getCreateStatment2()
        print createSQL
        self.execute(createSQL)
        print datetime.now(), 'Table:{0} created!!'.format(self.tableName),createSQL
        
        
            

    def getInsertStatment(self):
        return None

    def getInsertStatment2(self):
        """根据DDIF_FIELDINFO_GET返回的字段清单，拼接SQL 语句
                只适合整个表复制的情况，因为返回的值可能只有部分字段
        """
        return None
    
    def getDbConnection(self):
        return None
    def getSelectAllStatument(self):
        return 'select * from {0}'.format(self.tableName)
    def getCreateStatment(self):
        return None
    def getCreateStatment2(self):
        return None
    def getDropStatment(self):
        return None
    def getCheckExistStatment(self):
        return None
    def getCleanStatment(self):
        return None

if __name__ == '__main__':
    print 'test'
