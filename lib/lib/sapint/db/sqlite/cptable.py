# -*- coding: utf-8 -*-
# coding=utf-8
import sys
print "File :" , __file__
print "Name :" , __name__
print "Package :" , __package__
print 'getdefaultencoding',sys.getdefaultencoding()

reload(sys)
sys.setdefaultencoding("UTF-8")
print 'getdefaultencoding',sys.getdefaultencoding()
# sys.path.append('D:\\jython\\lib\\sqlite-jdbc-3.7.15-m1.jar')
# sys.path.append('D:\\jython\\lib\\sapjco3.jar')
# sys.path.append('D:\\jython\\lib\\lib')
from datetime import *


import sapint
from sapint.table.CReadTable import CReadTable

from com.ziclix.python.sql import zxJDBC
import json

import sapint.db.sapreadtable as sap



RfcReadTableInterface = 'ZMDM02_RFC_READ_TABLE'


def getDbConnection():
    
    driver = "org.sqlite.JDBC"  
    jdbc_url = "jdbc:sqlite:/d:/wangws/test.db3"
    conn =  zxJDBC.connect(jdbc_url,None ,None , driver)
    return conn
    #conn = getDbConnection()

def testConnection():
    TABLE_NAME      = "planet"
    TABLE_DROPPER   = "drop table if exists %s;"                      % TABLE_NAME
    TABLE_CREATOR   = "create table %s (name, size, solar_distance);" % TABLE_NAME
    RECORD_INSERTER = "insert into %s values (?, ?, ?);"              % TABLE_NAME
    PLANET_QUERY = """
    select name, size, solar_distance
    from %s
    order by size, solar_distance desc
    """ % TABLE_NAME
    
    execute(TABLE_DROPPER)
    execute(TABLE_CREATOR)
    
        

def readTableFields(pDestName,pTableName):
    try:
        table = CReadTable(pDestName)
        table.TableName = pTableName
        return table.GetFieldInfo()
    except AbapException, abex:
        return abex.messageText + " " + abex.key
    except JCoException, e :
        return  e.key + e.message 
    except :
#         traceback.print_exc()
        print sys.exc_info()
        # return sys.exc_info() 
        
def readTableContent(pDestName, pTableName, pFields, pOption, pRows, pDelimiter, pNoData):
    result = {}
#     if sapint.CheckSap(pDestName) == False:
#         print pDestName , "is not availble"
#         return None
    try:
        table = CReadTable(pDestName)
        table.TableName = pTableName
        table.Delimiter = pDelimiter
        if pRows !=None and int(pRows) > 0:
            table.RowCount = pRows
        table.NoData = pNoData
        
        table.FunctionName = RfcReadTableInterface
        
            
        for f in pFields:
            table.AddField(f)
        for o in pOption:
            table.AddCriteria(o)
            
        table.Run();
        result['fields'] = table.GetFields()
        result['data'] = table.GetResult()
#         result['fullfields'] = table.GetFieldInfo()
        return result
    
    except AbapException, abex:
        return abex.messageText + " " + abex.key
    except JCoException, e :
        return  e.key + e.message 
        
    except Exception,e:
#         traceback.print_exc()
        print sys.exc_info()
        raise e
        # return sys.exc_info()        

def getCreateStatment(pTableName, pFields):
    print 'create table ddl'
    if pFields == None:
        print 'Error occurs'
        sys.exit(0)
    max = len(pFields)
    print 'max ', max
    sql = []
    line = ''
    
#     sql.append('DROP TABLE IF EXISTS `{0}`;'.format(pTableName))
    sql.append('create table if not exists {0} ('.format(pTableName))
#     sql.append('id int(11) NOT NULL AUTO_INCREMENT,')
    # print 'create table ',pTableName
    for f in range(max):
        line = getFieldStatment(pFields[f])
        if f != max - 1:
            sql.append(line + ',' )
        else:
            sql.append(line)

    sql.append(') ;')
    stmt = '\n'.join(sql)
    return stmt

def getCreateStatment2(pTableName, pFields):
    print 'getCreateStatment2'
    if pFields == None:
        print 'Error occurs'
        sys.exit(0)
    max = len(pFields)
    print 'max ', max
    sql = []
    line = ''
    
#     sql.append('DROP TABLE IF EXISTS `{0}`;'.format(pTableName))
    sql.append('create table if not exists {0} ('.format(pTableName))
#     sql.append('id int(11) NOT NULL AUTO_INCREMENT,')
    # print 'create table ',pTableName
    keystmt = getPrimaryKey2(pTableName,pFields)
    
    for f in range(max):
        line = getFieldStatment2(pFields[f])
        if f != max - 1:
            print pFields[f][0],
#             if keystmt !='':
            sql.append(line + ',')
        else:
            if keystmt !='':
                sql.append(line + ',' )
                sql.append(keystmt)
            else:
                sql.append(line )

    sql.append(')')
    stmt = '\n'.join(sql)
    return stmt

def getPrimaryKey2(pTable,pFields):
    stmt = ''
    sql = []
    keycount = 0
    for f in pFields:
        if f[32] == 'X':
            keycount = keycount + 1
    
    if keycount > 0:
        sql.append("PRIMARY KEY (".format(pTable))
        for f in range(keycount):
            if pFields[f][32] == 'X':
                if f != keycount - 1:
                    sql.append(pFields[f][1] + ',')
                else:
                    sql.append(pFields[f][1])
        sql.append(')')
        stmt = '\n'.join(sql)
    return stmt
    

def getInsertStatment(pTableName, pFields):
    """根据表内容读取接口返回的字段清单，拼接SQL语句"""
    if pFields == None:
        print 'Error occurs'
        sys.exit(0)
    max = len(pFields)
#     print 'max ', max
    sql = []
    line = ''
    
    sql.append('INSERT INTO {0} ('.format(pTableName))
    for f in range(max):
        if f != max - 1:
            sql.append('"{0}",'.format(pFields[f][0]))
        else:
            sql.append('"{0}"'.format(pFields[f][0]))
    
    sql.append(') VALUES (')
    for f in range(max):
        if f != max - 1:
            sql.append('?,')
        else:
            sql.append('?')
    sql.append(')')
    stmt = ''.join(sql)
    return stmt

def getInsertStatment2(pTableName, pFields):
    """根据DDIF_FIELDINFO_GET返回的字段清单，拼接SQL 语句
            只适合整个表复制的情况，因为返回的值可能只有部分字段
    """
    if pFields == None:
        print 'Error occurs'
        sys.exit(0)
    
    max = len(pFields)
#     print 'max ', max
    sql = []
    line = ''
    
    sql.append('INSERT INTO "{0}" ('.format(pTableName))
    for f in range(max):
        if f != max - 1:
            sql.append('"{0}",'.format(pFields[f][1]))
        else:
            sql.append('"{0}"'.format(pFields[f][1]))
    
    sql.append(') VALUES (')
    for f in range(max):
        if f != max - 1:
            sql.append('?,')
        else:
            sql.append('?')
    sql.append(')')
    stmt = ''.join(sql)
    return stmt


def getFieldStatment(pField):
#     print pField
    stmt = ''
    name = pField[0]
    offs = pField[1]
    leng = int(pField[2])
    type = pField[3]
    text = pField[4]
#     print 'getFieldStatment data type is {0}'.format(type)
    if type == 'C':
        return '{0} varchar2({1})'.format(name, leng)
    elif type == 'D':
        return '{0} date'.format(name)
    elif type == 'T':
        return '{0} time'.format(name)
    elif type == 'P':
        return '{0} number({1})'.format(name, leng)
    elif type == 'F':
        return '{0} float'.format(name)
    elif type == 'N':
        return '{0} varchar2({1})'.format(name, leng)
    elif type == 's':
        return '{0} number({1})'.format(name, leng)
    else:
        print 'field:{0} type:{1} length:{2} text:{3} not justed'.format(name, type, leng, text)
        return '{0} varchar({1}'.format(name, leng)
    return name



    
def getFieldStatment2(pField):
#     print pField
    stmt = ''
    name = pField[1]
    offs = int(pField[4])
    leng = int(pField[8])
    type = pField[13]
    text = pField[4]
    decimals = int(pField[11])
    keyflag = pField[32]
#     print 'getFieldStatment2 data type is {0}'.format(type)
    if type == 'C':
        stmt = '"{0}" varchar2({1})'.format(name, leng * 3)
    elif type == 'D':
        stmt = '"{0}" date'.format(name)
#         return '`{0}` varchar(8)'.format(name)
    elif type == 'T':
        stmt = '"{0}" time'.format(name)
#         return '`{0}` varchar(6)'.format(name)
    elif type == 'P':
        stmt = '"{0}" number({1},{2})'.format(name,leng,decimals)
    elif type == 'I':
        stmt = '"{0}" number({1},{2})'.format(name,leng,decimals)
    elif type == 'F':
        stmt = '"{0}" float(126)'.format(name)
    elif type == 'N':
        stmt = '"{0}" varchar2({1})'.format(name, leng * 3)
    elif type == 's':
        stmt = '"{0}" number({1})'.format(name, leng)
    else:
        print 'field:{0} type:{1} length:{2} text:{3} not justed'.format(name, type, leng, text)
        stmt = '"{0}" varchar2(1)'.format(name, leng * 3)
        
    if keyflag == 'X':
        stmt = stmt + " not null"
    return stmt



def execute(stmt):
    """执行SQL语句，并返回第一行数据"""
    print datetime.now(),  'execute sql statment: ',stmt
    ret = None
    conn = getDbConnection()
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

def clearTableContent(pTable):
    re = execute("delete * from '{0}'").format(pTable)
    if re is None:
        return False
    else:
        return True
    
def checkTableExist(pTable):
    """SQLITE 检查数据库表是否已经存在"""
    print datetime.now(),'check if the table {0} is exist'.format(pTable)
    re = execute("SELECT name FROM sqlite_master WHERE type='table' AND name='{0}'".format(pTable))
    if re is None:
        return False
    else:
        return True
            
def insertData(stmt, data):
    """
                往数据库插入内表数据，在连接MYSQL数据库时，一定要把参数useUnicode=yes&characterEncoding=UTF-8加入，
                否则插入中文时会出否乱码
    """
    conn = getDbConnection()

    with conn:
        with conn.cursor() as c:
            for d in data:
                try:
                    c.executemany(stmt, d)
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

def selectAllData(pTableName):
    """读取所有的表数据"""
    conn = getDbConnection()
    with conn:
        with conn.cursor() as c:
            try:
                c.execute('select * from {0}'.format(pTableName))
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

def createDropTableStatment():
    
    stmt =  """create or replace procedure proc_dropifexists(
        p_table in varchar2 
    ) is
        v_count number(10);
    begin
       select count(*)
       into v_count
       from user_objects
       where object_name = upper(p_table);
    
       if v_count > 0 then
          execute immediate 'drop table ' || p_table ||' purge';
       end if;
    end proc_dropifexists;"""
    

    return execute(stmt)


def dropTable(pTable):
    execute("drop table if exists {0}".format(pTable))
    print datetime.now(),  'Table:{0} dropped!!'.format(pTable)
    
def cleanTable(pTable):
    execute("delete from {0}".format(pTable))
    print datetime.now(),  'Table:{0} cleanned!!'.format(pTable)
    
def createTableByFields(pTableName,fields):
    createSQL = getCreateStatment2(pTableName, fields)
    execute(createSQL)

#     createPrimary = getPrimaryKey2(pTable, fields)
#     if createPrimary != '':
#         execute(createPrimary)
#         print 'add primary key: ',createPrimary
    print datetime.now(), 'Table:{0} created!!'.format(pTableName),createSQL

def createTable(pDestName,pTableName):
    fields = None
    createSQL = None
    fields = readTableFields(pDestName,pTableName)
    createTableByFields(pDestName,fields)
    
def insertTable(pDestName, pTable, pFields, pOption, pRows,pDelimiter,pForce):
    
    table = None
    table = readTableContent(pDestName, pTable, pFields, pOption, pRows, pDelimiter, '')
    if table != None and table['data'] != []:
        insertSQL = None
        insertSQL = getInsertStatment(pTable, table['fields'])
        
        insertData(insertSQL, table['data'])
        print datetime.now(),  'Insert compelete!','lines: ',len(table['data']),insertSQL
    else:
        print datetime.now(),  'Nothing to insert'

def CopySAPTable(pDestName, pTable, pFields, pOption, pRows,pDelimiter,pForce):
    """复制SAP表到MYSQL数据库，在读取表的字段清单时使用读取表内容时返回的字段列表
        读取的速度比函数DDIF_FIELDINFO_GET读取的速度快，但是无法读取字段的小数位
    """
    """step1 read table content"""
    table = readTableContent(pDestName, pTable, pFields, pOption, pRows, pDelimiter, '')
    
    if pForce == True or checkTableExist(pTable) == False:
        
        dropTable(pTable)
        createSQL = getCreateStatment(pTable, table['fields'])
        execute(createSQL)

        print datetime.now(),'table:{0} created!!'.format(pTable),createSQL

    cleanTable(pTable)
    insertSQL = getInsertStatment(pTable, table['fields'])

    insertData(insertSQL, table['data'])
    print datetime.now(), 'Insert compelete!','lines: ',len(table['data']),insertSQL


def CopySAPTable2(pDestName, pTable, pFields, pOption, pRows,pDelimiter,pForce,pAppend):
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
    print datetime.now(),'Begin to copy table {0} from sap {1}'.format(pTable,pDestName)
#     read the table fields info
    tableExist = checkTableExist(pTable)
    if pForce == True or tableExist == False:
        dropTable(pTable)
        createTable(pDestName,pTable)
    else:
        if pAppend !=True:
            cleanTable(pTable)
        
    insertTable(pDestName, pTable, pFields, pOption, pRows,pDelimiter,pForce)

def PushDataBySAP(pTable,pFields,pData,pTableDef,pForce,pAppend):
    
    print datetime.now(),'Begin to copy table {0} from sap {1}'.format(pTable,pDestName)
#     read the table fields info
    tableExist = checkTableExist(pTable)
    if pForce == True or tableExist == False:
        dropTable(pTable)
#         createTable(pDestName,pTable)
        createTableByFields(pTable,pTableDef)
    else:
        if pAppend !=True:
            cleanTable(pTable)
            
#     table = None
#     table = readTableContent(pDestName, pTable, pFields, pOption, pRows, pDelimiter, '')
    if pData!= None: 
        if len(pData) > 0:
            insertSQL = None
            insertSQL = getInsertStatment(pTable,pFields)
            
            insertData(insertSQL, pData)
            print datetime.now(),  'Insert compelete!','lines: ',len(pData),insertSQL
    else:
        print datetime.now(),  'Nothing to insert'
    
def batchCopy(pDestName,pTableList,pDelimiter):
    for t in pTableList:
#         CopySAPTable(pDestName, t,[],[],1000,pDelimiter,True)
#         CopySAPTable(pDestName, t,[],[],None,pDelimiter,True)
#         CopySAPTable('AIP', 'mara',[],[],10,None,True)
        CopySAPTable2('AIP', 'marc',[],[],10,True,True)
if __name__ == '__main__':
    print 'test sqlite'

#     CopySAPTable2('BYD','MARA',[],[],1,None,True)
#     CopySAPTable2('BYD','MARC',[],[],1,None,True)
#     CopySAPTable2('BYD','MARM',[],[],1,None,True)
    CopySAPTable2('AIP','MARA',[],[],1,None,None,False)
#     print json.dumps(selectAllData('MAKT')).decode('unicode-escape')