# -*- coding: utf-8 -*-
# coding=utf-8
import sys
print sys.getdefaultencoding()

import sapint
from sapint.table.CReadTable import CReadTable

from com.ziclix.python.sql import zxJDBC
import json


def getDbConnection():
    driver = "oracle.jdbc.driver.OracleDriver"  
    jdbc_url = "jdbc:oracle:thin:@192.168.192.135:1521:orcl"
    "由于ORACLE的JDBC驱动不支持URL里指定encoding,需要使用自定义的包装类进行处理"
    driver = "cn.x.db.oracle.driver.wrapper.CommonDriver"  
    jdbc_url = "jdbc:oracle:thin:@192.168.192.135:1521:orcl?characterEncoding=utf-8"
    
    username = "C##SAPSR3"
    password = "wwsheng"
    
    conn =  zxJDBC.connect(jdbc_url, username, password, driver)
    return conn
    #conn = getDbConnection()


def readTableFields(pDestName,pTableName):
    if sapint.CheckSap(pDestName) == False:
        print pDestName , "is not availble"
        return None
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
    if sapint.CheckSap(pDestName) == False:
        print pDestName , "is not availble"
        return None
    try:
        table = CReadTable(pDestName)
        table.TableName = pTableName
        table.Delimiter = pDelimiter
        table.RowCount = pRows
        table.NoData = pNoData
        
        table.FunctionName = 'ZVI_RFC_READ_TABLE'
        
        for f in pFields:
            table.AddField(f)
        for o in pOption:
            table.AddCriteria(o)
            
        table.Run();
        result['fields'] = table.GetFields()
        result['data'] = table.Result
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
    
    max = len(pFields)
    print 'max ', max
    sql = []
    line = ''
    
#     sql.append('DROP TABLE IF EXISTS `{0}`;'.format(pTableName))
    sql.append('CREATE TABLE {0} ('.format(pTableName))
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
    
    max = len(pFields)
    print 'max ', max
    sql = []
    line = ''
    
#     sql.append('DROP TABLE IF EXISTS `{0}`;'.format(pTableName))
    sql.append('CREATE TABLE {0} ('.format(pTableName))
#     sql.append('id int(11) NOT NULL AUTO_INCREMENT,')
    # print 'create table ',pTableName
    for f in range(max):
        line = getFieldStatment2(pFields[f])
        if f != max - 1:
#             print pFields[f][0],
            sql.append(line + ',')
        else:
            sql.append(line )
#         else:
            # print pFields[f][0]
#             sql.append(line)
            # print pFields[f][4].decode('unicode-escape')
#     print sql
#     stmt = '\n\r'.join(sql)
    
#     sql.append('PRIMARY KEY (`id`)')
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
        sql.append("alter table {0} add constraint pk_{0} primary key  (".format(pTable))
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
    max = len(pFields)
    print 'max ', max
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
    """根据DDIF_FIELDINFO_GET返回的字段清单，拼接SQL 语句"""
    max = len(pFields)
    print 'max ', max
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
    print 'execute sql statment: ',stmt
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
                sys.exit(0)
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
    """MYSQL 检查数据库表是否已经存在"""
    re = execute("SELECT count(*) FROM dba_tables where table_name = '{0}'".format(pTable))
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
                    print sys.exc_info()
                    conn.rollback()
                    conn.close()
                    sys.exit(0)
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
            except Exception,e:
                conn.rollback()
                conn.close()
                print e
                print sys.exc_info()
                raise e
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

def droptable(pTable):
    
    stmt = """    
    BEGIN
       EXECUTE IMMEDIATE 'DROP TABLE {0}';
    EXCEPTION
       WHEN OTHERS THEN
          IF SQLCODE != -942 THEN
             RAISE;
          END IF;
    END;""".format(pTable)
#     execute("call proc_dropifexists('{0}')".format(pTable))
#     print 'Table:{0} dropped!!'.format(pTable)
#     execute(createSQL)
        
    return execute(stmt)

def cleanTable(pTable):
    execute("delete from {0}".format(pTable))
    print 'Table:{0} cleanned!!'.format(pTable)
    

def createTable(pDestName,pTable):
    fields = None
    createSQL = None
    fields = readTableFields(pDestName,pTable)
    createSQL = getCreateStatment2(pTable, fields)
    print 'Create sql:',createSQL
    execute(createSQL)

    createPrimary = getPrimaryKey2(pTable, fields)
    if createPrimary != '':
        execute(createPrimary)
        print 'add primary key: ',createPrimary
        
#     add comment to the column
#     for f in fields:
#         execute("""comment on column "{0}"."{1}" is '{2}'""".format(f[0],f[1],f[27] ))
    
    print 'Table:{0} created!!'.format(pTable)
    
def insertTable(pDestName, pTable, pFields, pOption, pRows,pDelimiter,pForce):
    table = None
    table = readTableContent(pDestName, pTable, pFields, pOption, pRows, pDelimiter, '')
    if table!= None and len(table['data']) > 0:
        insertSQL = None
        insertSQL = getInsertStatment(pTable, table['fields'])
        print 'Insert sql:',insertSQL
        insertData(insertSQL, table['data'])
        print 'Insert compelete!','lines: ',len(table['data'])
    else:
        print 'Nothing to insert'
    
    
def CopySAPTable(pDestName, pTable, pFields, pOption, pRows,pDelimiter,pForce):
    """复制SAP表到MYSQL数据库，在读取表的字段清单时使用读取表内容时返回的字段列表
        读取的速度比函数DDIF_FIELDINFO_GET读取的速度快，但是无法读取字段的小数位
    """
    """step1 read table content"""
    table = readTableContent(pDestName, pTable, pFields, pOption, pRows, pDelimiter, '')
    
    if pForce == True or checkTableExist(pTable) == False:
        createSQL = getCreateStatment(pTable, table['fields'])
        execute("call proc_dropifexists('{0}')".format(pTable))
        print 'table:{0} dropped!!'.format(pTable)
        execute(createSQL)
        print createSQL
        print 'table:{0} created!!'.format(pTable)

    insertSQL = getInsertStatment(pTable, table['fields'])
    
    print insertSQL
    insertData(insertSQL, table['data'])
    print 'Insert compelete!','lines: ',len(table['data'])
#     except Exception,e:
#         raise e
#         print sys.exc_info()

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
#     read the table fields info
    tableExist = checkTableExist(pTable)
    if pForce == True or tableExist == False:
        dropTable(pTable)
        createTable(pDestName,pTable)
    else:
        if pAppend !=True:
            cleanTable(pTable)
        
    insertTable(pDestName, pTable, pFields, pOption, pRows,pDelimiter,pForce)
    
def batchCopy(pDestName,pTableList,pDelimiter):
    for t in pTableList:
#         CopySAPTable(pDestName, t,[],[],1000,pDelimiter,True)
#         CopySAPTable(pDestName, t,[],[],None,pDelimiter,True)
#         CopySAPTable('AIP', 'mara',[],[],10,None,True)
        CopySAPTable2('AIP', 'marc',[],[],10,None,True)
if __name__ == '__main__':


    CopySAPTable2('BYD', 'MAKT',[],["SPRAS = '1'"],100,None,True)
#     CopySAPTable2('BYD', 'MARD',[],[],500,None,True)
#     CopySAPTable2('BYD', 'MARC',[],[],500,None,True)
#     CopySAPTable2('BYD', 'MARM',[],[],500,None,True)
#     print json.dumps(selectAllData('MAKT')).decode('unicode-escape')