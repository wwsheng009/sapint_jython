# -*- coding: utf-8 -*-

import sys

from SapTableToDb import SapTableToDb
from com.ziclix.python.sql import zxJDBC


class SapTableToMysql(SapTableToDb):
    
    def getDbConnection(self):
        driver = "org.gjt.mm.mysql.Driver"  
        jdbc_url = "jdbc:mysql://localhost/sapdb?useUnicode=yes&characterEncoding=utf-8"
        
        username = "root"
        password = ""
        
        conn =  zxJDBC.connect(jdbc_url, username, password, driver)
        return conn

    
    def getCreateStatment(self):
        print 'method getCreateStatment'
        if self.fieldsOut == None:
            print 'Error occurs,no fields'
            sys.exit(0)
        max = len(self.fieldsOut)
        print 'fields count : {0} '.format(max)
        sql = []
        line = ''

        sql.append('CREATE TABLE IF NOT EXISTS `{0}` ('.format(self.tableName))
        sql.append('`id` int(11) NOT NULL AUTO_INCREMENT,')
        for f in range(max):
            line = self.getFieldStatment(self.fieldsOut[f])
#             if f != max - 1:
            sql.append(line + ',' )
#             else:
#                 sql.append(line)
        sql.append('PRIMARY KEY (`id`)')
        sql.append(') ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1 ;')
#         sql.append(') ;')
        stmt = '\n'.join(sql)
        return stmt
    
    def getCreateStatment2(self):
        print 'getCreateStatment2'
        
        if self.tableDef == []:
            raise Exception('Error occurs,table definition is empty')
            
        max = len(self.tableDef)
        print 'max ', max
        sql = []
        line = ''
        
        sql.append('CREATE TABLE IF NOT EXISTS `{0}` ('.format(self.tableName))
#         keystmt = self.getPrimaryKey2(self.tableName,self.tableDef)
        keystmt = ''
        for f in range(max):
            line = self.getFieldStatment2(self.tableDef[f])
            if f != max - 1:
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
    def getPrimaryKey2(self):
        stmt = ''
        sql = []
        keycount = 0
        for f in self.tableDef:
            if f[32] == 'X':
                keycount = keycount + 1
        
        if keycount > 0:
            sql.append("PRIMARY KEY (")
            for f in range(keycount):
                if self.tableDef[f][32] == 'X':
                    if f != keycount - 1:
                        sql.append(self.tableDef[f][1] + ',')
                    else:
                        sql.append(self.tableDef[f][1])
            sql.append(')')
            stmt = '\n'.join(sql)
        return stmt
    
    def getFieldStatment(self,pField):
    #     print pField
        stmt = ''
        name = pField[0]
        offs = pField[1]
        leng = int(pField[2])
        type = pField[3]
        text = pField[4]
    #     print 'getFieldStatment data type is {0}'.format(type)
        if type == 'C':
            stmt = '`{0}` varchar({1})'.format(name, leng * 3)
        elif type == 'D':
            stmt = '`{0}` date'.format(name)
    #         return '`{0}` varchar(8)'.format(name)
        elif type == 'T':
            stmt = '`{0}` time'.format(name)
    #         return '`{0}` varchar(6)'.format(name)
        elif type == 'P':
            stmt = '`{0}` double'.format(name)
        elif type == 'F':
            stmt = '`{0}` float'.format(name)
        elif type == 'N':
            stmt = '`{0}` int({1})'.format(name, leng)
        elif type == 's':
            stmt = '`{0}` int({1})'.format(name, leng)
        else:
            print 'field:{0} type:{1} length:{2} text:{3} not justed'.format(name, type, leng, text)
            stmt = '`{0}` text'.format(name)
        return stmt

    def getFieldStatment2(self,pField):
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
            stmt = '`{0}` varchar({1})'.format(name, leng * 3)
        elif type == 'D':
            stmt = '`{0}` date'.format(name)
    #         return '`{0}` varchar(8)'.format(name)
        elif type == 'T':
            stmt = '`{0}` time'.format(name)
    #         return '`{0}` varchar(6)'.format(name)
        elif type == 'P':
            stmt = '`{0}` double({1},{2})'.format(name,leng,decimals)
        elif type == 'F':
            stmt = '`{0}` float({1},{2})'.format(name,leng,decimals)
        elif type == 'N':
            stmt = '`{0}` int({1})'.format(name, leng)
        elif type == 's':
            stmt = '`{0}` int({1})'.format(name, leng)
        else:
            print 'field:{0} type:{1} length:{2} text:{3} not justed'.format(name, type, leng, text)
            stmt = '`{0}` varchar({1})'.format(name, leng * 3)
            
#         if keyflag == 'X':
#             stmt = stmt + " not null"
        return stmt
    
    def getInsertStatment(self):
        """根据表内容返回的接口返回的字段清单，拼接SQL语句"""
        if self.fieldsOut == None:
            print 'read the table first'
            sys.exit(0)
        max = len(self.fieldsOut)
    #     print 'max ', max
        sql = []
        line = ''
        
        sql.append('INSERT INTO `{0}` ('.format(self.tableName))
        for f in range(max):
            if f != max - 1:
                sql.append('`{0}`,'.format(self.fieldsOut[f][0]))
            else:
                sql.append('`{0}`'.format(self.fieldsOut[f][0]))
        
        sql.append(') VALUES (')
        for f in range(max):
            if f != max - 1:
                sql.append('?,')
            else:
                sql.append('?')
        sql.append(')')
        stmt = ''.join(sql)
        self.insertSQL = stmt
        return stmt

    def getInsertStatment2(self):
        """根据DDIF_FIELDINFO_GET返回的字段清单，拼接SQL 语句
                只适合整个表复制的情况，因为返回的值可能只有部分字段
        """
        if self.tableDef == []:
            print 'get the table def first'
            sys.exit(0)
        
        max = len(pFields)
    #     print 'max ', max
        sql = []
        line = ''
        
        sql.append('INSERT INTO `{0}` ('.format(self.tableName))
        for f in range(max):
            if f != max - 1:
                sql.append('`{0}`,'.format(self.tableDef[f][1]))
            else:
                sql.append('`{0}`'.format(self.tableDef[f][1]))
        
        sql.append(') VALUES (')
        for f in range(max):
            if f != max - 1:
                sql.append('?,')
            else:
                sql.append('?')
        sql.append(')')
        stmt = ''.join(sql)
        self.insertSQL = stmt
        return stmt

    def getDropStatment(self):
        return "DROP TABLE IF EXISTS `{0}`".format(self.tableName)
    def getCheckExistStatment(self):
        return "SHOW TABLES LIKE '{0}'".format(self.tableName)
    def getCleanStatment(self):
        return "delete from {0}".format(self.tableName)
    
if __name__ == "__main__":
    sqlite = SapTableToMysql()
    sqlite.getDbConnection()
    print sqlite.tableName
    sqlite.tableName = 'MAKT'
    sqlite.CopySAPTable([],[],100,True,False)
#     sqlite.CopySAPTable([],[],1,True,False)
#     print sqlite.readTableFields()
#     print sqlite.readTableContent([],[],1)
#     print sqlite.getInsertStatment()
    
#     sqlite.cleanTable()
    print 'finished'