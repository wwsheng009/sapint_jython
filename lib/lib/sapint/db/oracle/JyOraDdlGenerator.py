# -*- coding: utf-8 -*-
#JyOraDdlGenerator.py 
#http://www.cnblogs.com/harrychinese/archive/2011/10/12/My_First_Python_App_To_Generate_Oracle_DDL.html
#http://www.cnblogs.com/harrychinese/archive/2011/10/12/My_First_Python_App_To_Generate_Oracle_DDL.html
'''
Created on 2011-11-10

@author: Harry
OraDdlGenerator Jython edition, using zxJDBC  
'''
from __future__ import with_statement 
from com.ziclix.python.sql import zxJDBC
import sys 


class JyOraDdlGenerator:
    
    def __init__(self):
        self.argConnect=""
        self.argOwner=""
        self.argObjectInFile=""
        self.argOutputPath="" 
        self.objectList=[]
        self.connectusername=""
        self.connectpassword=""
        self.connectserverip=""
        self.connectserverport=""
        self.connectservicename=""

    
        
        
    def getObjectList(self):
        objectList=[] 
        try:
            with open(self.argObjectInFile, "r") as text_file:
                for row in text_file: 
                    objectList.append(row.strip().upper())
        except Exception , ex:
            print(ex) 
     
        return objectList
     
    
    
    def composeSQL(self):
        sql="""SELECT  DBMS_METADATA.GET_DDL(AO.OBJECT_TYPE , AO.OBJECT_NAME, AO.OWNER) DDL_DEF,  AO.OWNER||'.'||AO.OBJECT_NAME FULL_OBJECT_NAME   FROM ALL_OBJECTS  AO
            WHERE 1=1
            AND AO.STATUS='VALID' 
            --AND AO.OWNER like :Owner 
            AND AO.OWNER like '%s'
            --AND AO.OBJECT_NAME LIKE :ObjectName
            AND AO.OBJECT_NAME LIKE '%s'
            AND AO.OWNER NOT IN --to filter out the system schema 
            ( 
            'PUBLIC'
            ,'SYSTEM'
            ,'SYS'
            ,'EXFSYS'
            ,'WMSYS'
            ,'DBSNMP'
            ,'OUTLN'
            ,'ORACLE_OCM'
            ,'ORAAUD'
            )
            AND AO.OBJECT_TYPE IN --DBMS_METADATA.GET_DDL() does not support PACKAGE BODY
            (
            ''
            ,'PROCEDURE'
            ,'TABLE'
            ,'INDEX'
            ,'TYPE'
            ,'VIEW'
            ,'FUNCTION'
            --,'PACKAGE BODY'  
            ,'PACKAGE'
            ,'SEQUENCE'
            )
            """
        return sql
    
    
    def saveDdlToFile(self, fullObjectName, DdlStatement):
        #fullFileName="{path}//{fullObjectName}.sql".format(path=self.argOutputPath,fullObjectName=fullObjectName)
        fullFileName="%s//%s.sql"%(self.argOutputPath,fullObjectName)
        with open(fullFileName, "w") as text_file:
            text_file.write(DdlStatement)
    
    
    def saveRowset(self, cursor):
        rows=cursor.fetchall()
        for DDL_DEF, FULL_OBJECT_NAME in rows:
            #print(DDL_DEF)
            self.saveDdlToFile(FULL_OBJECT_NAME, DDL_DEF)
         
 
    
    def saveDDL(self):
        try:
            driver = "oracle.jdbc.driver.OracleDriver"  
            jdbc_url= "jdbc:oracle:thin:@"+self.connectserverip+":"+self.connectserverport+":"+self.connectservicename                 
            driver = "cn.x.db.oracle.driver.wrapper.CommonDriver"  
            jdbc_url = "jdbc:oracle:thin:@192.168.192.135:1521:orcl?characterEncoding=utf-8"
            
            connection = zxJDBC.connect(jdbc_url, self.connectusername, self.connectpassword, driver)
            generator.normalizeArguments(connection)            
            cursor = connection.cursor()
            sql=self.composeSQL()
            
            if (self.argObjectInFile==""): 
                sql2=sql%(self.argOwner,"%")
                cursor.execute(sql2)
                #cursor.execute(sql, {'Owner':self.argOwner, 'ObjectName':"%"})
                self.saveRowset(cursor)
            else:
                for object1 in self.objectList:
                    sql2=sql%(self.argOwner,object1) 
                    cursor.execute(sql2)
                    #cursor.execute(sql, {'Owner':self.argOwner, 'ObjectName':object1})
                    self.saveRowset(cursor)
        except Exception , ex:
            print ex
        finally:
            if (cursor!=None):
                cursor.close()
            if (connection != None):
                connection.close()    
      

    
 

    def normalizeArguments(self, connection):
        if (self.argOwner=="*"):
            self.argOwner="%"
        elif (self.argOwner=="") and (connection!=None):
            self.argOwner=connection.username              
        if (self.argObjectInFile!=""):
            self.objectList= self.getObjectList()
        self.argOwner=self.argOwner.upper()
            
            
        
        
    
    def printUsage(self):
        usage="""JyOraDdlGenerator is to generate DDL script file for Oracle Object.
Usage:
   JyOraDdlGenerator connection=user/pwd@server_ip:server_port/service_name owner=ownerName objects_in_file=in_file output_path=path
Remark:
   1. If owner=*, it means it will this utility will export objects under all users schema 
   2. If owner option omitted, it means owner=connection.user  
   3. If objects_in_file option omitted, it means this utility will export all objects under the owner schema"""
        print(usage)


 
    def parseArguments(self):
        #sys.argv = ["myscript.py", "connection=user1/pwd1@10.10.141.12:1521/orcl", "owner=s", "objects_in_file=/home/user1/1.txt", "output_path=/home/user1/output"]
        cmdln_args=sys.argv[1:]
        #print(cmdln_args)
        argKeyValues=dict([arg.split("=") for arg in cmdln_args])
        """
        for arg in argKeyValues.iteritems():
            print(arg)
        """
        self.argConnect=argKeyValues["connection"]
        self.connectusername=self.argConnect[:self.argConnect.index("/")]
        self.connectpassword= self.argConnect[self.argConnect.index("/")+1:self.argConnect.index("@")]
        self.connectserverip=self.argConnect[self.argConnect.index("@")+1:self.argConnect.index(":")]
        self.connectserverport=self.argConnect[self.argConnect.index(":")+1:self.argConnect.rindex("/")]
        self.connectservicename=self.argConnect[self.argConnect.rindex("/")+1:]
       
         
        self.argOutputPath=argKeyValues["output_path"]
        
        if (argKeyValues.has_key("owner")):
            self.argOwner=argKeyValues["owner"]
        
        if (argKeyValues.has_key("objects_in_file")):
            self.argObjectInFile=argKeyValues["objects_in_file"]

        
        
        
        
if __name__=="__main__":
    generator=JyOraDdlGenerator()
    parsed=False    
    try:
        generator.parseArguments()
        parsed=True
    except Exception , ex:
        print("Argument parse failed.")
        generator.printUsage()
    if(parsed):               
        generator.saveDDL()
        print("............done")