# -*- coding: utf-8 -*-
print 'CReadTable.py imported'
import sys
# from .jy import sapint

# print sys.path
import datetime
from datetime import *
# 相对引用包时，会使用__name__作为相对文件夹,所以一定要考虑代码开始调用的位置。
# 如果在这个模块执行时，__name__就等于，__main__，那就是说，它无法找到相对的上级文件夹。
# 就会提示错误：ValueError: Attempted relative import in non-package
print "File :" , __file__
print "Name :" , __name__
print "Package :" , __package__

# 如果SAPINT不在系统路径中就会报错
import sapint
from sapint import SAPException
import json

from com.sap.conn.jco import JCoTable
from com.sap.conn.jco import JCoFunction
from com.sap.conn.jco import JCoException
from com.sap.conn.jco import AbapException

class CReadTable:
    Delimiter = ''
    __FetchedRows = 0
    __FieldsOut = []

    __FieldsInt = []
    __Options = []
    
    FunctionName = ''
    RowCount = 0
    RowSkip = 0
    TableName = ''
    __WhereClause = ''
    
    __DestName = ''
    __Dest = object

    Result = []
    
    def __init__(self, pSapSystem):
        self.__FieldsInt = []
        self.__Options = []
        self.__FieldsOut = []
        self.Delimiter = ''
        self.FunctionName = ''
        self.RowCount = 0 
        self.RowSkip = 0
        self.NoData = ''
        self.__DestName = pSapSystem
        if self.__DestName !=None and self.__DestName != '':
            self.init()
        
        self.__tableDef = []
    def init(self):
        # 测试目标实例是否可以正常工作。
        print 'check if the sap is running............................... '
        try:
            if sapint.CheckSap(self.__DestName) == False:
                raise SAPException('SAP system ' + self.__DestName + 'is not available')
            self.__Dest = sapint.GetDestination(self.__DestName)
        except JCoException, e:
            print e.key + e.message
#         except:
#             print sys.exc_info()

    def AddCriteria(self, SQL):
        if len(SQL) > 71:
            raise SAPException('SQL too long')
        else:
            self.__Options.append(SQL)
        
    def AddField(self, field):
        self.__FieldsInt.append(field)
    
    def __addWhereLine(self, toptions, whereline):
        toptions.appendRow()
        toptions.setValue("TEXT", whereline)
    
    # #直接返回表结果
    def GetResult(self):
        return self.Result
    # #直接返回字段列表
    def GetFields(self):
        return self.__FieldsOut
    
    def GetFieldsFull(self):
        return self.__tableDef
    def GetResultAsArray(self):
        print '\ncall methond GetResultAsArray()'
        return sapint.SharedFunction.EscapeList(self.Result)
        
    def GetHeaderAsArray(self):
        print '\ncall methond GetHeaderAsArray()'
        return sapint.SharedFunction.EscapeList(self.__FieldsOut)

    def GetResultAsJson(self):
        
        print '\ncall methond GetResultAsJson()'
        header = [x[0] for x in self.__FieldsOut]
        return sapint.SharedFunction.CombineHeaderAndContent(header, self.Result)
    
    
    def GetHeaderAsJson(self):
        """字段抬头"""
        print '\ncall methond GetHeaderAsJson()'
        header = ['FIELDNAME', 'OFFSET', 'LENGTH', 'TYPE', 'FIELDTEXT']
        return sapint.SharedFunction.CombineHeaderAndContent(header, self.__FieldsOut)
    
    
    
    def GetTableAsArray(self):
        print '\ncall methond GetTableAsArray()'
        dictJ = {}
        dictJ['fields'] = sapint.SharedFunction.EscapeList(self.__FieldsOut)
        dictJ['odata'] = sapint.SharedFunction.EscapeList(self.Result)
        dictJ['format'] = 'array'
        return dictJ
    
    def GetTableAsJson(self):
        print '\ncall methond GetTableAsJson()'
        header = ['FIELDNAME', 'OFFSET', 'LENGTH', 'TYPE', 'FIELDTEXT']
        dictJ = {}
        dictJ['fields'] = sapint.SharedFunction.CombineHeaderAndContent(header, self.__FieldsOut)
#         dictJ['fields'] = sapint.SharedFunction.EscapeList(self.__FieldsOut)
        header = [x[0] for x in self.__FieldsOut]
        dictJ['odata'] = sapint.SharedFunction.CombineHeaderAndContent(header, self.Result)
        dictJ['format'] = 'json'
        return dictJ
    
    
    def GetTableAsHtml(self, pOnlyTable):
        print datetime.now(), '==>Convert table content to html'
        html = ''
        strList = []
        if pOnlyTable != True:
            pOnlyTable = False
        if pOnlyTable == False :
            strList.append('<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">') 
            strList.append('<html xmlns="http://www.w3.org/1999/xhtml">')
            strList.append('<head>')
            strList.append('<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />')
            strList.append('</head>')
            strList.append('<body>')
        strList.append('<table class="saptable" id="{0}">'.format(self.TableName))
        # header = [x[0] for x in self.__FieldsOut]
        strList.append('<thead>')
        strList.append('<tr>')
        
        
        for field in self.__FieldsOut:
            strList.append('<th>')
            strList.append(field[0])
            strList.append(field[4])
            strList.append('</th>')
        strList.append('</tr>')
        strList.append('</thead>')
        cols = range(len(self.__FieldsOut))
        
        result = sapint.SharedFunction.EscapeList(self.Result)
        strList.append('<tbody>')
        for row in result:
            strList.append('<tr>')
            for col in cols:
                strList.append('<td>')
                if (row[col] == None):
                    strList.append('')
                else:
                    strList.append(str(row[col]))
                strList.append('</td>')
            strList.append('</tr>')
        strList.append('</tbody>')
        strList.append('</table>')
        if pOnlyTable == False :
            strList.append('</body>')
            strList.append('</html>')
        print datetime.now(), '==>Finished converting table content to html'
        html += ''.join(strList)
        return html
    
    def Run(self):
        
        t1 = None
        t2 = None
        print ''
        
        print datetime.now(), "Begin to read sap table {0} ...........".format(self.TableName)
        try:
            __function = object
            __table1 = object
            __table2 = object
            
            self.__FetchedRows = 0
            if self.RowCount == 0:
                self.RowCount = 299000000 
            if self.FunctionName == '':
                __function = self.__Dest.getRepository().getFunction("RFC_READ_TABLE")
            else:
                __function = self.__Dest.getRepository().getFunction(self.FunctionName)
            
           
            print datetime.now(),"TableName:", self.TableName, 'Rows:', self.RowCount, 'Skip:', self.RowSkip, 'Delimiter:', self.Delimiter
            __function.getImportParameterList().setValue("QUERY_TABLE", self.TableName);
            __function.getImportParameterList().setValue("ROWCOUNT", self.RowCount);
            __function.getImportParameterList().setValue("ROWSKIPS", self.RowSkip);
            __function.getImportParameterList().setValue("DELIMITER", self.Delimiter);
            __function.getImportParameterList().setValue("NO_DATA", self.NoData);

            print datetime.now(),"Table Options.:", self.__Options
            __table1 = __function.getTableParameterList().getTable("OPTIONS");
            __table1.clear()
            for op in self.__Options:
                if op <> '':
                    # print op
                    __table1.appendRow()
                    __table1.setValue("TEXT", op)
            
            print datetime.now(),"Table Fields.:", self.__FieldsInt
            __table2 = __function.getTableParameterList().getTable("FIELDS");
            __table2.clear()
            for fieldIn in self.__FieldsInt:
                if fieldIn <> '':
                    # print fieldIn
                    __table2.appendRow()  
                    __table2.setValue("FIELDNAME", fieldIn)
            
            
            # 开始调用
            t1 = datetime.now()
            print t1 , 'rfc function invoked beginned...........'
            __function.execute(self.__Dest)
            
            
            
            t2 = datetime.now()
            print t2, 'rfc function invoked end.','Duration is:{0}'.format(t2 - t1)
            
            
            self.processRetriveData(__function)
            
            __table6 = __function.getTableParameterList().getTable("ET_FIELDS");
            self.__tableDef = sapint.RfcTableToList(__table6)
#             print 'table definition ',self.__tableDef
            
            
            #小心不要在结构处理之前清除结构，因为它们的引用对象一样
            __table1.clear()
            __table2.clear()
            __table6.clear()
            
        except AbapException, abex:
            return abex.messageText + " " + abex.key
        except JCoException, e :
            return  e.key + e.message 
        except Exception, e:
            print sys.exc_info()
            print e
            raise e
            
        

    def processRetriveData(self,pfunction):
        # 返回的FIELD清单
            print ''
            
            t1 = datetime.now()
            print t1, 'processRetriveData....Converting data..........'
            __table3 = object
            __table4 = object
            __table3 = pfunction.getTableParameterList().getTable("FIELDS");
            __table4 = pfunction.getTableParameterList().getTable("DATA");
            print 'Field count: {0} ,data rows: {1}'.format(__table3.getNumRows(),__table4.getNumRows())
            
            
            __table3.firstRow()
            for rr in range(__table3.getNumRows()):
                __table3.setRow(rr)
                __row3 = []
                for c in range(__table3.getFieldCount()):
                    __row3.append(__table3.getValue(c))
                self.__FieldsOut.append(__row3)

#             print 'Data:' ,__table4
            __table4.firstRow()
            __dataTable = []
            for r in range(__table4.getNumRows()):
                __table4.setRow(r)
                __data = __table4.getValue(0);
                # print __data
                __row = []
                
                if self.Delimiter == '' or self.Delimiter == None:
                    for f in self.__FieldsOut:
                        __fieldName = f[0]
                        __offset = int(f[1])
                        __length = int(f[2])
                        __type = f[3]
                        d = __data[__offset: __offset + __length].strip()
                        try:
#                             print 'data:{0},type:{1},fieldname:{2}'.format(d,__type,__fieldName)
                            d = self.determineValueByType(d, __type, __fieldName)
                        except Exception, e:
                            print "Error Occurs when convert value",__data
                            raise e
                        __row.append(d)
                else:
                    __r = __data.split(self.Delimiter)
                    __row = [e.strip() for e in __r]
#                     print 'data length is :{0},and fields count is {1}'.format(len(__row),len(self.__FieldsOut))
                    __index = 0
                    for _findx in range(len(self.__FieldsOut)):
#                     for f in self.__FieldsOut:
                        f = self.__FieldsOut[_findx]
#                         print f
                        __type = f[3]
                        __fieldName = f[0]
                        try:
                            __row[_findx] = self.determineValueByType(__row[_findx], __type, __fieldName)
                        except Exception, e:
                            print "Error Occurs when convert value",__data
                            raise e
                __dataTable.append(__row)
            
            self.Result = __dataTable
            
            __table3.clear()
            __table4.clear()
            t2 = datetime.now()
            print t2, 'Converting data finished','Duration :{0}'.format(t2 - t1),'Field count: {0},rows count:{1}'.format(len(self.__FieldsOut), len(__dataTable))

    def determineValueByType(self, pInput, pType, pField):
        try:
            d = None
            d = pInput
            type = None
            type = pType
            if type == 'P' or type == 'F':
    #             print 'Convert double or float data...field:{0},type:{1},value:{2}'.format(pField,pType,pInput)
                if d[-1] == '-':
    #                 print d
                    d = d.rstrip('-')
                    d = '-' + d
    #                 print d
                    return float(d)
                else:
                    return float(d)
            elif type == 'D':
    #             print 'Convert DATE ...field:{0},type:{1},value:{2}'.format(pField,pType,pInput)
    #             if d <> '' or d <> None:
    #                 d = d.ljust(8,'0')
                if len(d) <> 8 and (d <> '' and d <> None):
                     
                    raise Exception('Exception date:{0} is not valid'.format(d))      
    #             print d
                
                if d == '00000000':
                    return None
                elif d == '':
                    return None
                else:
                    d = date(int(d[0:4]), int(d[4:6]), int(d[6:8]))
                return d
            
            elif type == 'T':
    #             print 'Convert TIME ...field:{0},type:{1},value:{2}'.format(pField,pType,pInput)
                if len(d) <> 6 and (d <> '' and d <> None):
                    raise Exception(' Exception time:{0} is not valid'.format(d))
    #             if d <> '' or d <> None:
    #                 d = d.ljust(6,'0')
                if d == '000000':
                    return None
                elif d == '':
                    return None
                else:
                    d = time(int(d[0:2]), int(d[2:4]), int(d[4:6]))
                return d
            elif type == 'N':
    #             print 'Convert N ...field:{0},type:{1},value:{2}'.format(pField,pType,pInput)
                if d <> '' and d <> None:
                    return int(d)
            elif type == 'b':
    #             print 'Convert b ...field:{0},type:{1},value:{2}'.format(pField,pType,pInput)
                
                if d <> '' and d <> None :
                    return int(d)
            elif type == 's':
    #             print 'Convert s ...field:{0},type:{1},value:{2}'.format(pField,pType,pInput)
                if d[-1] == '-':
    #                 print d
                    d = d.rstrip('-')
                    d = '-' + d
    #                 print d
                    return int(d)
                else:
                    return int(d)
#                 if d <> '' and d <> None and d <> 0:
#                     return int(d)
            elif type == 'C':
    #             print 'Convert CHAR ...field:{0},type:{1},value:{2}'.format(pField,pType,pInput)
                if d == '':
                    return None
                else:
                    return d
            elif type == 'I':
    #             print 'Convert I ...field:{0},type:{1},value:{2}'.format(pField,pType,pInput)
                if d[-1] == '-':
    #                 print d
                    d = d.rstrip('-')
                    d = '-' + d
    #                 print d
                    return int(d)
                else:
                    return int(d)
#                 if d <> '' and d <> None and d <> 0:
#                     return int(d)
            else:
                print 'Data Not converted ...field:{0},type:{1},value:{2}'.format(pField, pType, pInput)
                return d
        except Exception, e:
            raise Exception("Can't convert data...field:{0},type:{1},value:{2}".format(pField, pType, pInput))
    # 封装SAP系统的标准函数DDIF_FIELDINFO_GET
    def GetFieldInfo(self):
        print "call sap function DDIF_FIELDINFO_GET to get the table infomation"
        try:
            rfcFunFieldInfo = self.__Dest.getRepository().getFunction("DDIF_FIELDINFO_GET")
            rfcFunFieldInfo.getImportParameterList().setValue("TABNAME", self.TableName)
            
            rfcFunFieldInfo.execute(self.__Dest)
        
            fieldtab = rfcFunFieldInfo.getTableParameterList().getTable("DFIES_TAB")
            
            return sapint.RfcTableToList(fieldtab)
            # return jysapint.RfcTableToJson(fieldtab)
        except AbapException, abex:
            return abex.messageText + " " + abex.key
        except JCoException, e :
            return  e.key + e.message 
        except Exception, e:
            print e
            print sys.exc_info()
            # return sys.exc_info()
        


if __name__ == "__main__":
    print 'begin to get the sap table content via rfc function '
# 这里会出现错误
# No module named sapint
# 原因就是没有把sapint的上级目录包含在SYS.PATH里
#     readTable = CReadTable("AIP")
#     readTable.TableName = "MARD"
# #     readTable.RowCount = 100
#     readTable.FunctionName = "ZVI_RFC_READ_TABLE"
#     readTable.Delimiter = '@'
#     readTable.AddCriteria("LABST <> '0'")
#     readTable.Run()
#     
#     print 'no test delimiter'
#     readTable = CReadTable("AIP")
#     readTable.TableName = "MARD"
# #     readTable.RowCount = 100
#     readTable.FunctionName = "ZVI_RFC_READ_TABLE"
# #     readTable.Delimiter =  '@'
#     readTable.AddCriteria("LABST <> '0'")
#     readTable.Run()
    
#     readTable = CReadTable("AIP")
#     readTable.TableName = "MARA"
#     readTable.RowCount = 1
#     readTable.FunctionName = "ZVI_RFC_READ_TABLE"
#     readTable.Run()
#     print readTable.Result
#     readTable.GetFieldInfo()
#     print readTable.GetResultAsJson()
#     print readTable.GetResultAsArray()
#     print readTable.GetTableAsArray()
#     print readTable.GetTableAsJson()
#     print readTable.GetHeaderAsArray()
#     print readTable.GetHeaderAsJson()
#     print readTable.GetTableAsHtml(False)
#     print readTable.GetTableAsHtml(True)  
      
