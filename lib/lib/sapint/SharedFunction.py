# -*- coding: utf-8 -*-
import sys
# print sys.path
import json
import itertools
import datetime

# reload(sys)
# sys.setdefaultencoding('utf8') 

# 返回RFC函数的抬头字段列表。
def GetRfcTableHeader(fieldtab):
    print "GetRfcTableHeader"
    headers = []
    iterator = fieldtab.getRecordFieldIterator()
    while iterator.hasNextField():
        # fieldL =[]
        field = iterator.nextRecordField()
        name = field.getName()
        # typeName = field.getTypeAsString()
#         fieldL.append(name)
#         fieldL.append(typeName)
        headers.append(name)
    return headers



# 把rfctable转换成jython List,根据RFC表的列数，动态生成一个JSON数组
def RfcTableToList(fieldtab):
    print "RfcTableToList decode RFC table to python list"
    rows = []
    fieldtab.firstRow()
    # print("Table Rows Count: " + fieldtab.getNumRows())
    types = fieldtab.getMetaData()
    for r in range(fieldtab.getNumRows()):
        fieldtab.setRow(r)
        row = []
        
        for c in range(fieldtab.getFieldCount()):
            # 不应该在这里处理RFC返回的数据，因为这个LIST保存了最原始的信息。
            # print fieldtab.getValue(c)
            # value = fieldtab.getValue(c)
#             if '"' in value:
#                 print value
#                 value = value.replace('"','\\"')
#                 print value
            
            t = types.getType(c)
#             print 'rfc type is ', t
            v = fieldtab.getValue(c)
            nv = None
            if t == 0:
                nv = str(v)
            elif t == 2:
                nv = int(v)
            elif t == 1:
                if v == '00000000':
                    nv = None
                else:
                    nv = date(int(v[0:4]), int(v[4:6]), int(v[6:8]))
            elif t == 3:
                if v == '000000':
                    nv = None
                else:
                    nv = time(int(v[0:2]), int(v[2:4]), int(v[4:6]))
            elif t == 6:
                nv = int(v)
            elif t == 7:
                nv = float(v)
            else:
                nv = v
            row.append(nv)
        rows.append(row)
    return rows

# 合并抬头与内容,并返回新的LIST
def CombineHeaderAndContent(headers, list):
    print "CombineHeaderAndContent to JSON object"
    newlist = None
    newlist = EscapeList(list)
    newheader = headers
#     for element in list:
#             row = []
#             for ele in element:
#                 if '\\' in ele:
#                     ele = ele.replace('\\','\\\\')
#                     print ele
#                 if '"' in ele:
#                     ele = ele.replace('"','\\"')
#                     print ele
#                 row.append(ele)
#             newlist.append(row)
#             
    
    dicList = []
    row = {}
    result = ""
    try:
        for element in newlist:
            # row = dict(zip(headers,element))
            row = dict(itertools.izip(newheader, element))
#             print 'combined row',row
            newrow = {}
            for k, v in row.iteritems():
                if v != None:
                    newrow[k] = row[k]
            
#             print 'newrow ',newrow
            dicList.append(newrow)
#         result = json.dumps(dicList)
#         result = result.decode('unicode-escape')
    except:
        print sys.exc_info()
        # result = list
        result = []
    return dicList


# 根据传入的LIST，把它转换成JS中JSON格式的数组。这个方法返回的数据比ListToJson返回的数据在体积上要小，也更快。
# 但应用方便并不是特别的广，因为很多的JS库并没有针对数组操作的处理。
def EscapeList(plist):
    if isinstance(plist, list) == False:
        print 'Error .. not list'
        return None
    
    print "EscapeList" , len(plist)
    result = ""
    newlist = []
    # 在返回的结果中可能会包含JSON 不兼容的字符，所构造一个新的列表。
    try:
        for element in plist:
            if isinstance(element, list) == False:
                print 'Error .. not list'
                break
            row = []
            for ele in element:
                nele = None
                nele = ele
                
                
                if ele != None:
                    if isinstance(ele, datetime.date):
                        nele = ele.strftime('%Y-%m-%d')
#                         print 'converted type is date: {0}'.format(nele)
                    elif isinstance(ele, datetime.time):
                        nele = ele.strftime('%H:%M:%S')
#                         print 'converted type is time: {0}'.format(nele)
                    elif isinstance(ele, int):
#                         print 'ele is numberd'
                        nele = int(ele)
                    elif isinstance(ele, unicode):
                        if '\\' in ele:
                            nele = ele.replace('\\', '\\\\')
                            print nele
                        if '"' in ele:
                            nele = ele.replace('"', '\\"')
#                             print '\\\\ meet',nele
                            print nele
#                     else:
#                         print 'not converted',ele
                row.append(nele)
            newlist.append(row)
    
#         result = json.dumps(newlist)
#         #这样才可以反解析出正确的中文
#         print 'Finished Convert python list to Json Array'
#         result = result.decode('unicode-escape')
#         print result
#         print "解析结束"
    except Exception, e:
        
        print sys.exc_info()
        raise e
        # result = list
    
    return newlist

# if __name__ == "__main__":
#     print EscapeList([[u'测"试']])