# -*- coding: utf-8 -*-

from com.ziclix.python.sql import zxJDBC as zxJDBCA

from dbexts import dbexts
import json
import sys
# sys.setdefaultencoding('utf-8')

from PyHandler import PyHandler

# sys.setdefaultencoding('utf8')
def getCodeFolder(id):
    #     try:
    ret = None
    d,u,p,v = "jdbc:mysql://localhost/cmd_main",'root',"","org.gjt.mm.mysql.Driver"
    with zxJDBCA.connect(d,u,p,v,CHARSET='utf-8') as conn:
        with conn:
            with conn.cursor() as c:
                c.datahandler = PyHandler(c.datahandler)
                pId = id;
                if pId is None:
                    pId = ''
                    
                str = "select * from codefolder where parentid = '" + pId + "'"
                print 'get folder sql string ',str
#                 str = "select * from codefolder "
#                 print str
                c.execute(str)
    #         print json.dumps(c.fetchall()).decode('unicode-escape')
#                 print c.rowcount
#                 while (c.next()):
                all = c.fetchall()
                print 'get all folder:....',all
                
#                 print json.dumps(all).decode('unicode-escape')
                return json.dumps(all).decode('unicode-escape')
#                 for r in all:
#                     print json.dumps(r).decode('unicode-escape')
#                 while True:
#                     row = c.fetchone()
#                     if row is None:
#                         break
#                     else:
#                         ret = json.dumps(row).decode('unicode-escape')
#                         print 'return................',row,ret
#                         return row


def getCodeList(folderId):
    d,u,p,v = "jdbc:mysql://localhost/cmd_main",'root',"","org.gjt.mm.mysql.Driver"
    with zxJDBCA.connect(d,u,p,v,CHARSET='utf-8') as conn:
        with conn:
            with conn.cursor() as c:
                c.datahandler = PyHandler(c.datahandler)
                pId = id;
                if not pId:
                    pId = ''
                str = "select id from code where treeid = '" + folderId + "'"
#                 str = "select * from codefolder "
                print str
                c.execute(str)
    #         print json.dumps(c.fetchall()).decode('unicode-escape')
                print c.rowcount
#                 while (c.next()):
                all = c.fetchall()
                  
                print json.dumps(all).decode('unicode-escape')
#                  for r in all:
#                      print json.dumps(r).decode('unicode-escape')
#                 while True:
#                     row = c.fetchone()
#                     if row is None:
#                         break
#                     print row
                
#                     print json.dumps(row).decode('unicode-escape')
def getCode(id):
    d,u,p,v = "jdbc:mysql://localhost/cmd_main",'root',"","org.gjt.mm.mysql.Driver"
    with zxJDBCA.connect(d,u,p,v,CHARSET='utf-8') as conn:
        with conn:
            with conn.cursor() as c:
                c.datahandler = PyHandler(c.datahandler)
                pId = id;
                if not pId:
                    pId = ''
                str = "select * from code where id = '" + pId + "'"
#                 str = "select * from codefolder "
#                 print str
                c.execute(str)
#                 print json.dumps(c.fetchall()).decode('unicode-escape')
                print c.rowcount
#                 while (c.next()):
                one = c.fetchone()
                print one
#                 print json.dumps(all).decode('unicode-escape')
#                  for r in all:
#                      print json.dumps(r).decode('unicode-escape')
#                 while True:
#                     row = c.fetchone()
#                     if row is None:
#                         break
#                     print row
                
#                     print json.dumps(row).decode('unicode-escape')
if __name__ == "__main__":
    print 'haha'
    getCodeFolder('')
#     getCodeFolder('046535cf-2081-42ca-b472-5f04fd93dfac')
#     getCodeList('046535cf-2081-42ca-b472-5f04fd93dfac')
#     getCode('15f6c1c3-4717-4500-81de-24e48953625a')