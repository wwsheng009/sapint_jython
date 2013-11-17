# -*- coding: utf-8 -*-

#把mysql-connector-java-5.0.8-bin.jar放在lib文件夹下面
#doc http://www.jython.org/archive/21/docs/zxjdbc.html

from com.ziclix.python.sql import zxJDBC
from dbexts import dbexts
import json

def testdb():
    d, u, p, v = "jdbc:mysql://localhost/sapdb", 'root', "", "org.gjt.mm.mysql.Driver"
    db = zxJDBC.connect(d,u,p,v,CHARSET='utf-8')
    c = db.cursor()
    c.execute("SELECT * FROM makt");
    ret = c.fetchone()
    print 'one record: ', json.dumps(ret).decode('unicode-escape')
#     print cursor.fetchall()
    all = c.fetchall()
    print all
    print 'all records: ' , json.dumps(all).decode('unicode-escape')
    print c.description
    
#   dbextjs usage  
    mysqlcon = dbexts("mysqltest", "D:\jython\lib\dbexts.ini")
    print mysqlcon.table()
    print "中文打印测试"
    
    c.close()
    db.close()
    
if __name__ == "__main__":
    testdb()