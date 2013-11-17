# -*- coding: utf-8 -*-

from sapint.db.sqlite import cptable as sqlitedb
from sapint.db.sqlite import cptable as mysqldb
from sapint.db.oracle import cptable as oracledb


def copy_materia(pDest,pMaterial,pAppend):
    cond = """MATNR LIKE '{0}'""".format(pMaterial)
    
    sqlitedb.CopySAPTable2(pDest,'MARA',[],[cond],None,None,False,pAppend)
    sqlitedb.CopySAPTable2(pDest,'MARC',[],[cond],None,None,False,pAppend)
    sqlitedb.CopySAPTable2(pDest,'MARM',[],[cond],None,None,False,pAppend)
    
    sqlitedb.CopySAPTable2(pDest,'MARD',[],[cond],None,None,False,pAppend)
    
#     批量库存
    sqlitedb.CopySAPTable2(pDest,'MCHB',[],[cond],None,None,False,pAppend)
#     供应商的全部的特殊库存
    sqlitedb.CopySAPTable2(pDest,'MSSL',[],[cond],None,None,False,pAppend)
    
#     供应商特殊库存
    sqlitedb.CopySAPTable2(pDest,'MSLB',[],[cond],None,None,False,pAppend)
    
#     客户的特殊库存
    sqlitedb.CopySAPTable2(pDest,'MSKU',[],[cond],None,None,False,pAppend)
    
#     手头的全部客户订单
    sqlitedb.CopySAPTable2(pDest,'MSSA',[],[cond],None,None,False,pAppend)
    
#     销售订单库存
    sqlitedb.CopySAPTable2(pDest,'MSKA',[],[cond],None,None,False,pAppend)
      
#     项目库存总数
    sqlitedb.CopySAPTable2(pDest,'MSSQ',[],[cond],None,None,False,pAppend)
    
#     项目库存
    sqlitedb.CopySAPTable2(pDest,'MSPR',[],[cond],None,None,False,pAppend)

#     供应商的特殊库存
    sqlitedb.CopySAPTable2(pDest,'MKOL',[],[cond],None,None,False,pAppend)
       
#     mysqldb.CopySAPTable2(pDest,'MARA',[],[cond],None,None,False,None)
#     mysqldb.CopySAPTable2(pDest,'MARC',[],[cond],None,None,False,None)
#     mysqldb.CopySAPTable2(pDest,'MARM',[],[cond],None,None,False,None)
#     mysqldb.CopySAPTable2(pDest,'MARD',[],[cond],None,None,False,None)
#     
#     
#     oracledb.CopySAPTable2(pDest,'MARA',[],[cond],None,None,False,None)
#     oracledb.CopySAPTable2(pDest,'MARC',[],[cond],None,None,False,None)
#     oracledb.CopySAPTable2(pDest,'MARM',[],[cond],None,None,False,None)
#     oracledb.CopySAPTable2(pDest,'MARD',[],[cond],None,None,False,None)
    
#     2013@eis

if __name__ == "__main__":
#     copy_materia("BYD","000000000000110052",False)
    sqlitedb.CopySAPTable2("AIP",'MARA',[],[],10000,None,False,False)