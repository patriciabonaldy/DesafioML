from config.database import OracleDB

def getdetaillotesite(id_lote):
    l_detaillotesite = searchDetailLoteSite(id_lote)
    return l_detaillotesite
  
    
def searchDetailLoteSite(id_lote):
    sqlString ="""select id_lote,site,id_item,price,start_time,name,descripcion,nickname
                    from c##meli1.detalle_lote_site"""
    data_detailotesite = []                
    if id_lote is not None and id_lote!='':
        sqlString = sqlString+" where id_lote = {} ".format(id_lote)
        ora = OracleDB()
        data_detailotesite = ora.execute_query(sqlString)
    return data_detailotesite