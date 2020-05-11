from config.database import OracleDB

ora = OracleDB()


def getlotesite(id_lote):
    l_lote = searchlote(id_lote)
    return l_lote


def searchlote(id_lote):
    sqlString = """SELECT ID_LOTE,NOMBRE_ARCHIVO,ENCODE,EXTENSION,SEPARADOR_COLUMNA,FECHA_INSERCION,ESTADO,
                    case when DETALLE is null then ' ' else detalle end DETALLE
                    FROM c##meli1.lote_site """
    if id_lote is not None and id_lote!='' :
        sqlString = sqlString+" where id_lote = {} ".format(id_lote)
    sqlString = sqlString+" order by ID_LOTE "
    
    data_lote = ora.execute_query(sqlString)
    return data_lote
