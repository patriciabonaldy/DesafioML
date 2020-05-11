import cx_Oracle
from sqlalchemy import create_engine
import config
from monitor import watch_memory
import datetime
from flask import jsonify

import logging

""" ---------------------------------------------------------------------------------------- """
""" Oracle """


class OracleDB:
    """
       OracleDB Database
    """
    def __init__(self):
        self.username = config.config.user
        self.password = config.config.pwd
        self.ipdb = config.config.connectionstring
        self.engine = None
        self.conn = None
        self.rconn = None
        self.oracle_connection_string = ('oracle+cx_oracle://{username}:{password}@' + self.ipdb  )

    def connect(self):
        try:
            self.engine = create_engine(
                self.oracle_connection_string.format(
                    username=self.username,
                    password=self.password
                ), pool_size=100)
            self.conn = self.engine.connect()
            self.rconn = self.engine.raw_connection()
        except cx_Oracle.DatabaseError as e:
            self.engine = None
            logging.error('Exception: ' + str(e))
            exit(1)


    def get_connection_CX(self):
        """ Obtiene la conexion a Oracle """
        try:
            connection =  cx_Oracle.connect(self.username, self.password, self.ipdb, threaded=False, encoding="UTF-8", nencoding="UTF-8")
            connection.autocommit = False
            return connection
        except Exception as e:
            logging.error('Exception: ' + str(e))


    def connection_close(self):
        self.conn.close()


    def close_connection(self, connection):
        """ Cierra la conexion a Oracle """
        try:
            if connection is not None:
                connection.close()
        except Exception as e:
            logging.error('Exception: ' + str(e))    


    def limpia_none(self, data):
        data_new = []
        for row in data:
            reg = tuple(x if x is not None else '' for x in row)
            data_new.append(reg)
        return data_new  


    def execute_query(self, sqlString):
        self.connect()
        res = self.conn.execute(sqlString).fetchall()
        self.connection_close()
        if len(res):
            res = self.limpia_none(res)
        return res


    def execute_query_CX(self, connection, string_sql):
        try:
            cursor_pe = connection.cursor()
            cursor_pe.execute(string_sql)
            reg = cursor_pe.fetchone()
            cursor_pe.close()
            return reg
        except Exception as e:  
            logging.error('Exception: ' + str(e))    


    def insertar_header_lote(self, df):
        connection = self.get_connection_CX()
        try:            
            cursor_ins = connection.cursor()
            try:
                #obtenemos la secuencia
                string_sql = "SELECT SEQUENCE1.nextval FROM dual"
                seq = self.execute_query_CX(connection, string_sql)
                v_list = list(df)
                v_list[0] = seq[0]
                mapa = tuple(v_list)            
                cursor_ins.execute("""
                    INSERT INTO c##meli1.LOTE_SITE
                        (ID_LOTE, 
                        NOMBRE_ARCHIVO, 
                        ENCODE, 
                        EXTENSION, 
                        SEPARADOR_COLUMNA, 
                        FECHA_INSERCION, 
                        ESTADO)
                    VALUES
                        ( :1, :2, :3, :4, :5, sysdate, :6)
                """, mapa)
                connection.commit()
                return seq[0]
            except Exception as e:
                logging.error('Exception: ' + str(e))
            finally:
                cursor_ins.close()   
        except Exception as e:
            logging.error('Exception: ' + str(e))
        finally:        
            self.close_connection(connection)    


    def insertar_en_detalle_lote_site(self, cursor_ins, reg_count, mapa):
        cursor_ins.executemany("""
            INSERT INTO c##meli1.DETALLE_LOTE_SITE
                (SITE, ID_ITEM, ID_LOTE)
            VALUES
                (:1, :2, :3)
        """,  mapa, batcherrors = True)
        count_error = len(cursor_ins.getbatcherrors())
        reg_count = reg_count + cursor_ins.rowcount
        return reg_count


    def create_paginado(self, id_lote, df):
        mapa = []      
        for row in df:
            watch_memory("oracle", "create_lote_500")
            try:
                if row is None:
                    continue
                if row[0] is None:
                    continue

                row[1] = int(row[1])
                row.append(id_lote)
                mapa.append(tuple(row))
                if len(mapa) == 5000:
                    yield mapa
            except Exception as e:
                #logging.error('Exception: item ivalido' + str(row))
                pass     
        if len(mapa) > 0: 
            yield mapa   


    def save_dataset_to_oracle_bulk(self, connection, id_lote, df):
        """ Guarda el dataframe obtenido en la db cada 100 registros """
        logging.info('saveDataFrameToOracle')
        try:        
            reg_count = 0
            cursor_ins = connection.cursor()
            watch_memory("oracle", "save_dataset_to_oracle_bulk")
            try:          
                reg_count = self.insertar_en_detalle_lote_site(cursor_ins, reg_count, df)   
                connection.commit()                                            
            except cx_Oracle.DatabaseError as e:
                errorObj, = e.args
                logging.error("Row", cursor_ins.rowcount, "has error", errorObj.message)   
            finally:      
                cursor_ins.close()                            
            return reg_count
        except Exception as e:
            logging.error('Exception: ' + str(e))


    def update_estado_lote(self, id_lote, estado):
        """ Guarda el dataframe obtenido en la db cada 100 registros """
        logging.info('update estado')
        connection = self.get_connection_CX()
        cursor_lote = None
        try:        
            values = [estado, id_lote]
            cursor_lote = connection.cursor()
            cursor_lote.execute("UPDATE c##meli1.LOTE_SITE SET ESTADO= :edo WHERE ID_LOTE= :lote",  values)
            connection.commit()   
        except Exception as e:
            logging.error('Exception: ' + str(e))
        finally:      
            cursor_lote.close()   
            self.close_connection(connection)        


if __name__ == '__main__':
    ora = OracleDB()
    ora.connect()
    ora.connection_close()