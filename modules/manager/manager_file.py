import logging
import os
from   os import listdir
from   os.path import isfile, join
from   multiprocessing import Value, Semaphore, Pool
from   modules.manager.pool_worker import apply_async_with_callback, waitPool
from   modules.manager.manager_request import ManagerRequest
from   modules.manager.file_item import FileItem
from   modules.manager.manager_worker import ManagerWorker
from   modules.strategies.strategy import Strategy
from   monitor import profile, watch_memory
from   config import OracleDB
import config 
import time

logger = logging.getLogger()
sem=Semaphore()

class ManagerFile():



    def __init__(self):
        self.oracle = OracleDB()
        self.manager_request = ManagerRequest()
        self.filename = ""
        self.list_files = []
        self.detail_chunks = []
        self.result = []
        self.total_uploaded = 0
        self.typeExtension = ""
        self.limitedColumn = ""
        self.encoding = ""
        self.strategy = None
        self.num_rows = 0
        self.total_rows = 0
        self.total_error_rows = 0
        

    def get_info_file(self, secure_filename, file_chunck):
        file_chunck.seek(0, os.SEEK_END)
        file_length = file_chunck.tell()
        file_chunck.seek(0,0)
        filename = secure_filename(file_chunck.filename) 
        extens = filename.rsplit('.', 1)[1].lower()
        return filename, extens, file_length
        

    def split_File(self, id_lote,file_object): 
        try:
            with open(file_object.fname, 'r', encoding=self.encoding) as f:            
                bloque = f.read()
                file_object.lines = bloque.splitlines() 
            return file_object 
        except FileNotFoundError:  
            raise Exception('No existe el archivo {}'.format(file_object.fname))      
        except Exception as e:
            logger.info("Error obteniendo las lineas del archivo {} ".format(file_object.fname) )
            self.oracle.update_estado_lote(id_lote,'ER')
            raise Exception('Error general, por favor contacte el administrador')


    def guarda_header_lote(self): 
        df = (1,self.filename, 
                self.encoding, 
                self.typeExtension, 
                self.limitedColumn,'PE')

        return self.oracle.insertar_header_lote(df)


    def create_paginado(self):
        mapa = []      
        for f in self.list_files:
            try:
                if f is None:
                    continue
                mapa.append(f)
                if len(mapa) == 2:
                    yield mapa
            except Exception as e:
                #logging.error('Exception: item ivalido' + str(row))
                pass     
        if len(mapa) > 0: 
            yield mapa      
            

    def get_estadisticas(self):
        logger.info("Total split files {}".format(str(len(self.list_files)) )  )
        logger.info("Total regs insertados {}".format(self.num_rows  )  )

    @profile
    def upload_chunks(self, file, filename, extens, limitedLine, limitedColumn, encoding):       
        try:  
            self.remove_files()
            self.filename = filename
            self.typeExtension = extens
            self.limitedLine = limitedLine
            self.limitedColumn = limitedColumn
            self.encoding = encoding            
            self.total_uploaded = 0
            self.strategy = Strategy(extens, limitedColumn, limitedLine)
            
            count_chunks = 0
            stream = file.stream
            n = 0          

            while True:
                chunk = stream.read(int(config.config.max_chunk_size))
                fname = filename.replace("."+extens, str(n)+"."+extens)
                size  = len(chunk)                
                save_path  = os.path.join(config.config.data_dir, fname)
                
                file_chunk = FileItem(save_path, size, encoding, extens, limitedLine, limitedColumn)
                self.list_files.append(file_chunk)
                try:
                    with open(save_path, "wb") as f:
                        n+=1
                        if chunk is not None and size>0:
                            self.total_uploaded += size
                            count_chunks += 1
                            f.write(chunk)   
                            temp = {}
                            temp ['chunk_counts'] = count_chunks
                            temp ['total_bytes']  = self.total_uploaded
                            temp ['status']       = 'uploading...'
                            temp ['success']      = True
                            self.detail_chunks.append(temp)
                        else:
                            f.close()
                            temp = {}
                            temp ['chunk_counts'] = count_chunks
                            temp ['total_bytes']  = self.total_uploaded
                            temp ['status'] = 'DONE'
                            temp ['success'] = True
                            self.detail_chunks.append(temp)
                            break   
                except FileNotFoundError:  
                    raise Exception('No existe el archivo {}'.format(fname))            
                except Exception as e:
                    temp = {}
                    temp ['chunk_counts'] = count_chunks
                    temp ['total_bytes']  = self.total_uploaded
                    temp ['status'] = e
                    temp ['success'] = False
                    self.detail_chunks.append(temp)
        except Exception as e:
            logger.info("Error creando los chunk files" )
            raise Exception('Error general, por favor contacte el administrador') 


    def get_total_uploaded(self):
        return self.total_uploaded


    @profile
    def procesa_file(self,id_lote):
        for f in self.list_files:
            watch_memory("ManagerFile", "procesa_file")    
            self.split_File(id_lote,f)   
            self.process_lines(id_lote, f)
        self.oracle.update_estado_lote(id_lote,'PR')    
        self.get_estadisticas()             

    
    def process_lines(self, id_lote, f):
                
        try:
            pool = Pool()
            self.result = []

            for lines in  f.paginate_lines(): 
                if len(lines)==0:
                    pass
                watch_memory("ManagerFile", "process_lines") 
                if pool._state ==  'CLOSE':
                    pool = Pool()
                result = apply_async_with_callback(pool, id_lote, lines, self.strategy).get(999999999)
                copy_result = result.copy()
                [x.append(id_lote) for x in result ]  
                              
                response_ok = self.get_manager_resquest(copy_result, f)
                self.result.append(response_ok)

            waitPool(pool)     
            if len(self.result) >0:  
                conex = self.oracle.get_connection_CX()    
                logging.info('Archivo {} guardando bloque linea'.format(f.fname))
                try:  
                    for reg in  self.result:
                        blocks = reg.values.tolist()
                        [ n.append(id_lote) for n in blocks]
                        regs = list([ tuple(n) for n in blocks])
                        self.num_rows += self.oracle.save_dataset_to_oracle_bulk(conex, id_lote,regs) 
                    logging.info('--------------------------------------------------------')
                except Exception as econex:
                    logger.info("Error procesando sub-archivo {}".format(f.fname) )  
                finally:
                    self.oracle.close_connection(conex)  
        except Exception as e:
            logger.info("Error procesando sub-archivo {}".format(f.fname) )         


    def get_manager_resquest(self, copy_result, f):
        try:
            id_lote = copy_result[0][2]
            return self.manager_request.get_atributes_item(copy_result)   
        except Exception as e:
            logger.info("Error obteniendo peticiones- archivo {}, id_lote= {}".format(f.fname, id_lote) ) 
            
    
    def remove_files(self):
        onlyfiles = [join(config.config.data_dir, f) for f in listdir(config.config.data_dir) if isfile(join(config.config.data_dir, f))]
        for fname in onlyfiles:
            try:
                if os.path.isfile(fname): # this makes the code more robust
                    os.remove(fname)
            except Exception as e:
                logger.info('Failed to delete %s. Reason: %s' % (fname, e))
