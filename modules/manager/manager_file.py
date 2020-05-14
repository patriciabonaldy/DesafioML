import logging
import os
from   os import listdir
from   os.path import isfile, join
from   multiprocessing import Value, Semaphore, Pool
from   modules.manager.pool_worker import Worker, MyPool
from   modules.manager.manager_request import ManagerRequest
from   modules.manager.file_item import FileItem
from   modules.manager.manager_worker import ManagerWorker
from   modules.strategies.strategy import Strategy
from   monitor import profile, watch_memory
from   config import OracleDB
import re
import config 
import time

logger = logging.getLogger()
sem=Semaphore()

class ManagerFile():



    def __init__(self):
        self.oracle = OracleDB()
        self.manager_request = ManagerRequest()
        self.lines = []
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
        """
            recibe el lote, y el fichero chunk 
            retorna todas las lineas de un archivo
        """
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
        """
            guarda el id_lote del archivo
        """
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
        """ metodo que se invoca en el route /upload
            recibe el archivo asi como tambien la configuracion escogida por el usuario
            divide el archivos en varios de aprox 300 mbs 
        """    
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
            
            text = stream.read().decode(self.encoding)
            lines = re.split(limitedLine+'|\*|\n', text)
            self.lines = self.paginate_lines(lines)
            self.total_uploaded  = len(text)
        except Exception as e:
            logger.info("Error creando los chunk files" )
            raise Exception('Error general, por favor contacte el administrador') 


    def parse_lines(self, id_lote):
        pool = Pool(8)
        jd = Worker()
        parse = self.strategy.get_strategy().parser
        self.lines = jd.apply_map(pool, id_lote, list(self.lines), parse)         
        pool.close()
        pool.join()


    @profile
    def process_lines2(self, id_lote):
        self.parse_lines(id_lote)  
        self.get_resquest(id_lote) 


    def paginate_lines(self, lines):
        """
            retorna un paginado de lineas de cada 2500 
        """
        tope = 4000
        rango = round(len(lines)/tope)
        if len(lines) <=tope:
            yield lines
        for n in range(rango):
            x = (tope*n)
            y = ((n+1)*tope)            
            if n==2:
                yield lines[x:]
            else:
                yield lines[x:y] 


    def get_total_uploaded(self):
        return self.total_uploaded    
        

    def process_lines(self, id_lote, f):
        """ 
            recibe el id_lote y un objeto de archivo chunk
            procesa las lineas del mism,
            realiza las peticiones a los
            diferentes endpoints y el resultado guarda en BD
        """           
        try:
            pool = Pool()
            self.result = []
            result = []
            jd = Worker()
            parse = self.strategy.get_strategy().parser
            result = jd.apply_map(pool, id_lote, list(f.paginate_lines()), parse) 
            [ [x.append(id_lote) for x in lns] for lns in result ]  
                            
            #response_ok = self.get_manager_resquest(id_lote, result, f)
            #self.result= response_ok
            
            jd.waitPool(pool)     
            """if len(self.result) >0:  
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
                    self.oracle.close_connection(conex)  """
        except Exception as e:
            logger.info("Error procesando sub-archivo {}".format(f.fname) )         

    def get_sub_list(self, i):
        return self.lines[i]


    def get_resquest(self, id_lote):
        try:
            pool = MyPool(3)
            [ [x.append(id_lote) for x in lns] for lns in self.lines ]
            array_length = len(self.lines)
            multi_result = pool.map(self.manager_request.get_atributes_item, self.lines)
            #multi_result = [pool.map(self.manager_request.get_atributes_item, (lns) )  for lns in self.lines].get(99999)
        except Exception as e:
            logger.info("Error obteniendo peticiones- archivo {}, id_lote= {}".format(f.fname, id_lote) ) 
        finally:
            pool.close()
            pool.join()    
            
    
    def remove_files(self):
        """ 
            se invoca antes de dividir el archivo grande por varios pequeños
            limpia e directorio resources/data
        """  
        onlyfiles = [join(config.config.data_dir, f) for f in listdir(config.config.data_dir) if isfile(join(config.config.data_dir, f))]
        for fname in onlyfiles:
            try:
                if os.path.isfile(fname): # this makes the code more robust
                    os.remove(fname)
            except Exception as e:
                logger.info('Failed to delete %s. Reason: %s' % (fname, e))
