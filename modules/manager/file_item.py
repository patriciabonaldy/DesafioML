from   monitor import watch_memory
import logging
import os
import time

MEGABYTES =1024*1024
logger = logging.getLogger()


class FileItem():
    
    def __init__(self, fname, size, encoding, typeExtension, limitedLine, limitedColumn):
        #self.lock = Lock()
        self.id_lote = 0
        self.encoding = encoding #'utf-8' 
        self.typeExtension = typeExtension
        self.limitedLine = limitedLine
        self.limitedColumn = limitedColumn 
        self.fname = fname
        self.lines = [] 


    def paginate_lines(self):
        tope = 2500
        rango = round(len(self.lines)/tope)
        if len(self.lines) <=tope:
            yield self.lines
        for n in range(rango):
            x = (tope*n)
            y = ((n+1)*tope)            
            if n==2:
                yield self.lines[x:]
            else:
                yield self.lines[x:y] 
