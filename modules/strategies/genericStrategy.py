import logging
import abc


logger = logging.getLogger()
class GenericStrategy():

    def __init__(self, limitedColumn):
        self.limitedColumn  = limitedColumn

    def parser(self, line):
        try:
            value = line.split(self.limitedColumn)
            site, id_item = value[0], int(value[1])
            value[0] = value[0].replace('"','')
            return value
        except Exception as e: 
            pass 
            #logger.error('Exception GenericStrategy[parser]: formato linea invalido -'+line )    

