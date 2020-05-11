import logging
import json


logger = logging.getLogger()
class JsonStrategy():


    def parser(self, line):
        try:
            data = json.loads(line.rstrip('\n|\r'))
            if 'site' in data and  'id' in data:
                return [data["site"], data["id"]]
            else:
                logger.error('Exception JsonStrategy[parser]: formato linea invalido -'+line )    
            #json_record = json.dumps(data, ensure_ascii=False)
        except Exception as e:  
            pass
            #logger.error('Exception JsonStrategy[parser]: formato linea invalido -'+line ) 
        
            

