from   modules.manager.pool_worker import  apply_imap
import pandas as pd 
import logging
import time
import json
import requests
from   config import OracleDB



logger = logging.getLogger()

class ManagerRequest():


    def __init__(self):
        self.oracle = OracleDB()
        self.response_list = []
        self.response_failed = []
        self.response_ok = []
        self.items = []
        self.urls_partial = { "base": "https://api.mercadolibre.com",
                              "items": "/items?ids=",
                              "category": "/categories/"
                            }


    def create_list_item(self):
        tope = 20
        rango = round(len(self.items)/tope)
        if len(self.items) <=tope:
            yield ','.join(list(self.items))
        for n in range(rango+1):
            x = (tope*n)
            y = ((n+1)*tope)            
            if n==rango:
                yield ','.join(list(self.items[x:]))
            else:
                yield ','.join(list(self.items[x:y])) 


    def execute_request(self, list_items_url):
        response_list = apply_imap(list_items_url)
        response_failed = []
        list_ok = []
        for resp in response_list:
            if resp.status_code != 200:
                response_failed.append(resp.url.replace(url,"") ) 
            else:
                resp_list = []
                text = resp.text.rstrip('\n|\r|;')
                resp_list = list(json.loads(text))

                response_failed.append(list([ n["body"] for n  in resp_list if n["code"]!=200]) )
                list_ok.append(list([ n["body"] for n  in resp_list if n["code"]==200]))  

        return response_list, response_failed, list_ok               


    def get_category(self):
        try:
            gp_category = self.response_ok.groupby("category_id")["category_id"].count()
            gp_category = list(gp_category.keys())
            list_url_ctg = list([base+url_ctg+ct for ct in gp_category])
            self.response_list_ctg = apply_imap(list_url_ctg)
        except requests.RequestException as re:
            logger.info("No existen los ID en las APIs de ML.") 
            raise requests.RequestException ("No existen los ID en las APIs de ML.")      
        except Exception as e:
            logger.info("Error obteniendo peticiones- ")




        
    def get_atributes_item(self, copy_result):
        try:
            id_lote     = copy_result[0][2]
            base        = self.urls_partial["base"]
            url_items   = self.urls_partial["items"]
            url_ctg     = self.urls_partial["category"]
            self.items  = [reg[0]+reg[1]  for reg in copy_result]
            list_items  = self.create_list_item()
            list_items_url = list( [ base+url_items+n for n in list_items])

            self.response_list, response_failed, list_ok = self.execute_request(list_items_url)
            if len(list_ok) > 0:
                response_ok = pd.DataFrame(list_ok)
                self.response_ok =  response_ok[['id', 'site_id', 'price', 'start_time', 'category_id', 'currency_id', 'seller_id']] 
                self.response_ok['price'] = self.response_ok['price'].fillna(0)
                self.response_ok['currency_id'] = self.response_ok['currency_id'].fillna(0)
                self.get_category()                
            else:
                self.oracle.update_estado_lote(id_lote,'ER')
                raise requests.RequestException ("No existen los ID en las APIs de ML.")
        except requests.RequestException as re:
            logger.info("No existen los ID en las APIs de ML.") 
            raise requests.RequestException ("No existen los ID en las APIs de ML.")      
        except Exception as e:
            logger.info("Error obteniendo peticiones- ")

            