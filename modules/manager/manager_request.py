from   modules.manager.pool_worker import  apply_imap_request, apply_map_request
from   multiprocessing import Pool
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
        self.response_ctg_ok = []
        self.response_currency_ok = []
        self.response_seller_ok = []
        self.items = []
        self.urls_partial = { "base"    : "https://api.mercadolibre.com",
                              "items"   : "/items?ids=",
                              "category": "/categories/",
                              "currencies": "/currencies/",
                              "seller"  : "/users/"
                            }

    
    def create_list_item(self):
        """
         Retorna un generator de ID's de item paginados de a 20
        """
        tope = 20
        rango = round(len(self.items)/tope)
        if len(self.items) <=tope:
            yield ','.join(list(self.items))
        try:
            for n in range(rango+1):
                x = (tope*n)
                y = ((n+1)*tope)    
                if x > len(self.items):
                    break;        
                if n==rango:
                    yield ','.join(list(self.items[x:]))
                else:
                    yield ','.join(list(self.items[x:y])) 
        except Exception as e:  
            logger.error("Error en metodo create_list_item -"+str(e))     
        


    def execute_request(self, url, list_items_url):
        """
            Recibe una url base, mas un lote de peticiones
            a la misma, realizando request en paralelo 
            y retornando un data frame con las que esten en estado 200
            las que falle se logean automatica en el archivo app.log
        """
        response_list = apply_imap_request(list_items_url)
        response_failed = []
        response_ok = []
        for resp in response_list:
            if resp.status_code != 200:
                logger.error(str(resp._content) )     
            else:
                resp_list = []                
                try:
                    text = resp.text.rstrip('\n|\r|;')
                    resp_list = json.loads(text)
                    resp_list = list(resp_list)
                    res_failed = list([ n["body"] for n  in resp_list if n["code"]!=200]) 
                    list_ok    = list([ n["body"] for n  in resp_list if n["code"]==200])
                    response_failed  += res_failed
                    response_ok += list_ok  
                except Exception as e:
                    list_ok = json.loads(text) 
                    response_ok.append(list_ok )    

                              

        if len(response_failed)>0:
            logger.error("los siguientes ID no existe en Meli - url:"+url+": "+str(response_failed) ) 
        return response_list, response_failed, response_ok   


    def get_currencies(self):
        """
            A tra ves de un lote de response del servicio /items
            agrupa el data frame por tipo de moneda
            realiza consultas en paralelo al servicio /curriencies
        """
        try:
            base        = self.urls_partial["base"]
            url_ctg     = self.urls_partial["currencies"]
            url         = base+url_ctg
            gp_currency = self.response_ok.groupby("currency_id")["currency_id"].count()
            gp_currency = list(gp_currency.keys())
            list_url_sl = list([base+url_ctg+str(ct) for ct in gp_currency if ct !=''])
            response_currency_ok, response_failed, list_ok = self.execute_request(url, list_url_sl)
            if len(list_ok) > 0:
                response_ok = pd.DataFrame(list_ok)
                self.response_currency_ok =  response_ok[['id','description']]
                self.response_currency_ok.columns = ['currency_id', 'description_currency'] 
            else:
                self.oracle.update_estado_lote(id_lote,'ER')
                raise requests.RequestException ("No existen los ID en -  {} de ML.".format(url))
        except requests.RequestException as re:
            logger.error("No existen los ID en -  {} de ML.".format(url)) 
            raise requests.RequestException ("No existen los ID en -  {} de ML.".format(url))     
        except Exception as e:
            logger.error("Error obteniendo peticiones- ")



    def get_sellers(self):
        """
            A tra ves de un lote de response del servicio /items
            realiza consultas en paralelo al servicio /users
        """
        try:
            base        = self.urls_partial["base"]
            url_ctg     = self.urls_partial["seller"]
            url         = base+url_ctg
            gp_sellers = self.response_ok.groupby("seller_id")["seller_id"].count()
            gp_sellers = list(gp_sellers.keys())
            list_url_sl = list([base+url_ctg+str(ct) for ct in gp_sellers])
            response_list_ctg, response_failed, list_ok = self.execute_request(url, list_url_sl)
            if len(list_ok) > 0:
                response_ok = pd.DataFrame(list_ok)
                self.response_seller_ok =  response_ok[['id','nickname']] 
                self.response_seller_ok.columns = ['seller_id', 'nickname']
            else:
                self.oracle.update_estado_lote(id_lote,'ER')
                raise requests.RequestException ("No existen los ID en -  {} de ML.".format(url))
        except requests.RequestException as re:
            logger.error("No existen los ID en -  {} de ML.".format(url)) 
            raise requests.RequestException ("No existen los ID en -  {} de ML.".format(url))     
        except Exception as e:
            logger.error("Error obteniendo peticiones- ")                        


    def get_categories(self):
        """
            A tra ves de un lote de response del servicio /items
            agrupa el data frame por tipo de categoria
            realiza consultas en paralelo al servicio /categories
        """
        try:
            base        = self.urls_partial["base"]
            url_ctg     = self.urls_partial["category"]
            url         = base+url_ctg
            gp_category = self.response_ok.groupby("category_id")["category_id"].count()
            gp_category = list(gp_category.keys())
            list_url_ctg = list([base+url_ctg+ct for ct in gp_category])
            response_list_ctg, response_failed, list_ok = self.execute_request(url, list_url_ctg)
            if len(list_ok) > 0:
                response_ok = pd.DataFrame(list_ok)
                self.response_ctg_ok =  response_ok[['id','name']] 
                self.response_ctg_ok.columns = ['category_id', 'name_catg']
            else:
                self.oracle.update_estado_lote(id_lote,'ER')
                raise requests.RequestException ("No existen los ID en -  {} de ML.".format(url))
        except requests.RequestException as re:
            logger.error("No existen los ID en -  {} de ML.".format(url)) 
            raise requests.RequestException ("No existen los ID en -  {} de ML.".format(url))     
        except Exception as e:
            logger.error("Error obteniendo peticiones- ")


    def merge_df(self):
        """
            une los data frame currency, category, seller 
            con el dataframe padre (response_ok)
        """
        if len(self.response_ok) == 0:
            return
        else:
            self.response_ok['id'] = self.response_ok['id'].str.replace(r'\D+', '').astype('int')
            merged_ctg          = pd.merge(left=self.response_ok, right=self.response_ctg_ok, how='left', left_on='category_id', right_on='category_id')
            self.response_ok.drop('category_id', axis=1, inplace=True)            

            if len(self.response_currency_ok) > 0:
                merged_currency     = pd.merge(left=merged_ctg, right=self.response_currency_ok, how='left', left_on='currency_id', right_on='currency_id')
                self.response_ok.drop('currency_id', axis=1, inplace=True)
            
                merged_seller       = pd.merge(left=merged_currency, right=self.response_seller_ok, how='left', left_on='seller_id', right_on='seller_id')
                self.response_ok    = merged_seller
                self.response_ok.drop('seller_id', axis=1, inplace=True)        


    def get_atributes_item(self, copy_result):
        """
            a traves de una lista de sites y id
            realiza peticiones en paralelo al servicio /items 
            con el response, busca la descripcion de los
            campos currency, category y seller
            retorna un data frame 
        """
        try:
            id_lote     = copy_result[0][2]
            base        = self.urls_partial["base"]
            url_items   = self.urls_partial["items"]
            url         = base+url_items
            self.items  = [reg[0]+reg[1]  for reg in copy_result]
            list_items  = self.create_list_item()
            list_items_url = list( [ base+url_items+n for n in list_items])
            pool = Pool(2)
            response_list = apply_imap_request(pool, list_items_url)
            pool.close()
            pool.join() 
            """self.response_list, response_failed, list_ok = self.execute_request(url, list_items_url)
            if len(list_ok) > 0:
                response_ok = pd.DataFrame(list_ok)
                self.response_ok =  response_ok[['id', 'site_id', 'price', 'start_time', 'category_id', 'currency_id', 'seller_id']] 
                pd.options.mode.chained_assignment = None
                self.response_ok['price'].fillna(0, inplace=True)
                self.response_ok['currency_id'].fillna('', inplace=True)
                self.get_categories()   
                self.get_currencies()                  
                self.get_sellers()    
                self.merge_df()  
                self.response_ok['name_catg'].fillna('', inplace=True) 
                self.response_ok['description_currency'].fillna('', inplace=True) 
                self.response_ok['nickname'].fillna('', inplace=True)
                       
            else:
                self.oracle.update_estado_lote(id_lote,'ER')
                raise requests.RequestException ("No existen los ID en - {} de ML.".format(url))
            return self.response_ok[['id', 'site_id', 'price', 'start_time', 'name_catg', 'description_currency', 'nickname']] """
            return list_items_url
        except requests.RequestException as re:
            logger.error("No existen los ID en -  {} de ML.".format(url)) 
            raise requests.RequestException ("No existen los ID en -  {} de ML.".format(url))
        except Exception as e:
            logger.error("Error obteniendo peticiones- ")
            self.response_ok[['id', 'site_id', 'price', 'start_time', 'name_catg', 'description_currency', 'nickname']] 

            