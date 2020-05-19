from   modules.manager.pool_worker import  apply_imap_request, apply_map_request
from   modules.manager.async_request import AsyncRequest
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

    
    def create_list_item(self, items):
        """
         Retorna un generator de ID's de item paginados de a 20
        """
        tope = 20
        rango = round(len(items)/tope)
        if len(items) <=tope:
            yield ','.join(list(items))
        try:
            for n in range(rango+1):
                x = (tope*n)
                y = ((n+1)*tope)    
                if x > len(items):
                    break;        
                if n==rango:
                    yield ','.join(list(items[x:]))
                else:
                    yield ','.join(list(items[x:y])) 
        except Exception as e:  
            logger.error("Error en metodo create_list_item -"+str(e))     
        


    def execute_request_async(self, type_parse, url, list_items_url):
        """
            Recibe una url base, mas un lote de peticiones
            a la misma, realizando request en paralelo 
            y retornando un data frame con las que esten en estado 200
            las que falle se logean automatica en el archivo app.log
        """      
        response_ok = []  
        async_request = AsyncRequest()
        response_ok = async_request.run_request(list_items_url,type_parse)
        return response_ok

    def get_currencies(self,id_lote):
        """
            A tra ves de un lote de response del servicio /items
            agrupa el data frame por tipo de moneda
            realiza consultas en paralelo al servicio /curriencies
        """
        try:
            base        = self.urls_partial["base"]
            url_ctg     = self.urls_partial["currencies"]
            url         = base+url_ctg
            response_ok = []
            gp_currency = self.response_ok.groupby("currency_id")["currency_id"].count()
            gp_currency = list(gp_currency.keys())
            list_url_sl = list([base+url_ctg+str(ct) for ct in gp_currency if ct !=''])
            list_ok = self.execute_request_async('OTHER', url, list_url_sl)
            time.sleep(5)
            if len(list_ok) > 0:
                for lista in list_ok:
                    valor = lista[['id','description']]
                    response_ok.append(valor.values.tolist()[0])

                self.response_currency_ok = pd.DataFrame(response_ok) 
                self.response_currency_ok.columns = ['currency_id', 'description_currency']
            else:
                self.oracle.update_estado_lote(id_lote,'ER')
                raise requests.RequestException ("No existen los ID en -  {} de ML.".format(url))
        except requests.RequestException as re:
            logger.error("No existen los ID en -  {} de ML.".format(url)) 
            raise requests.RequestException ("No existen los ID en -  {} de ML.".format(url))     
        except Exception as e:
            logger.error("Error obteniendo peticiones- /currencies")



    def get_sellers(self, id_lote):
        """
            A tra ves de un lote de response del servicio /items
            realiza consultas en paralelo al servicio /users
        """
        try:
            base        = self.urls_partial["base"]
            url_ctg     = self.urls_partial["seller"]
            url         = base+url_ctg
            response_ok = []
            gp_sellers = self.response_ok.groupby("seller_id")["seller_id"].count()
            gp_sellers = list(gp_sellers.keys())
            list_url_sl = list([base+url_ctg+str(ct) for ct in gp_sellers])
            list_ok = self.execute_request_async('OTHER', url, list_url_sl)
            time.sleep(5)
            if len(list_ok) > 0:
                for lista in list_ok:
                    valor = lista[['id','nickname']]
                    response_ok.append(valor.values.tolist()[0])

                self.response_seller_ok = pd.DataFrame(response_ok) 
                self.response_seller_ok.columns = ['seller_id', 'nickname']
            else:
                self.oracle.update_estado_lote(id_lote,'ER')
                raise requests.RequestException ("No existen los ID en -  {} de ML.".format(url))
        except requests.RequestException as re:
            logger.error("No existen los ID en -  {} de ML.".format(url)) 
            raise requests.RequestException ("No existen los ID en -  {} de ML.".format(url))     
        except Exception as e:
            logger.error("Error obteniendo peticiones- /sellers")                        


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
            response_ok = []
            gp_category = self.response_ok.groupby("category_id")["category_id"].count()
            gp_category = list(gp_category.keys())
            list_url_ctg = list([base+url_ctg+ct for ct in gp_category])
            list_ok     = self.execute_request_async('OTHER', url, list_url_ctg)
            time.sleep(5)
            if len(list_ok) > 0:
                for lista in list_ok:
                    valor = lista[['id','name']]
                    response_ok.append(valor.values.tolist()[0])

                self.response_ctg_ok = pd.DataFrame(response_ok) 
                self.response_ctg_ok.columns = ['category_id', 'name_catg']
            else:
                raise requests.RequestException ("No existen los ID Category en -  {} de ML.".format(url))
        except requests.RequestException as re:
            logger.error("No existen los ID Category en -  {} de ML.".format(url)) 
            raise requests.RequestException ("No existen los ID en -  {} de ML.".format(url))     
        except Exception as e:
            logger.error("Error obteniendo peticiones- /Categories ")


    def merge_df(self):
        """
            une los data frame currency, category, seller 
            con el dataframe padre (response_ok)
        """
        if len(self.response_ok) == 0:
            return
        else:
            try:
                self.response_ok['id'] = self.response_ok['id'].str.replace(r'\D+', '').astype('int')
                merged_ctg          = pd.merge(left=self.response_ok, right=self.response_ctg_ok, how='left', left_on='category_id', right_on='category_id')
                self.response_ok.drop('category_id', axis=1, inplace=True)          
                if len(self.response_currency_ok) > 0:
                    merged_currency     = pd.merge(left=merged_ctg, right=self.response_currency_ok, how='left', left_on='currency_id', right_on='currency_id')
                    self.response_ok.drop('currency_id', axis=1, inplace=True)    
                    merged_seller       = pd.merge(left=merged_currency, right=self.response_seller_ok, how='left', left_on='seller_id', right_on='seller_id')
                    self.response_ok    = merged_seller
                    self.response_ok.drop('seller_id', axis=1, inplace=True)   
            except Exception as e:
                logger.error("Error en merge_df "+strr(e))    


    def get_atributes_item(self, id_lote, base_url, list_items):
        response_ok = []
        url_items   = self.urls_partial["items"]
        url         = base_url+url_items
        list_items_url = list( [ base_url+url_items+n for n in list_items])
        response_ok = self.execute_request_async('ITEM',url, list_items_url)
        for lista in response_ok:
            self.response_ok += lista.values.tolist()

        if len(self.response_ok) ==0:
            self.oracle.update_estado_lote(id_lote,'ER')
            raise requests.RequestException ("No existen los ID en - {} de ML.".format(url))

        self.response_ok = pd.DataFrame(self.response_ok) 
        self.response_ok.columns = ['id', 'site_id', 'price', 'start_time', 'category_id', 'currency_id', 'seller_id']
        

    def get_all_atributes(self, id_lote, copy_result):
        """
            a traves de una lista de sites y id
            realiza peticiones en paralelo al servicio /items 
            con el response, busca la descripcion de los
            campos currency, category y seller
            retorna un data frame 
        """
        try:
            pd.options.mode.chained_assignment = None
            response_ok = []
            base        = self.urls_partial["base"]            
            self.items  = [ [copy_result[i][j][0]+copy_result[i][j][1] for j in range(len(copy_result[i]))] for i in range(len(copy_result)) ]
            for items in self.items:
                try:
                    list_items  = self.create_list_item(items)
                    self.get_atributes_item(id_lote, base,list_items )                       
                    self.get_currencies(id_lote)                  
                    self.get_sellers(id_lote)   
                    """self.get_categories() 
                    self.merge_df()  
                    self.response_ok['name_catg'].fillna('', inplace=True) 
                    self.response_ok['description_currency'].fillna('', inplace=True) 
                    self.response_ok['nickname'].fillna('', inplace=True)
                    response_ok.append(self.response_ok[['id', 'site_id', 'price', 'start_time', 'name_catg', 'description_currency', 'nickname']] )
                    """
                except Exception as e:
                    logger.error("Error obteniendo atributos- "+str(e))
            return self.response_ok
        except requests.RequestException as re:
            logger.error("No existen los ID en -  {} de ML.".format(url)) 
            raise requests.RequestException ("No existen los ID en -  {} de ML.".format(url))
        except Exception as e:
            logger.error("Error obteniendo atributos- ")
            self.response_ok[['id', 'site_id', 'price', 'start_time', 'name_catg', 'description_currency', 'nickname']]             