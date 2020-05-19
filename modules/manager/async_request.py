#!/usr/bin/env python3

"""Asynchronously get links embedded in multiple pages' HMTL."""

import asyncio
import logging
import sys
from   typing import IO
import aiohttp
from   aiohttp import ClientSession
import pandas as pd 
import json

logger = logging.getLogger()

class AsyncRequest:

    def __init__(self):
        self.result = []

    async def fetch_request(self, url: str, session: ClientSession, **kwargs):
        """GET request wrapper to fetch page HTML.

        kwargs are passed to `session.request()`.
        """
        resp = await session.request(method="GET", url=url, **kwargs)
        resp.raise_for_status()
        logger.info("Got response [%s] for URL: %s", resp.status, url)
        html = await resp.text()
        return html


    async def parse(self, type_parse, response, **kwargs):
        try:
            response_ok =  []
            try:
                response_failed, resp_ok  = [],[]
                text        = response.rstrip('\n|\r|;')
                resp_list   = json.loads(text)
                resp_list   = list(resp_list)
                res_failed  = list([ n["body"] for n  in resp_list if n["code"]!=200]) 
                list_ok     = list([ n["body"] for n  in resp_list if n["code"]==200])
                response_failed  += res_failed
                resp_ok  += list_ok
                
            except Exception as e:
                list_ok = json.loads(response.rstrip('\n|\r|;')) 
                response_ok = pd.json_normalize(list_ok)

            if type_parse == 'ITEM':
                list([ resp_ok[i] for i in range(len(resp_ok))  if resp_ok[i] is not None] ) 
                response_ok     = pd.DataFrame(resp_ok) 
                response_ok =  response_ok[['id', 'site_id', 'price', 'start_time', 'category_id', 'currency_id', 'seller_id']]   
                response_ok['price'].fillna(0, inplace=True)
                response_ok['currency_id'].fillna('', inplace=True) 

        except Exception as e:
            logger.error(str(e))  

        if len(response_failed)>0:
            logger.error("los siguientes ID no existe en Meli - url:"+str(response_failed) ) 

        return response_ok


    async def request_multiple_urls(self, url: str, type_parse: str, session: ClientSession, **kwargs) -> set:
        """Find HREFs in the HTML of `url`."""
        response_failed, response_ok = [],[]
        try:
            logger.info(url)
            response = await self.fetch_request(url=url, session=session, **kwargs)
            
            response_ok = await self.parse(type_parse, response, **kwargs)
        except (aiohttp.ClientError,aiohttp.http_exceptions.HttpProcessingError,) as e:
            logger.error("aiohttp exception for %s [%s]: %s", url,getattr(e, "status", None),getattr(e, "message", None),)
        except Exception as e:
            logger.exception("Non-aiohttp exception occured:  %s", getattr(e, "__dict__", {}))
        
        return response_ok


    async def bulk_urls(self, urls: set, type_parse, **kwargs):
        """Crawl & write concurrently to `file` for multiple `urls`."""
        tasks = []
        async with ClientSession() as session:        
            for url in urls:
                tasks.append(
                    self.request_multiple_urls(url=url, type_parse = type_parse, session=session, **kwargs)
                )
            self.result = await asyncio.gather(*tasks)
            resp_ok = list([ self.result[i] for i in range(len(self.result))  if len(self.result[i]) > 0 ] ) 
            self.result = resp_ok
        

    def run_request(self, urls, type_parse):
        asyncio.run(self.bulk_urls(urls=urls, type_parse = type_parse))
        return self.result
        
