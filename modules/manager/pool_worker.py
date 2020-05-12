from   multiprocessing import Pool, current_process
from functools import lru_cache
from   monitor import watch_memory
import requests
import time


id_lote = None
strategy2 = None
session = None
result = []
response_list = []

class Worker:
    def _initializer(self, id_lote, strategy):
        self.id_lote = id_lote
        self.strategy = strategy
        self.parser   = strategy


    def task(self, lines):   
        watch_memory("pool worker", "parse") 
        lns = [self.parser(ln) for ln  in lines if self.parser(ln) is not None]
        return lns


    def waitPool(self, pool):
        pool.close()
        pool.join()    


    def apply_map(self,  pool, id_lote, lista, strategy):    
        try:
            self._initializer(id_lote, strategy)
            result =pool.imap(self.task, lista)
            return result
        except KeyboardInterrupt:
            # Allow ^C to interrupt from any thread.
            print('\033[0m')
            print('User Interupt\n')
        finally:
            pool.close()

def set_global_session():
    global session
    if not session:
        session = requests.Session()


def download_site(url):
    with session.get(url) as response:
        name = current_process().name
        return response


def apply_imap(sites):    
    set_global_session()
    pool = Pool()
    try:        
        result = pool.imap(download_site, sites) 
        time.sleep(5)
        return result
    except KeyboardInterrupt: 
        print('User Interupt\n')
    finally:
        pool.close()    