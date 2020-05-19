import multiprocessing
import multiprocessing.pool as pool
from   multiprocessing import Pool, current_process, Process
from functools import lru_cache
from   monitor import watch_memory
import requests
import time


id_lote = None
strategy2 = None
session = None
result = []
response_list = []


class NoDaemonProcess(Process):
    @property
    def daemon(self):
        return False

    @daemon.setter
    def daemon(self, value):
        pass


class NoDaemonContext(type(multiprocessing.get_context())):
    Process = NoDaemonProcess

# We sub-class multiprocessing.pool.Pool instead of multiprocessing.Pool
# because the latter is only a wrapper function, not a proper class.
class MyPool(pool.Pool):
    def __init__(self, *args, **kwargs):
        kwargs['context'] = NoDaemonContext()
        super(MyPool, self).__init__(*args, **kwargs)

class Worker:
    def _initializer(self, id_lote, strategy):
        self.id_lote = id_lote
        self.strategy = strategy
        self.parser   = strategy


    def task(self, line):   
        watch_memory("pool worker", "parse") 
        #lns = [self.parser(ln) for ln  in lines if self.parser(ln) is not None]
        lns = self.parser(line)
        if lns is not None:
            return lns


    def waitPool(self, pool):
        pool.close()
        pool.join()    


    def apply_map(self,  pool, id_lote, lista, strategy):    
        try:
            self._initializer(id_lote, strategy)
            result =pool.map(self.task, lista)
            return result
        except KeyboardInterrupt:
            # Allow ^C to interrupt from any thread.
            print('\033[0m')
            print('User Interupt\n')
        finally:
            pool.close()


    def apply_map_request(self, sites):    
        set_global_session()
        pool = Pool(5)
        try:        
            result = pool.map(download_site, sites) 
            time.sleep(3)
            return result
        except KeyboardInterrupt: 
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


def apply_map_request(sites):    
    set_global_session()
    pool = Pool(5)
    try:        
        result = pool.map(download_site, sites) 
        time.sleep(3)
        return result
    except KeyboardInterrupt: 
        print('User Interupt\n')
    finally:
        pool.close() 

def apply_imap_request(pool, sites):    
    set_global_session()
    try:        
        result = pool.imap(download_site, sites) 
        time.sleep(5)
        return result
    except KeyboardInterrupt: 
        print('User Interupt\n')
    finally:
        pool.close()    