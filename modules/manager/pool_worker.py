from   multiprocessing import Pool, current_process
from   monitor import watch_memory
import requests
import time


session = None
result = []
response_list = []


def set_global_session():
    global session
    if not session:
        session = requests.Session()


def task( id_lote, lines, strategy):   
    parse = strategy.get_strategy().parser
    watch_memory("pool worker", "parse") 
    lns = [parse(ln) for ln  in lines if parse(ln) is not None]
    return lns


def download_site(url):
    with session.get(url) as response:
        name = current_process().name
        #print(f"{name}:Read {len(response.content)} from {url}")
        return response


def waitPool( pool):
    pool.close()
    pool.join()    


def apply_async_with_callback( pool, id_lote, lista, strategy):    
    try:
        result =pool.apply_async(task, args = (id_lote, lista, strategy))  
        return result
    except KeyboardInterrupt:
        # Allow ^C to interrupt from any thread.
        print('\033[0m')
        print('User Interupt\n')
    finally:
        pool.close()


def apply_imap(sites):    
    set_global_session()
    pool = Pool()
    try:        
        result = pool.imap(download_site, sites) 
        return result
    except KeyboardInterrupt: 
        print('User Interupt\n')
    finally:
        pool.close()    

"""
if __name__ == "__main__":
    sites = [
        "https://www.jython.org",
        "http://olympus.realpython.org/dice",
    ] * 80
    start_time = time.time()
    apply_map_with_callback(sites)
    duration = time.time() - start_time
    print(f"Downloaded {len(sites)} in {duration} seconds")
    print(response_list)
"""