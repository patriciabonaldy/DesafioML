import multiprocessing as mp,os
import logging
import time


logger = logging.getLogger()

class ManagerWorker:

    def __init__(self):     
        self.started_at = time.time()     
        self.pool = None    
        self.jobs = []

    
    def create_jobs(self, process_wrapper, args_job): #(self.fname, start_block,end_block)
        pool = mp.Process(target=process_wrapper, args=args_job )   
        self.jobs.append(pool)

    def create_pool(self, size):
        self.pool = mp.Pool(size)
    

    def set_pool(self, process_wrapper, args_job):
        self.pool.map(process_wrapper, args_job)

    def wait_jobs(self):
        for proc in self.jobs:
            proc.join()  


    def execute_job(self,rerun=None):                
        if rerun is not None:    
            time.sleep(rerun)   
        for proc in self.jobs:             
            proc.start()    

    def get_pool(self):
        return self.jobs    


    def exists_process_live(self):
        for proc in self.jobs:
            if proc.is_alive():
                return True  
        return False        



    