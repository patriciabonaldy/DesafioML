import psutil
import logging
import time
import decimal

ctx = decimal.Context()
ctx.prec = 10
logger = logging.getLogger()

def harward_info():
    dc={}
    dc['cp_usage'] = psutil.cpu_percent(interval=1)
    dc['processor'] = psutil.cpu_count(logical=False)
    dc['memory_use'] = psutil.virtual_memory()
    return dc 


def memory_use():
    return psutil.virtual_memory().percent


def cpu_use():
    return psutil.cpu_percent()


def float_to_str(f):
    """ Convierte un float a string sin usar notacion cientifica """
    d1 = ctx.create_decimal(repr(f))
    return format(d1, 'f')

def profile(func):
    """ Decorator que calcula el tiempo que tarda una funcion.
        Uso: @profile antes de def """
    def wrap(*args, **kwargs):
        started_at = time.time()
        result = func(*args, **kwargs)
        elapsed_ms = (time.time() - started_at) * 1000
        logger.info('f: ' + func.__name__ + ' has taken: ' + float_to_str(elapsed_ms) + ' ms')
        #log.root_logger.info(col.highlight('yellow', col.colourise('black', 'f: ' + func.__name__ + ' has taken: ' + float_to_str(elapsedMs) + ' ms')))
        return result
    return wrap


def config_logging(path, file_name, log_level):
    formatter = "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s"
    log_formatter = logging.Formatter(formatter)
    file_path_and_name = "{0}/{1}.log".format(path, file_name)
    file_handler = logging.FileHandler(file_path_and_name)
    file_handler.setFormatter(log_formatter)
    logger.addHandler(file_handler)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    #logger.addHandler(console_handler)
    logger.setLevel(log_level)
    logger.info('Logging configurado: '.join(file_path_and_name))
    return logger

def watch_memory(name_class, name_method):
    memory = memory_use()
    if memory > 75 :
        logger.info("memory al  {} - esperando a liberar - proceso {}[{}]".format(memory, name_class, name_method))
        while 1:
            time.sleep(50)
            memory = memory_use()
            if memory < 60:
                break  
        logger.info("memory al  {} - retomando actividad".format(memory))
        

def watch_cpu():
    _cpu = cpu_use()
    if _cpu >= 80 :
        logger.info("cpu al  {} - esperando a liberar".format(_cpu))
        while 1:
            time.sleep(0.001)
            _cpu = cpu_use()
            if _cpu < 60:
                break  
        logger.info("cpu al  {} - retomando actividad".format(_cpu))
        