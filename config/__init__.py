#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import os
import json
import logging
import logging.config
from  config.database import OracleDB
from box import Box
import configparser
 
here = os.path.abspath(os.path.dirname(__file__))
dir_root = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)),".."))
resources = os.path.join(dir_root, 'resources')

Config = configparser.ConfigParser()
Config.read("config.ini")

def ConfigSectionMap(section):
    dict1 = {}
    options = Config.options(section)
    for option in options:
        try:
            dict1[option] = Config.get(section, option)
            if dict1[option] == -1:
                print("skip: %s" % option)
        except:
            print("exception on %s!" % option)
            dict1[option] = None
    return dict1



#Configuraciones Iiciales
configurationSizeFile = ConfigSectionMap('ConfigurationSizeFile')
extension = ConfigSectionMap('Extension')
separator = ConfigSectionMap('Separator')
fileencode = ConfigSectionMap('FileEncode')
PORT = int(ConfigSectionMap('Port')['port'])
connectiondb = ConfigSectionMap('ConnectionDB')



MAX_CONTENT_LENGTH = configurationSizeFile['max_content_length']      #3gb
max_chunk_size =  configurationSizeFile['max_chunk_size']
EXTENSION = extension['extensionallow']
EXTENSION = (json.loads(EXTENSION.replace("'",'"')))
SEPARATOR = separator['type']
SEPARATOR = (json.loads(SEPARATOR.replace("'",'"')))
FILEENCODE = fileencode['typefile']
FILEENCODE = (json.loads(FILEENCODE.replace("'",'"')))
USER = connectiondb['user']
USER = (json.loads(USER.replace("'",'"')))
PWD = connectiondb['pwd']
CONNECTIONSTRING = connectiondb['connection_string']

config = Box({
    "env": "development",
    "host": "127.0.0.1",
    "port": PORT,
    "session_cache_dir": os.path.join(here, "cache", "session"),
    "session_secret": 'bad_secret',  # make real one with os.urandom(32).hex()
    "data_dir": os.path.join(resources, "data"),
    "max_size_file": MAX_CONTENT_LENGTH,
    "max_chunk_size": max_chunk_size,
    "extension_allowed": EXTENSION,
    "separator_allowed": SEPARATOR,
    "encode_allowed": FILEENCODE,
    "ssl": False,
    "user": USER,
    "pwd": PWD,
    "connectionstring": CONNECTIONSTRING
})

os.makedirs(config.session_cache_dir, exist_ok=True)
os.makedirs(config.data_dir, exist_ok=True)




