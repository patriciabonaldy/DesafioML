#!/usr/bin/env python3

from   os.path import isfile, join
import aiofiles as aiof
import asyncio
import async_timeout
import config 
import logging
import os


logger = logging.getLogger()

class AsyncFile:

    def __init__(self):
        self.result = []


    async def write_multiple_files(self, filename: str, encoding, cont: int, lines, **kwargs):
        """Find HREFs in the HTML of `url`."""
        try:
            filename = join(config.config.data_dir, str(cont)+filename)
            async with aiof.open(filename, "w", encoding=encoding) as out:
                for ln in lines:
                    await out.write(ln)
                await out.flush()
        except Exception as error:
            logger.error("Could not write data to {} - {}".format(filename, error)) 


    async def bulk_lines(self, filename, encoding, block_lines, **kwargs):
        """Crawl & write concurrently to `file` for multiple `lines`."""
        tasks = []  
        cont = 0    
        for lines in block_lines:
            tasks.append(
            self.write_multiple_files(filename=filename, encoding=encoding, cont=cont, lines=lines, **kwargs)
            )
            cont += 1
        await asyncio.gather(*tasks)

    def run_save_file(self, filename, encoding, lines):
        asyncio.run(self.bulk_lines(filename=filename, encoding=encoding, block_lines=lines))