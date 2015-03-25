#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'lbotti'


import os
import time
import logging
import sqlite3





logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger('buildfilesystem').setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)

poolpath = '/srv/BackupPC/cpool'
pcpath = '/srv/BackupPC/pc'

dbfile = '/srv/BackupPC/backuppc.sqlite.db'

poolfiles = {}
todofiles = {}

def walkpoolpath():

    logger.debug("Cycling on Pool Directory")


    start_time = time.time()
    for subdir, dirs, links in os.walk(poolpath):


        for link in links:
            statvalue = os.lstat(os.path.join(subdir, link))

            poolfiles[statvalue.st_ino] = link


    logger.info("Pool walked in %d minutes",(time.time() - start_time)/60)



    start_time2 = time.time()
    logger.debug("Cycling on pc directory")
    for subdir,dirs,files in os.walk(pcpath):

        for file in files:
            statvalue = os.lstat(os.path.join(subdir,file))

            if (statvalue.st_ino in poolfiles.keys()):
                todofiles[file] = poolfiles[statvalue.st_ino]
                #print file, ' ', poolfiles[statvalue.st_ino]

    logger.info("pc walked in %d minutes" ,  ((time.time() - start_time2)/60))





def initDB():
    try:
        con = sqlite3.connect(dbfile)

        with con:
            cur = con.cursor()
            cur.execute("CREATE TABLE IF NOT EXISTS BackupFiles(Id INTEGER PRIMARY KEY, Filename TEXT, inode TEXT)")

    except sqlite3.Error, e:
        logger.error("Cannot open SQLlite DB %s", dbfile)
    finally:
        if con:
            con.close()



if __name__ == '__main__':

    walkpoolpath()