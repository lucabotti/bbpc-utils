#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'lbotti'

import os
import time
import threading

import utils.logutils
from utils.poolfilemanager import PoolFileDBManager


poolpath = '/srv/BackupPC/cpool'
pcpath = '/srv/BackupPC/pc'

dbfile = '/srv/BackupPC/backuppc.sqlite.db'

poolfiles = {}
todofiles = {}

logger = None
poolfilemgr = None

listOfThreads = []


class ThreadedPathWalker():
    thread_count = 4

    lock = threading.Lock()

    paths = []


    def work_on_files(self, path, threadNumber=0):
        logger.debug("Thread %s working on path %s", threadNumber, path)
        start_time = time.time()
        for rootpath, dirs, files in os.walk(path):
            logger.debug("rootpath = %s", rootpath)
            logger.debug("dirs size = %s", len(dirs))
            logger.debug("files # = %s", len(files))

            for f in files:
                logger.debug("Thread %s working on file %s", threadNumber, f)
                filename = os.path.join(rootpath, f)
                statvalue = os.lstat(filename)
                poolfilemgr.insert_pool_file(statvalue.st_ino, filename)
        logger.info("Path %s walked in %d minutes", path, (time.time() - start_time) / 60)


    def pop_queue(self):
        path = None
        self.lock.acquire()

        if self.paths:
            path = self.paths.pop()

        self.lock.release()

        return path

    def dequeue(self, threadNumber):
        logger.debug("Thread %s entering in dequeue", threadNumber)
        while True:
            path = self.pop_queue()
            if not path:
                return None
            self.work_on_files(path, threadNumber)

    def start(self):
        threads = []

        for i in range(self.thread_count):
            t = threading.Thread(target=self.dequeue, kwargs={'threadNumber': i})
            t.start()
            threads.append(t)

        [t.join() for t in threads]


class WalkerThread(threading.Thread):
    def __init__(self, path, counter):
        super(WalkerThread, self).__init__()
        self.path = path
        self.threadNumber = counter
        self.stoprequest = threading.Event()

    def run(self):
        while not self.stoprequest.isSet():
            logger.debug("Thread %s working on path %s", self.threadNumber, path)
            start_time = time.time()
            for rootpath, dirs, files in os.walk(path):
                logger.debug("rootpath = %s", rootpath)
                logger.debug("dirs size = %s", len(dirs))
                logger.debug("files # = %s", len(files))

                for f in files:
                    logger.debug("Thread %s working on file %s", self.threadNumber, f)
                    filename = os.path.join(rootpath, f)
                    statvalue = os.lstat(filename)
                    poolfilemgr.insert_pool_file(statvalue.st_ino, filename)
            logger.info("Path %s walked in %d minutes", path, (time.time() - start_time) / 60)

    def join(self, timeout=None):
        self.stoprequest.set()
        super(WalkerThread, self).join(timeout)

if __name__ == '__main__':
    # add paths to queue
    poolfilemgr = PoolFileDBManager(dbfile)
    logger = utils.logutils.initlogger()
    # threadedpathwalker = ThreadedPathWalker()
    # threadedpathwalker.paths = os.listdir(poolpath)
    # threadedpathwalker.start()

    pool = []
    counter = 0

    for path in os.listdir(poolpath):
        counter += 1
        pool.append(WalkerThread(path, counter))

    for thread in pool:
        thread.start()

    for thread in pool:
        thread.join()


