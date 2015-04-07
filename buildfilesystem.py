#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'lbotti'

import os
import time
import threading
from Queue import Queue

import utils.logutils


poolpath = '/srv/BackupPC/cpool'
pcpath = '/srv/BackupPC/pc'

dbfile = '/srv/BackupPC/backuppc.sqlite.db'

poolfiles = {}
todofiles = {}

logger = None
poolfilemgr = None

listOfThreads = []


class ProducerThread(threading.Thread):
    def __init__(self, producer_queue, consumer_queue):
        super(ProducerThread, self).__init__()
        self.producer_queue = producer_queue
        self.consumer_queue = consumer_queue


    def run(self):

        while True:
            path = self.producer_queue.get()
            self.explore(path)
            self.producer_queue.task_done()

    def explore(self, path):
        logger.debug("Thread exploring %s", path)
        for rootpath, dirs, files in os.walk(path):
            for f in files:
                filename = os.path.join(rootpath, f)
                logger.debug("Queueing %s", filename)
                self.consumer_queue.put(filename)


class WalkerThread(threading.Thread):
    def __init__(self, queue):
        super(WalkerThread, self).__init__()
        self.queue = queue

    def run(self):
        while True:
            file = self.queue.get()
            logger.debug("Thread working on file %s", file)
            statvalue = os.lstat(file)
            # poolfilemgr.insert_pool_file(statvalue.st_ino, file)
            self.queue.task_done()


if __name__ == '__main__':
    # add paths to queue
    # poolfilemgr = PoolFileDBManager(dbfile)
    logger = utils.logutils.initlogger()

    producer_queue = Queue()
    consumer_queue = Queue()

    start_time = time.time()

    logger.info(" ************ Starting producer Threads ************ ")
    for numberofthreads in xrange(os.listdir(poolpath).__len__()):
        logger.info("Starting thread number %d", numberofthreads)
        producer = ProducerThread(producer_queue, consumer_queue)
        producer.start()

    logger.info(" ************ Populating Queue ************ ")
    for directory in os.listdir(poolpath):
        realdirectory = os.path.join(poolpath, directory)
        logger.info(" Inserting Directory %s in producer queue ", realdirectory)
        producer_queue.put(realdirectory)

    logger.info(" ************ Starting consumer Threads ************ ")
    for x in xrange(10):
        worker = WalkerThread(consumer_queue)
        # worker.daemon = True
        worker.start()

    consumer_queue.join()
    producer_queue.join()
    logger.info(" ************ Ending consumer Threads ************ ")

    end_time = time.time()

    logger.info(" *********** Three Walked in %s minutes", (end_time - start_time) / 60)