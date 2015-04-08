__author__ = 'lbotti'

import logging

debuglogfile = "debug.log"
runninglog = "run.log"

logger = None


def initlogger():
    # logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # logging.getLogger('buildfilesystem').setLevel(logging.DEBUG)
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    # create file handler which logs even debug messages
    fh = logging.FileHandler(debuglogfile)
    fh.setLevel(logging.DEBUG)
    # create console handler with INFO / ERROR log level
    rh = logging.FileHandler(runninglog)
    rh.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - (%(threadName)-10s) - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    rh.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.addHandler(rh)

    logger.info("Logging Initialized")

    return logger


