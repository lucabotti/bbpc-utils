__author__ = 'lbotti'

import sqlite3
import logging

# module_logger = logging.getlogger(__name__)


class PoolFileDBManager():
    connection = None
    logger = None

    def __init__(self, sqlite_file):
        try:
            self.logger = logging.getLogger()
            self.connection = sqlite3.connect(sqlite_file)
            self.create_pool_table()


        except sqlite3.Error, e:
            self.logger.error("Cannot open SQLlite DB %s", sqlite_file)
            print e.message


    def __del__(self):

        self.connection.close()

    def create_pool_table(self):
        cursor = self.connection.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS cpool(id INTEGER PRIMARY KEY, inode TEXT,
                       name TEXT)
        ''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS pcpool(id INTEGER PRIMARY KEY, inode TEXT,
                       name TEXT)
        ''')
        self.logger.debug("Table cpool created")
        self.connection.commit()

    def insert_pool_file(self, inode, name):

        cursor = self.connection.cursor()
        parameters = (inode,)
        rows = cursor.execute("SELECT name FROM cpool WHERE inode = ?", parameters)
        if rows <= 0:
            cursor.execute("INSERT INTO cpool(inode, name) VALUES (?,?)", (inode, name))
            self.connection.commit()
            self.logger.debug("Inode %s with name %s added", inode, name)
        else:
            self.logger.debug("Inode %s with name %s skipped", inode, name)

    def insert_pc_file(self, inode, name):
        cursor = self.connection.cursor()
        parameters = (inode,)
        rows = cursor.execute("SELECT name FROM pcpool WHERE inode = ?", parameters)
        if rows <= 0:
            cursor.execute("INSERT INTO pccpool(inode, name) VALUES (?,?)", (inode, name))
            self.connection.commit()
            self.logger.debug("Inode %s with name %s added", inode, name)
        else:
            self.logger.debug("Inode %s with name %s skipped", inode, name)










