__author__ = 'lbotti'

import sqlite3


class PoolFileDBManager():
    connection = None
    logger = None

    def __init__(self, sqlite_file):
        try:
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
        self.connection.commit()

    def insert_pool_file(self, inode,name):
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO cpool(inode, name) values (?,?)", (inode,name))
        self.connection.commit()






