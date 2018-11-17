
# The entry point to all lamplight api calls
import os
import MySQLdb
from flask import Flask

app = Flask(__name__)

class LBDB(object):
    _connection = None

    @staticmethod
    def get():
        try:
            LBDB._connection.ping(True)
        except (MySQLdb.OperationalError, AttributeError) as e:
            LBDB._connection = MySQLdb.connect(
                host=os.environ['LOCAB_DB_HOST'],
                user=os.environ['LOCAB_DB_USER'],
                passwd=os.environ['LOCAB_DB_PWORD'],
                db='bman7777$LampLight_v1',
                charset="utf8")

        return LBDB._connection

db = LBDB()

import views