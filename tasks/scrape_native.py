from lxml import html
import requests
import os
import MySQLdb
import sys

def populate(db, cursor, lang):
    concord_cursor = db.cursor(MySQLdb.cursors.DictCursor)

    prefix = "H" if lang == "hebrew" else "G"
    row = cursor.fetchone()
    row_idx = 0
    while row:
        page = requests.get(f"https://studybible.info/strongs/"
                            f"{prefix}{row['extern_id']}")
        page.encoding = 'utf-8'
        tree = html.fromstring(page.content)

        concord_root = None
        if lang == "hebrew":
            concord_root = tree.xpath(
                '//section/div/article/span/text()')[0].strip()
        elif lang == "greek":
            concord_root = tree.xpath(
                '//section/article/dl/dt/span[@class="greek"]/text()')[0].strip()

        try:
            concord_cursor.execute("""UPDATE Concordance SET native = %s
                                      WHERE concord_id = %s;""",
                                   (concord_root, row['concord_id']))
        except MySQLdb.Error as ins_err:
            print("rollback {}:{} due to: {}".format(row['chapter'],
                row['verse_num'], ins_err))
            db.rollback()
        finally:
            db.commit()
            print(str(row['concord_id'])+"= "+concord_root)

        row = cursor.fetchone()
        row_idx += 1

    concord_cursor.close()

def init():

    num_args = len(sys.argv)
    if num_args < 1:
        print("Need a type 'hebrew'/'greek' to run this")
        exit()

    db = MySQLdb.connect(host=os.environ['LOCAB_DB_HOST'],
                         user=os.environ['LOCAB_DB_USER'],
                         passwd=os.environ['LOCAB_DB_PWORD'],
                         db='bman7777$LampLight_v1', charset="utf8")

    # cache off all books of bible into map for quick access
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("""SELECT * FROM Concordance WHERE lang = %s;""",
                   (sys.argv[1], ))

    # loop through all concordances of all languages and scape the relevant info
    populate(db, cursor, sys.argv[1])

    cursor.close()
    db.close()

init()