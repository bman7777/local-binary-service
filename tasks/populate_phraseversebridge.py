
import os
import json
import MySQLdb

def insert_mapping(db, db_row, run_cursor):
    verses = json.loads(db_row['verse_tree'])
    for book, chp_map in verses.items():
        for chp, verse_list in chp_map.items():
            for verse_num in verse_list:
                hash_val = (int(book) << 16) | (int(chp) << 8) | int(verse_num)
                try:
                    run_cursor.execute("""INSERT INTO PhraseVerseBridge(phrase_id, verse_hash) values(%s, %s);""",
                                   (str(db_row['phrase_id']), str(hash_val)))
                except MySQLdb.Error as up_err:
                    print("rollback due to: {}".format(up_err))
                    db.rollback()
                finally:
                    db.commit()


def init():

    db = MySQLdb.connect(host=os.environ['LOCAB_DB_HOST'],
                         user=os.environ['LOCAB_DB_USER'],
                         passwd=os.environ['LOCAB_DB_PWORD'],
                         db='bman7777$LampLight_v1', charset="utf8")

    # cache off all books of bible into map for quick access
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("""SELECT verse_tree, phrase_id from Phrase;""")
    run_cursor = db.cursor(MySQLdb.cursors.DictCursor)

    row = cursor.fetchone()
    num_found = 0
    print_digit = 0
    while row is not None:
        insert_mapping(db, row, run_cursor)
        row = cursor.fetchone()
        num_found += 1
        if (num_found / 100) >= print_digit:
            print(str(num_found)+" phrases found so far")
            print_digit += 1

    run_cursor.close()
    cursor.close()
    db.close()

init()