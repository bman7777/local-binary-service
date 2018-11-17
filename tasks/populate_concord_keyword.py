

# loop over every word for each concord and see which is the most common word_id that references concord X

import json
import os
import MySQLdb

def find_primary_keyword(db, db_row):
    word_tree = json.loads(db_row["word_tree"])
    high_cnt = 0
    high_word = ""

    for word, book_list in word_tree.items():
        num = 0
        for chp, verse_list in book_list.items():
            num += len(verse_list)

        if num > high_cnt:
            high_cnt = num
            high_word = word

    word_cursor = db.cursor(MySQLdb.cursors.DictCursor)
    word_cursor.execute("""SELECT word_id FROM Word WHERE word=%s;""",
                        (high_word,))

    if not word_cursor.rowcount:
        word_cursor.execute("""SELECT word_id FROM Word WHERE LOWER(word)=%s;""",
                               (high_word.lower(),))

    if word_cursor.rowcount:
        word_row = word_cursor.fetchone()
        try:
            word_cursor.execute("""UPDATE Concordance SET primary_word_id = %s
                                   WHERE concord_id = %s;""",
                                (word_row["word_id"], db_row["concord_id"]))
        except MySQLdb.Error as up_err:
            print("rollback due to: {}".format(up_err))
            db.rollback()
        finally:
            db.commit()
    else:
        print("what is up with: "+str(db_row["concord_id"]))

    word_cursor.close()

def init():

    db = MySQLdb.connect(host=os.environ['LOCAB_DB_HOST'],
                         user=os.environ['LOCAB_DB_USER'],
                         passwd=os.environ['LOCAB_DB_PWORD'],
                         db='bman7777$LampLight_v1', charset="utf8")

    # cache off all books of bible into map for quick access
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("""SELECT * from Concordance;""")

    row = cursor.fetchone()
    while row is not None:
        find_primary_keyword(db, row)
        row = cursor.fetchone()

    cursor.close()

    db.close()

init()
