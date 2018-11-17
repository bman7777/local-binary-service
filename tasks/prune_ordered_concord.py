import json
import os
import pickle
import MySQLdb

def check_ordered_concord(db, db_row):
    con_cursor = db.cursor(MySQLdb.cursors.DictCursor)

    order_concord = pickle.loads(db_row['ordered_concord'])
    has_edits = False
    order_out = []
    word_key = db_row['word'].lower()

    for concord_id, dist in order_concord:
        con_cursor.execute("""SELECT word_tree, english FROM Concordance
                              WHERE concord_id = %s;""", (concord_id,))

        if con_cursor.rowcount:
            concord_row = con_cursor.fetchone()
            word_tree = json.loads(concord_row['word_tree'])

            if word_key not in word_tree:
                print(concord_row['english']+ " does not have: "+db_row['word'].lower())
                has_edits = True
            else:
                order_out.append((concord_id, dist))

    if has_edits:
        try:
            con_cursor.execute("""UPDATE Word SET ordered_concord = %s
                                  WHERE word_id = %s;""",
                                    (pickle.dumps(order_out,
                                        protocol=pickle.HIGHEST_PROTOCOL),
                                     db_row['word_id']))
        except MySQLdb.Error as up_err:
            print("rollback due to: {}".format(up_err))
            db.rollback()
        finally:
            db.commit()

    con_cursor.close()

def init():

    db = MySQLdb.connect(host=os.environ['LOCAB_DB_HOST'],
                         user=os.environ['LOCAB_DB_USER'],
                         passwd=os.environ['LOCAB_DB_PWORD'],
                         db='bman7777$LampLight_v1', charset="utf8")

    # cache off all books of bible into map for quick access
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("""SELECT * from Word;""")

    row = cursor.fetchone()
    while row is not None:
        check_ordered_concord(db, row)
        row = cursor.fetchone()

    cursor.close()

    db.close()

init()