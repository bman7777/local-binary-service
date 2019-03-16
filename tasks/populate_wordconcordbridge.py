
import json
import os
import pickle
import MySQLdb


def insert_mapping(db, db_row, run_cursor):

    concord_info = pickle.loads(db_row['ordered_concord'])
    word_id = str(db_row['word_id'])
    final_map = []

    # note: I'm assuming that the counts are already ascending
    for concord, count in concord_info:
        final_map.append({'concord_id':concord, 'match':True, 'similar':False, 'synonym':False})

    if db_row['details']:
        details = json.loads(db_row['details'])
        for det in details:
            if "syn" in det:
                for syn in det['syn']:
                    if " " not in syn:
                        run_cursor.execute("""select ordered_concord from Word where word = %s;""", (syn,))
                        word_row = run_cursor.fetchone()
                        while word_row is not None:
                            concord_info = pickle.loads(word_row["ordered_concord"])
                            for concord, count in concord_info:
                                found_it = False
                                for item in final_map:
                                    if item['concord_id'] == concord:
                                        item['synonym'] = True
                                        found_it = True
                                        break

                                if not found_it:
                                    final_map.append({'concord_id':concord, 'match':False, 'similar':False, 'synonym':True})

                            word_row = run_cursor.fetchone()

    for item in final_map:
        if item['match'] or item['synonym'] or item['similar']:
            run_cursor.execute("""SELECT b.book_id, count(*) as total
                                  from ConcordanceVerseBridge Cvb
                                  JOIN Verse v ON v.hash = Cvb.verse_hash
                                  JOIN Book b ON b.book_id = v.book_id
                                  WHERE concord_id = %s
                                  GROUP BY b.book_id
                                  ORDER BY total DESC LIMIT 1;""",
                                  (item['concord_id'],))
            common_row = run_cursor.fetchone()
            most_common_book = common_row["book_id"] if common_row else 0

            run_cursor.execute("""select count(*) as total
                                  from ConcordanceVerseBridge Cvb
                                  WHERE concord_id = %s;""",
                                  (item['concord_id'],))
            common_row = run_cursor.fetchone()
            num_verses = common_row["total"] if common_row else 0

            try:
                run_cursor.execute("""INSERT INTO
                                      WordConcordanceBridge(word_id, concord_id,
                                      is_match, is_similar, is_synonym,
                                      num_verses, most_common_book_id)
                                      values(%s, %s, %s, %s, %s, %s, %s);""",
                               (word_id, item['concord_id'],
                               "1" if item['match'] else "0",
                               "1" if item['similar'] else "0",
                               "1" if item['synonym'] else "0",
                               num_verses, most_common_book))
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
    cursor.execute("""SELECT word_id, word, ordered_concord, details from Word;""")
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
