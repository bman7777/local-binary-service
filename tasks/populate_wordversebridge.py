
import json
import os
import pickle
import MySQLdb

def insert_mapping(db, db_row, run_cursor):
    concord_info = pickle.loads(db_row['ordered_concord'])
    word_id = str(db_row['word_id'])
    final_map = {}

    for concord, count in concord_info:
        run_cursor.execute("""SELECT word_tree FROM Concordance WHERE concord_id=%s;""", (str(concord),))
        con_row = run_cursor.fetchone()

        if con_row:
            word_tree = json.loads(con_row["word_tree"])
            for word, word_map in word_tree.items():
                match = "1" if word.lower() == db_row["word"].lower() else "0"
                similar =  "0" if match == "1" else "1"
                for book, chp_map in word_map.items():
                    for chp, verse_list in chp_map.items():
                        for verse_num in verse_list:
                            hash_val = (int(book) << 16) | (int(chp) << 8) | int(verse_num)

                            if hash_val not in final_map:
                                final_map[hash_val] = {'match': match, 'similar': similar, 'synonym': "0"}
                            else:
                                final_map[hash_val]['match'] = match if match == "1" else final_map[hash_val]['match']
                                final_map[hash_val]['similar'] = similar if similar == "1" else final_map[hash_val]['similar']

    if db_row['details']:
        details = json.loads(db_row['details'])
        syn_cursor = db.cursor(MySQLdb.cursors.DictCursor)
        for det in details:
            if "syn" in det:
                if " " in det["syn"]:
                    run_cursor.execute("""select distinct PhraseVerseBridge.verse_hash from Phrase
                                          JOIN PhraseVerseBridge ON Phrase.phrase_id = PhraseVerseBridge.phrase_id
                                          where LOWER(Phrase.phrase) LIKE LOWER('%s');""", (det["syn"],))
                    word_row = run_cursor.fetchone()
                    while word_row is not None:
                        hash_val = word_row["verse_hash"]
                        if hash_val not in final_map:
                            final_map[hash_val] = {'match': "0", 'similar': "0", 'synonym': "1"}
                        else:
                            final_map[hash_val]['synonym'] = "1"
                        word_row = run_cursor.fetchone()
                else:
                    run_cursor.execute("""select ordered_concord from Word where LOWER(word) LIKE LOWER("%s");""", (det["syn"],))
                    word_row = run_cursor.fetchone()
                    while word_row is not None:
                        concord_info = pickle.loads(word_row["ordered_concord"])
                        if det["syn"] in concord_info:
                            for book, chp_map in concord_info[det["syn"]].items():
                                for chp, verse_list in chp_map.items():
                                    for verse_num in verse_list:
                                        hash_val = (int(book) << 16) | (int(chp) << 8) | int(verse_num)

                                        if hash_val not in final_map:
                                            final_map[hash_val] = {'match': "0", 'similar': "0", 'synonym': "1"}
                                        else:
                                            final_map[hash_val]['synonym'] = "1"

                        word_row = run_cursor.fetchone()

        syn_cursor.close()

    for hash_val, stuff in final_map.items():
        if stuff['match'] == "1" or stuff['synonym'] == "1" or stuff['similar'] =="1":
            try:
                run_cursor.execute("""INSERT INTO
                                      WordVerseBridge(word_id, verse_hash,
                                      is_match, is_synonym, is_similar)
                                      values(%s, %s, %s, %s, %s);""",
                               (word_id, str(hash_val), stuff['match'], stuff['synonym'], stuff['similar']))
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