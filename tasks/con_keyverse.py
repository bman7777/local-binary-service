import json
import os
import MySQLdb


def populate(db):
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("""SELECT * FROM Concordance;""")

    for idx in range(cursor.rowcount):
        this_row = cursor.fetchone()

        row_cursor = db.cursor(MySQLdb.cursors.DictCursor)
        verse_tree = json.loads(this_row['verse_tree'])

        # count which book has the most verses in this concord
        book_count = {}

        for b, obj in verse_tree.items():
            most_verse = 0
            for chp,v_list in obj.items():
                num_verse = len(v_list)
                if b not in book_count:
                    book_count[b] = {'chp':0, 'vrs':num_verse}
                else:
                    book_count[b]['vrs'] += num_verse

                if num_verse > most_verse:
                    most_verse = num_verse
                    book_count[b]['chp'] = chp

        highest_book = ''
        high_count = 0
        for b,counter in book_count.items():
            if counter['vrs'] > high_count:
                high_count = counter['vrs']
                highest_book = b

        num_verse = len(verse_tree[highest_book][book_count[highest_book]['chp']])
        hash_val = ((int(highest_book) << 16) |
                    (int(book_count[highest_book]['chp']) << 8) |
                    (verse_tree[highest_book][book_count[highest_book]['chp']][num_verse-1]))

        try:
            row_cursor.execute("""UPDATE Concordance SET key_verse = %s
                                  WHERE concord_id = %s; """,
                                  (hash_val, this_row['concord_id']))
        except MySQLdb.Error as ins_err:
            print("rollback {}:{} due to: {}".format(this_row['concord_id'],
                                                     hash_val, ins_err))
            db.rollback()
        finally:
            db.commit()
            print("setting: "+str(this_row['concord_id'])+" to "+str(hash_val))

        row_cursor.close()

    cursor.close()

def init():

    db = MySQLdb.connect(host=os.environ['LOCAB_DB_HOST'],
                         user=os.environ['LOCAB_DB_USER'],
                         passwd=os.environ['LOCAB_DB_PWORD'],
                         db='bman7777$LampLight_v1', charset="utf8")

    # loop through all concordances of all languages and fill out key verse
    populate(db)

    db.close()

init()
