import MySQLdb
import pickle

from __init__ import db

def get_highlight(db_row):

    con_dict = {}
    if "concord_set" in db_row and db_row["concord_set"]:
        word_cursor = db.get().cursor(MySQLdb.cursors.DictCursor)
        con_list = pickle.loads(db_row["concord_set"])
        for con in con_list:
            if str(con) not in con_dict:
                word_cursor.execute("""SELECT native, description, Word.word
                                       FROM Concordance
                                       JOIN Word ON Concordance.primary_word_id = Word.word_id
                                       WHERE concord_id=%s;""", (int(con),))
                if word_cursor.rowcount:
                    con_dict[str(con)] = word_cursor.fetchone()

        word_cursor.close()

    return {
                "book": db_row["title"],
                "chapter": db_row["chapter"],
                "verse": db_row["verse_num"],
                "text": (db_row["hypertext"] if db_row["hypertext"] else
                         db_row["text"]),
                "concord_info": con_dict
            }
