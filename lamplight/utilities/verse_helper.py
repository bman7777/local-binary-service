import json
import MySQLdb
import operator
import pickle

from __init__ import db


def resolve_verses(out_tree):
    data = []
    highlight_verse = None
    book_freq = {}
    verse_count = 0
    cursor = db.get().cursor(MySQLdb.cursors.DictCursor)
    query = """SELECT Book.title, Book.book_id, Verse.hypertext, Verse.text,
               Verse.chapter, Verse.verse_num, Verse.concord_set
               FROM Book
               JOIN Verse on Verse.book_id = Book.book_id
               WHERE
               Book.book_id=%s and
               Verse.chapter=%s and
               Verse.verse_num=%s;"""

    for book in out_tree.keys():
        for chapter in out_tree[book].keys():
            for verse in out_tree[book][chapter]:
                cursor.execute(query, (book, chapter, verse))
                if cursor.rowcount:
                    row = cursor.fetchone()

                    if row["title"] not in book_freq:
                        book_freq[row["title"]] = 0

                    book_freq[row["title"]] += 1
                    verse_count += 1
                    data.append({"book": row["title"],
                                 "chapter": chapter,
                                 "verse": verse,
                                 "text": row["text"]})
                    if not highlight_verse:
                        highlight_verse = get_highlight(row)

    cursor.close()

    sorted_freq = sorted(book_freq.items(), key=operator.itemgetter(1))
    num_sorted = len(sorted_freq)
    other = 0
    if num_sorted > 2:
        for idx in range(0, len(sorted_freq) - 2):
            other += sorted_freq[idx][1]

        sorted_freq = sorted_freq[-2:]

    translated_list = []
    for item in reversed(sorted_freq):
        translated_list.append({'key': item[0], 'value': item[1]})

    if other:
        translated_list.append({'key': 'Other', 'value': other})

    stats = [{'type': 'doughnut', 'data': translated_list}]
    if verse_count:
        stats.append({'type': 'stat', 'data':
            [{"key": "Verse Matches:", "value":verse_count}]})

    return (data, highlight_verse, stats)


def from_concords(concord_list, word_list, page, page_size):
    data = []
    highlight_verse = None
    query = """SELECT Book.title, Book.book_id, Verse.hypertext, Verse.text,
              Verse.chapter, Verse.verse_num, Verse.concord_set
              FROM Book
              JOIN Verse on Verse.book_id = Book.book_id
              WHERE Book.book_id=%s and
              Verse.chapter=%s and Verse.verse_num=%s;"""
    book_freq = {}
    first_book = {"title": "", "id": -1}
    for con in concord_list:
        word_cursor = db.get().cursor(MySQLdb.cursors.DictCursor)
        word_cursor.execute("""SELECT word_tree FROM Concordance
                              WHERE concord_id=%s;""", (int(con),))

        for idx in range(word_cursor.rowcount):
            word_tree = json.loads(word_cursor.fetchone()["word_tree"])
            cursor = db.get().cursor(MySQLdb.cursors.DictCursor)

            # if word_list is empty, add all words from word_tree in
            if len(word_list) <= 0:
                for w in word_tree:
                    word_list.append(w)

            for word_key in word_list:
                if word_key in word_tree:
                    for book in word_tree[word_key].keys():
                        for chapter in word_tree[word_key][book].keys():
                            for verse in word_tree[word_key][book][chapter]:
                                cursor.execute(query, (book, chapter, verse))
                                if cursor.rowcount:
                                    row = cursor.fetchone()

                                    if row["title"] not in book_freq:
                                        book_freq[row["title"]] = 0

                                    if first_book["id"] < 0 or row["book_id"] < first_book["id"]:
                                        first_book["id"] = row["book_id"]
                                        first_book["title"] = row["title"]

                                    book_freq[row["title"]] += 1
                                    data.append({"book": row["title"],
                                                 "chapter": chapter,
                                                 "verse": verse,
                                                 "text": row["text"]})
                                    if not highlight_verse:
                                        highlight_verse = get_highlight(row)

            cursor.close()
        word_cursor.close()

    sorted_freq = sorted(book_freq.items(), key=operator.itemgetter(1))
    num_sorted = len(sorted_freq)
    other = 0
    if num_sorted > 2:
        for idx in range(0, len(sorted_freq) - 2):
            other += sorted_freq[idx][1]

        sorted_freq = sorted_freq[-2:]

    translated_list = []
    for item in reversed(sorted_freq):
        translated_list.append({'key': item[0], 'value': item[1]})

    if other:
        translated_list.append({'key': 'Other', 'value': other})

    # todo: add a stat for the concord/word that is most common to the verse
    # set (other than the highlighted verse/word.)

    stats = [{'type': 'doughnut', 'data': translated_list}]
    if first_book["title"]:
        stats.append({'type': 'stat', 'data':
            [{"key": "First Mention:", "value":first_book["title"]}]})

    return (data, highlight_verse, stats)

def count_verses_from_concord(concord_list, word_list):
    num = 0
    word_cursor = db.get().cursor(MySQLdb.cursors.DictCursor)
    book_map = {}
    query = """SELECT COUNT(*), Book.title
               FROM Book
               JOIN Verse on Verse.book_id = Book.book_id
               WHERE Book.book_id=%s and
               Verse.chapter=%s and Verse.verse_num=%s;"""

    for con in concord_list:
        word_cursor.execute("""SELECT word_tree FROM Concordance
                               WHERE concord_id=%s;""", (int(con),))

        for idx in range(word_cursor.rowcount):
            word_tree = json.loads(word_cursor.fetchone()["word_tree"])
            cursor = db.get().cursor()
            for word_key in word_list:
                if word_key in word_tree:
                    for book in word_tree[word_key].keys():
                        for chapter in word_tree[word_key][book].keys():
                            for verse in word_tree[word_key][book][chapter]:
                                cursor.execute(query, (book, chapter, verse))
                                row = cursor.fetchone()

                                if row[1] not in book_map:
                                    book_map[row[1]] = 0

                                num += row[0]
                                book_map[row[1]] += row[0]

            cursor.close()

    book_title = ""
    highest_cnt = 0
    for book_key,cnt in book_map.items():
        if cnt > highest_cnt:
            highest_cnt = cnt
            book_title = book_key

    word_cursor.close()

    return num, book_title

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
