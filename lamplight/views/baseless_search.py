import json
import re
import MySQLdb
from urllib.parse import unquote

from __init__ import app, db
from utilities import verse_helper
from flask import request, Response

@app.route('/baseless-search', methods=['GET'])
def baseless_search():
    if "concord" in request.args:
        return Baseless.concord_search(
            int(request.args.get("concord", "")),
            int(unquote(request.args.get("page", "0"))),
            int(unquote(request.args.get("pageSize", "50"))))
    else:

        similar = False
        synonym = False
        for item in request.args:
            if item == "synonym":
                synonym = True
            elif item == "similar":
                similar = True

        return Baseless.text_search(
            unquote(request.args.get("text", "")),
            similar,
            synonym,
            int(unquote(request.args.get("page", "0"))),
            int(unquote(request.args.get("pageSize", "50"))))


class Baseless(object):

    @staticmethod
    def get_verse_specific(book, chapter, verse):
        data = []
        highlight = None

        # Float as verse number is OK, as long as its a whole number
        # not sure who would ever do this...
        if float(chapter).is_integer() and float(verse).is_integer():
            chapter = int(float(chapter))
            verse = int(float(verse))

            # only positive numbers!
            if chapter > 0 and verse > 0:
                cursor = db.get().cursor(MySQLdb.cursors.DictCursor)
                cursor.execute("""SELECT Book.title, Verse.hypertext,
                                         Verse.text, Verse.chapter,
                                         Verse.verse_num, Verse.concord_set
                                  FROM Book JOIN Verse on
                                  Verse.book_id = Book.book_id WHERE
                                  (Book.short_title=%s or Book.title=%s)
                                  AND
                                  (Verse.chapter=%s and
                                   Verse.verse_num=%s);""",
                                  (book, book, chapter, verse))
                if cursor.rowcount:
                    row = cursor.fetchone()
                    data.append({
                                    "book": row["title"],
                                    "chapter": chapter,
                                    "verse": verse,
                                    "text": row["text"]
                                })

                    highlight = verse_helper.get_highlight(row)


                cursor.close()
        return data, highlight

    @staticmethod
    def get_phrase(search_str, page, page_size):
        criteria = [{'english': {'words': [search_str]}}]
        cursor = db.get().cursor(MySQLdb.cursors.DictCursor)
        cursor_string, join_stmts, where_stmts = verse_helper.get_query_parts(
            criteria, page, page_size,
            False, 'All')
        cursor.execute(cursor_string + join_stmts + where_stmts)
        data = []
        highlight = None
        has_next = False
        if cursor.rowcount:
            for idx in range(cursor.rowcount):
                if idx >= page_size:
                    has_next = True
                    break

                row = cursor.fetchone()
                if not highlight:
                    highlight = verse_helper.get_highlight(row)

                if row:
                    data.append({
                                    "book": row["title"],
                                    "chapter": row["chapter"],
                                    "verse": row["verse_num"],
                                    "text": row["text"],
                                    "type": "verse",
                                    "lang": "english"
                                })

        cursor_string, join_stmts, where_stmts = verse_helper.get_query_parts(
            criteria, page, page_size, True, 'All')
        cursor.execute(cursor_string + join_stmts + where_stmts)
        stats = verse_helper.get_stats(cursor)
        cursor.close()

        return data, highlight, stats, has_next

    @staticmethod
    def get_word(search_word, similar, synonym, page, page_size):

        data = []
        highlight = None
        has_next = False

        sim_clause = "OR Wcb.is_similar = 1" if similar else ""
        sim_clause += "OR Wcb.is_synonym = 1" if synonym else ""

        cursor = db.get().cursor(MySQLdb.cursors.DictCursor)
        cursor.execute("""SELECT Concordance.concord_id, Word.word,
                          Concordance.native, Concordance.lang,
                          Concordance.description, Wcb.num_verses,
                          Book.title
                          FROM Concordance
                          JOIN WordConcordanceBridge Wcb ON
                                      Concordance.concord_id = Wcb.concord_id
                          JOIN Word ON Wcb.word_id = Word.word_id
                          JOIN Book ON Wcb.most_common_book_id = Book.book_id
                          WHERE Word.word_ci = %s AND (Wcb.is_match = 1 {})
                          ORDER BY Wcb.bridge_id DESC
                          LIMIT %s
                          OFFSET %s;""".format(sim_clause),
                          (search_word.lower(), page_size+1, page*page_size))

        print("rows: "+str(cursor.rowcount))
        if cursor.rowcount:
            for idx in range(cursor.rowcount):
                if len(data) >= page_size:
                    has_next = True
                    break

                row = cursor.fetchone()
                data.append({'type': 'category',
                             'lang':row['lang'],
                             'native': row['native'],
                             'english': row['word'],
                             'description': row['description'],
                             'num_verse': row['num_verses'],
                             'common_book': row['title'],
                             'concord_ids': [row['concord_id']]})

                if False:
                    pass #highlight = verse_helper.get_highlight(row)

        cursor.execute("""SELECT Concordance.lang, COUNT(*) AS total
                          FROM Concordance
                          JOIN WordConcordanceBridge Wcb ON Concordance.concord_id = Wcb.concord_id
                          JOIN Word ON Wcb.word_id = Word.word_id
                          WHERE Word.word = %s AND (Wcb.is_match = 1 {})
                          GROUP BY Concordance.lang;""".format(sim_clause),
                          (search_word.lower(),))

        stats = []
        if cursor.rowcount:
            stat_data = []
            for idx in range(cursor.rowcount):
                row = cursor.fetchone()
                stat_data.append({'key': row['lang'], 'value': row['total']})

            if stat_data:
                stats.append({'type': 'doughnut', 'data': stat_data})

        cursor.close()
        return data, highlight, stats, has_next

    @staticmethod
    def text_search(search_str, similar, synonym, page, page_size):

        # get rid of leading/trailing space since they add nothing
        search_str = search_str.strip()
        found_it = False
        if search_str:
            # verse input
            verse_match = re.match("[-.0-9]*[ ]*[a-zA-Z]+[ ]*[-.0-9]+\:[-.0-9]+",
                                   search_str)

            # check to see that we have a match and that the length is
            # similar to the length of the input string ( this rules out
            # matches like "What does John 3:16 mean?"
            if verse_match and (len(verse_match.group()) + 4) > len(search_str):
                verse_parts = verse_match.group().split(":")
                book_match = re.match("[-.0-9]*[ ]*[a-zA-Z]+", verse_parts[0])
                book = book_match.group()
                data, highlight = Baseless.get_verse_specific(
                                    book, verse_parts[0][len(book):],
                                    verse_parts[1])
                if data:
                    found_it = True
                    resp = Response(json.dumps(
                        {
                            "data": data,
                            "highlight_verse": highlight,
                            "stats": []
                        }))

            # one word match
            elif search_str.find(' ') == -1:

                data, highlight, stats, next_page = Baseless.get_word(search_str, similar, synonym, page, page_size)
                if data:
                    found_it = True
                    resp = Response(json.dumps(
                        {
                            "data": data,
                            "highlight_verse": highlight,
                            "stats": stats,
                            "next_page": next_page
                        }))

            # phrase/question/statement search
            else:
                data, highlight, stats, next_page = Baseless.get_phrase(search_str, page, page_size)
                if data:
                    found_it = True
                    resp = Response(json.dumps(
                        {
                            "data": data,
                            "highlight_verse": highlight,
                            "stats": stats,
                            "next_page": next_page
                        }))

        # we found nothing, so return nothing
        if not found_it:
            resp = Response()
            resp.status_code = 204
        else:
            # all actual responses should be json
            resp.headers["Content-Type"] = "application/json"

        return resp

    @staticmethod
    def concord_search(concord, page, page_size):

        word_cursor = db.get().cursor(MySQLdb.cursors.DictCursor)
        data = []
        highlight = None

        word_cursor.execute("""SELECT word_tree
                              FROM Concordance WHERE concord_id=%s;""",
                              (concord,))

        if word_cursor.rowcount:
            tree = json.loads(word_cursor.fetchone()["word_tree"])

            for wd in tree:
                vs_cursor = db.get().cursor(MySQLdb.cursors.DictCursor)
                book_title = "Psalms"
                #highest_book_cnt = 0
                data.append({'type': 'category',
                             'lang': 'english',
                             'english': wd,
                             'num_verse': 17,
                             'common_book': book_title,
                             'words': [wd]})

                if book_title and not highlight:
                    pass #highlight = verse_helper.get_highlight(row)

                vs_cursor.close()

        word_cursor.close()
        found_it = False
        if data:
            found_it = True
            resp = Response(json.dumps(
                {
                    "data": data,
                    "highlight_verse": highlight,
                    "stats": []
                } ))

        # we found nothing, so return nothing
        if not found_it:
            resp = Response()
            resp.status_code = 204
        else:
            # all actual responses should be json
            resp.headers["Content-Type"] = "application/json"

        return resp
