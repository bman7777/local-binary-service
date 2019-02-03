import json
import re
import MySQLdb
from urllib.parse import unquote

from __init__ import app, db
from utilities import concord_helper, verse_helper
from flask import request, Response

@app.route('/baseless-search', methods=['GET'])
def baseless_search():
    if "concord" in request.args:
        return Baseless.concord_search(
            int(request.args.get("concord", "")),
            int(unquote(request.args.get("page", "0"))),
            int(unquote(request.args.get("pageSize", "15"))))
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
            synonym,
            similar,
            int(unquote(request.args.get("page", "0"))),
            int(unquote(request.args.get("pageSize", "15"))))


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
                                  Verse.text, Verse.chapter, Verse.verse_num,
                                  Verse.concord_set
                                  FROM Book JOIN Verse on
                                  Verse.book_id = Book.book_id WHERE
                                  (Book.short_title=%s or Book.title=%s)
                                  and
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
    def get_phrase(search_str):
        data = []
        highlight = None

        cursor = db.get().cursor(MySQLdb.cursors.DictCursor)
        cursor.execute("""SELECT verse_tree
                          FROM Phrase WHERE
                          (Book.short_title=%s or Book.title=%s)
                          and
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
    def text_search(search_str, page, page_size):

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

                low_src = search_str.lower()
                merged_concords = concord_helper.from_str(low_src, synonym, similar)

                # if there are multiple concords for this word, then
                # we need to ask the user for context
                num_concords = len(merged_concords)
                data = None
                highlight = None
                if num_concords > 1:
                    data, highlight, stats = concord_helper.resolve_concords(
                                        merged_concords, [low_src])
                elif num_concords == 1:
                    data, highlight, stats = verse_helper.from_concords(
                        [merged_concords[0][0]], [low_src], page, page_size)

                if data:
                    found_it = True
                    resp = Response(json.dumps(
                        {
                            "data": data,
                            "highlight_verse": highlight,
                            "stats": stats
                        } ))

            # phrase/question/statement search
            else:
                data, highlight = Baseless.get_phrase(search_str)
                if data:
                    found_it = True
                    resp = Response(json.dumps(
                        {
                            "data": data,
                            "highlight_verse": highlight,
                            "stats": []
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
        data, highlight, stats = concord_helper.resolve_words([concord])
        found_it = False
        if data:
            found_it = True
            resp = Response(json.dumps(
                {
                    "data": data,
                    "highlight_verse": highlight,
                    "stats": stats
                } ))

        # we found nothing, so return nothing
        if not found_it:
            resp = Response()
            resp.status_code = 204
        else:
            # all actual responses should be json
            resp.headers["Content-Type"] = "application/json"

        return resp
