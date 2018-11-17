import json
import MySQLdb
import random

from __init__ import app, db
from flask import request, Response
from urllib.parse import unquote
from utilities import verse_helper


@app.route('/verse', methods=['GET'])
def verse_lookup():
    return Verse.fetch(
        unquote(request.args.get("book", "")),
        unquote(request.args.get("chapter", "")),
        unquote(request.args.get("verse", "")))


@app.route('/random-verse', methods=['GET'])
def random_verse():
    verse_list = [
        ("John", "3", "16"),
        ("Romans", "8", "28"),
        ("Hebrews", "11", "1"),
        ("2 Timothy", "4", "7"),
        ("Revelation", "1", "7"),
        ("Joshua", "1", "9"),
        ("Jeremiah", "29", "11"),
        ("Deuteronomy", "6", "5"),
        ("Hebrews", "11", "10"),
        ("Daniel", "12", "3"),
    ]

    verse_pick = random.choice(verse_list)
    return Verse.fetch(verse_pick[0], verse_pick[1], verse_pick[2])

class Verse(object):

    @staticmethod
    def fetch(book, chapter, verse):
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
                                  Verse.text, Verse.concord_set, Verse.chapter,
                                  Verse.verse_num
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

        resp= None
        if data:
            resp = Response(json.dumps({"data": data,
                        "highlight_verse": highlight}))
            resp.headers["Content-Type"] = "application/json"

        return resp
