
import json
import MySQLdb
from urllib.parse import unquote

from __init__ import app, db
from flask import request, Response
from utilities import verse_helper


@app.route('/search', methods=['POST'])
def combined_search():
    return Combination.search(
        request.get_json(),
        int(unquote(request.args.get("page", "0"))),
        int(unquote(request.args.get("pageSize", "50"))),
        unquote(request.args.get("book", "All")))


class Combination(object):

    @staticmethod
    def search(criteria, page, page_size, book):
        resp = None
        if criteria:
            cursor = db.get().cursor(MySQLdb.cursors.DictCursor)
            cursor_string, join_stmts, where_stmts = verse_helper.get_query_parts(
                criteria, page, page_size, False, book)
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
                criteria, page, page_size, True, book)
            cursor.execute(cursor_string + join_stmts + where_stmts)

            if data:
                resp = Response(json.dumps(
                    {
                        "data": data,
                        "highlight_verse": highlight,
                        "stats": verse_helper.get_stats(cursor),
                        "next_page": has_next
                    } ))

            cursor.close()

        # we found nothing, so return nothing
        if not resp:
            resp = Response()
            resp.status_code = 204
        else:
            # all actual responses should be json
            resp.headers["Content-Type"] = "application/json"

        return resp
