
import collections
import json
import MySQLdb
import operator
from urllib.parse import unquote

from __init__ import app, db
from flask import request, Response
from utilities import verse_helper


@app.route('/search', methods=['POST'])
def combined_search():
    return Combination.search(
        request.get_json(),
        int(unquote(request.args.get("page", "0"))),
        int(unquote(request.args.get("pageSize", "50"))))


class Combination(object):

    @staticmethod
    def get_stats(cursor):
        stats = []
        if cursor.rowcount:
            book_freq = collections.defaultdict(int)
            verse_count = 0
            for idx in range(cursor.rowcount):
                row = cursor.fetchone()
                book_freq[row["title"]] = row["total"]
                verse_count += row["total"]

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
                stats.append({'type': 'stat', 'data': [
                    {"key": "Verse Matches:", "value":verse_count}]})

        return stats


    @staticmethod
    def get_query_parts(criteria, page, page_size, is_stat):
        cursor_string = ""
        join_stmts = ""
        where_stmts = " WHERE"
        curr_char = 'A'
        is_first = True
        for crit in criteria:
            if is_first:
                if is_stat:
                    cursor_string += ("SELECT Book.title, COUNT(*) AS total"
                                      " FROM Verse {}".format(curr_char))
                else:
                    cursor_string += ("SELECT Book.title, {}.hypertext, {}.text,"
                                      " {}.chapter, {}.verse_num, {}.concord_set"
                                      " FROM Verse {}".format(curr_char, curr_char,
                                      curr_char, curr_char, curr_char, curr_char))
                join_stmts += " JOIN Book ON {}.book_id = Book.book_id".format(curr_char)
                is_first = False
            else:
                where_stmts += " AND"
                cursor_string += " INNER JOIN Verse {} ON A.hash = {}.hash".format(
                    curr_char, curr_char)

            if "native" in crit:
                join_stmts += (" JOIN ConcordanceVerseBridge {}vb ON "
                               "{}.hash = {}vb.verse_hash").format(
                                   curr_char, curr_char, curr_char)
                join_stmts += (" JOIN Concordance {}c ON "
                               "{}c.concord_id = {}vb.concord_id").format(
                                   curr_char, curr_char, curr_char)

                where_stmts += " {}c.concord_id = {}".format(
                    curr_char, crit["native"]["concords"][0])

            elif "english" in crit:
                if " " in crit["english"]["words"][0]:
                    join_stmts += (" JOIN PhraseVerseBridge {}vb ON "
                                   "{}.hash = {}vb.verse_hash").format(
                                       curr_char, curr_char, curr_char)
                    join_stmts += (" JOIN Phrase {}p ON "
                                   "{}p.phrase_id = {}vb.phrase_id").format(
                                       curr_char, curr_char, curr_char)

                    where_stmts += ' {}p.phrase_ci = "{}"'.format(
                        curr_char, crit["english"]["words"][0].lower())
                else:
                    join_stmts += (" JOIN WordVerseBridge {}vb ON "
                                   "{}.hash = {}vb.verse_hash").format(
                                       curr_char, curr_char, curr_char)
                    join_stmts += (" JOIN Word {}w ON "
                                   "{}w.word_id = {}vb.word_id").format(
                                       curr_char, curr_char, curr_char)

                    where_stmts += (' {}w.word_ci = "{}" AND ({}vb.is_match=1').format(
                        curr_char, crit["english"]["words"][0], curr_char)

                    if crit["english"].get("synonym", False):
                        where_stmts += " OR {}vb.is_synonym=1".format(curr_char)

                    if crit["english"].get("similar", False):
                        where_stmts += " OR {}vb.is_similar=1".format(curr_char)

                    where_stmts += ")"

            curr_char = chr(ord(curr_char)+1)

        if is_stat:
            where_stmts += " GROUP BY A.book_id"
        else:
            where_stmts += " LIMIT {} OFFSET {}".format(page_size, page*page_size)

        return cursor_string, join_stmts, where_stmts


    @staticmethod
    def search(criteria, page, page_size):
        resp = None
        if criteria:
            cursor = db.get().cursor(MySQLdb.cursors.DictCursor)
            cursor_string, join_stmts, where_stmts = Combination.get_query_parts(
                criteria, page, page_size, False)
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

            cursor_string, join_stmts, where_stmts = Combination.get_query_parts(
                criteria, page, page_size, True)
            cursor.execute(cursor_string + join_stmts + where_stmts)

            if data:
                resp = Response(json.dumps(
                    {
                        "data": data,
                        "highlight_verse": highlight,
                        "stats": Combination.get_stats(cursor),
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
