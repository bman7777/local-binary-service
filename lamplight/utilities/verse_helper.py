import collections
import operator
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


def get_stats(cursor):
    stats = []
    if cursor.rowcount:
        book_list = []
        author_freq = collections.defaultdict(int)
        verse_count = 0
        for idx in range(cursor.rowcount):
            row = cursor.fetchone()
            book_list.append({"name": row["title"], "count": row["total"]})
            verse_count += row["total"]
            author_freq[row["author"]] += row["total"]

        sorted_author_freq = sorted(author_freq.items(),
                                    key=operator.itemgetter(1))
        num_sorted = len(sorted_author_freq)
        other = 0
        if num_sorted > 2:
            for idx in range(0, len(sorted_author_freq) - 2):
                other += sorted_author_freq[idx][1]

            sorted_author_freq = sorted_author_freq[-2:]

        translated_list = []
        for item in reversed(sorted_author_freq):
            translated_list.append({'key': item[0], 'value': item[1]})

        if other:
            translated_list.append({'key': 'Other', 'value': other})

        stats = [{'type': 'doughnut', 'data': translated_list}]
        if verse_count:
            stats.append({'type': 'stat', 'data': [
                {"key": "Verse Matches:", "value":verse_count}]})
        if book_list:
            stats.append({'type': 'tabs', 'data': book_list})

    return stats


def get_query_parts(criteria, page, page_size, is_stat, book):
    cursor_string = ""
    join_stmts = ""
    where_stmts = " WHERE"
    curr_char = 'A'
    is_first = True
    for crit in criteria:
        if is_first:
            if is_stat:
                cursor_string += ("SELECT Book.title, Book.author,"
                                  " COUNT(*) AS total FROM Verse {}".format(
                                      curr_char))
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
        if book != 'All':
            where_stmts += ' AND Book.title = "'+book+'" '

        where_stmts += " ORDER BY A.hash ASC LIMIT {} OFFSET {}".format(
            page_size+1, page*page_size)

    return cursor_string, join_stmts, where_stmts
