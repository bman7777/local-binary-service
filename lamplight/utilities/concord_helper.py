import json
import MySQLdb
import pickle

from __init__ import db
from . import verse_helper

def from_str(search_str):
    low_src = search_str.lower()
    cursor = db.get().cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("""SELECT ordered_concord, word FROM Word
                      WHERE LOWER(word)=%s;""", (low_src,))

    merged_concords = []
    concord_set = set()
    for idx in range(cursor.rowcount):
        row = cursor.fetchone()
        this_con_set = pickle.loads(row['ordered_concord'])
        if not merged_concords:
            for concord,word_dist in this_con_set:
                if concord in concord_set:
                    continue
                concord_set.add(concord)
                merged_concords.append((concord,word_dist))
        else:
            # merge A and B as sorted lists
            # also make sure that the same concord isn't in 2 places
            curr_idx = 0
            for concord,word_dist in this_con_set:
                if concord in concord_set:
                    continue

                if curr_idx < len(merged_concords):
                    while word_dist > merged_concords[curr_idx][1]:
                        curr_idx += 1
                        if curr_idx >= len(merged_concords):
                            break

                if curr_idx >= len(merged_concords):
                    merged_concords.append((concord, word_dist))
                else:
                    merged_concords.insert(curr_idx, (concord, word_dist))

                concord_set.add(concord)
                curr_idx += 1

    cursor.close()
    return merged_concords

def resolve_concords(merged_concords, word_list):
    word_cursor = db.get().cursor(MySQLdb.cursors.DictCursor)
    data = []
    highlight = None
    used_natives = {}
    hebrew_track = {'key': 'Hebrew', 'value': 0}
    greek_track = {'key': 'Greek', 'value': 0}
    for concord,word_dist in merged_concords:

        if not highlight:
            word_cursor.execute("""SELECT Con.lang, Con.native, Word.word,
                                   Con.description,
                                   Ver.chapter, Ver.verse_num, Bk.title,
                                   Ver.hypertext, Ver.text, Ver.concord_set
                                   FROM Concordance As Con
                                   JOIN Verse as Ver ON Con.key_verse = Ver.hash
                                   JOIN Book as Bk ON Bk.book_id = Ver.book_id
                                   JOIN Word ON Con.primary_word_id = Word.word_id
                                   WHERE Con.concord_id=%s;""",
                                   (concord,))
        else:
            word_cursor.execute("""SELECT lang, native, Word.word, description
                                   FROM Concordance As Con
                                   JOIN Word ON Con.primary_word_id = Word.word_id
                                   WHERE concord_id=%s;""",
                                   (concord,))

        if word_cursor.rowcount:
            row = word_cursor.fetchone()
            desc = row['description'] if row['description'] else ""
            if row['native'] not in used_natives:
                used_natives[row['native']] = {}

            if desc not in used_natives[row['native']]:
                used_natives[row['native']][desc] = [concord]
                num_ver, book_title = verse_helper.count_verses_from_concord(
                    [concord], word_list)

                if row['lang'] == 'hebrew':
                    hebrew_track['value'] += num_ver
                else:
                    greek_track['value'] += num_ver

                data.append({'type': 'category',
                             'lang':row['lang'],
                             'native': row['native'],
                             'english': row['word'],
                             'description': desc,
                             'num_verse': num_ver,
                             'common_book': book_title})
            else:
                used_natives[row['native']][desc].append(concord)

            if not highlight:
                highlight = verse_helper.get_highlight(row)

    if data:
        for d in data:
            d['concord_ids'] = used_natives[d['native']][d['description']]

    word_cursor.close()

    return (data, highlight,
        [{'type': 'doughnut', 'data': [hebrew_track, greek_track]}])


def resolve_words(merged_concords):
    word_cursor = db.get().cursor(MySQLdb.cursors.DictCursor)
    data = []
    highlight = None

    for concord in merged_concords:

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
    return (data, highlight, [])
