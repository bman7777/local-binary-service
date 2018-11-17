import json
import MySQLdb
import pickle

from __init__ import db
from . import concord_helper, verse_helper

def _add_to_out_tree(out_tree, word_tree, word_key):
    for book in word_tree[word_key].keys():
        for chapter in word_tree[word_key][book].keys():
            for verse in word_tree[word_key][book][chapter]:

                if book in out_tree:
                    if chapter in out_tree[book]:
                        if verse in out_tree[book][chapter]:
                            continue

                if book not in out_tree:
                    out_tree[book] = {}

                if chapter not in out_tree[book]:
                    out_tree[book][chapter] = []

                if verse not in out_tree[book][chapter]:
                    out_tree[book][chapter].append(verse)

def _build_tree(out_tree, cursor, crit):
    if "native" in crit:
        cons = crit["native"].get("concords", [])
        cursor.execute("""SELECT word_tree FROM Concordance
                          WHERE concord_id IN %s;""", (tuple(cons),))

        engs = crit.get("english", {}).get("words", [])
        for idx in range(cursor.rowcount):
            word_tree = json.loads(cursor.fetchone()["word_tree"])
            if engs:
                for word_key in engs:
                    if word_key in word_tree:
                        _add_to_out_tree(out_tree, word_tree, word_key)
                    elif word_key.lower() in word_tree:
                        _add_to_out_tree(out_tree, word_tree, word_key.lower())
            else:
                for word_key in word_tree:
                    _add_to_out_tree(out_tree, word_tree, word_key)

    elif "english" in crit:
        in_words = crit["english"].get("words", [])
        word_set = set()
        for wd in in_words:
            word_set.add(wd)

            if wd[0].isupper():
                word_set.add(wd.lower())
            elif len(wd) > 1:
                word_set.add(wd[0].upper() + wd[1:])

        if word_set:
            quoted_words = [MySQLdb.escape_string(wd).decode('utf-8') for wd in word_set]
            sql_words = "('" + "', '".join(quoted_words) + "')"
            cursor.execute("""SELECT ordered_concord FROM Word
                              WHERE word IN """+sql_words)

            concord_set = set()
            for idx in range(cursor.rowcount):
                row = cursor.fetchone()
                this_con_set = pickle.loads(row['ordered_concord'])
                for concord,word_dist in this_con_set:
                    concord_set.add(concord)

            if concord_set:
                cursor.execute("""SELECT word_tree FROM Concordance
                                  WHERE concord_id IN %s;""",
                                  (tuple(concord_set),))
                for idx in range(cursor.rowcount):
                    word_tree = json.loads(cursor.fetchone()["word_tree"])
                    for word_key in word_set:
                        if word_key in word_tree:
                            _add_to_out_tree(out_tree, word_tree, word_key)
                        elif word_key.lower() in word_tree:
                            _add_to_out_tree(out_tree, word_tree, word_key.lower())


def from_criteria(criteria, page, page_size):
    out_tree = {}
    cursor = db.get().cursor(MySQLdb.cursors.DictCursor)

    for crit_idx, crit in enumerate(criteria):
        if crit_idx == 0:
            _build_tree(out_tree, cursor, crit)
        else:
            filter_out_tree = {}
            _build_tree(filter_out_tree, cursor, crit)

            tmp_out_tree = {}
            for book in filter_out_tree.keys():
                if book in out_tree:
                    for chapter in filter_out_tree[book].keys():
                        if chapter in out_tree[book]:
                            for verse in filter_out_tree[book][chapter]:
                                if verse in out_tree[book][chapter]:
                                    if book not in tmp_out_tree:
                                        tmp_out_tree[book] = {}

                                    if chapter not in tmp_out_tree[book]:
                                        tmp_out_tree[book][chapter] = []

                                    if verse not in tmp_out_tree[book][chapter]:
                                        tmp_out_tree[book][chapter].append(verse)

            out_tree = tmp_out_tree


    cursor.close()
    return verse_helper.resolve_verses(out_tree)
