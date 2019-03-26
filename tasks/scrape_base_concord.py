import json
import os
import pickle
import MySQLdb
from lxml import html
import requests
from nltk import word_tokenize


# globals
known_replace = {}
pointless_words = ["a", "from", "in", "about", "thing", "for", "what", "does",
                   "things", "ones", "one", "into", "being", "or", "to",
                   "than", "around", "off", "down", "back", "up", "away",
                   "through", "again", "after", "more", "along", "throughout",
                   "over", "forth", "freely", "the", "my"]
unknown_replace = open("unknown-replace.txt","w")

def prettyify(in_word, alternative, has_paren_count=False):

    if not in_word:
        in_word = alternative

    in_word = in_word.replace("*", "")
    val = None
    if has_paren_count:
        start_paren = in_word.find("(")
        end_paren = in_word.rfind(")")

        if start_paren >= 0 and end_paren >= 0:
            val = int(in_word[start_paren+1:end_paren])
            in_word = in_word[:start_paren]
        else:
            val = 0

    in_word = in_word.strip()
    if in_word.find(" ") < 0:
         return (in_word, val)
    else:
        if in_word in known_replace:
            return (known_replace[in_word], val)
        elif alternative:
            alternative = alternative.strip()
            return (alternative, val)
        else:
            # first try to find a keyword in it as a replacement
            word_list = in_word.split(" ")
            for word in reversed(word_list):
                if word not in pointless_words:
                    unknown_replace.write(f"'{in_word}' => '{word}'\n")
                    print(f"'{in_word}' => '{word}'")
                    return (word, val)

    return ("",)


def lev_dist(source, target):
    if source == target:
        return 0


    # Prepare a matrix
    slen, tlen = len(source), len(target)
    dist = [[0 for i in range(tlen+1)] for x in range(slen+1)]
    for i in range(slen+1):
        dist[i][0] = i
    for j in range(tlen+1):
        dist[0][j] = j

    # Counting distance, here is my function
    for i in range(slen):
        for j in range(tlen):
            cost = 0 if source[i] == target[j] else 1
            dist[i+1][j+1] = min(
                            dist[i][j+1] + 1,   # deletion
                            dist[i+1][j] + 1,   # insertion
                            dist[i][j] + cost   # substitution
                        )
    return dist[-1][-1]


def insert(db, cursor, concord_id, lang, english, verse_tree,
           trans_map, word_map):
    try:
        cursor.execute("""INSERT INTO Concordance
                        (extern_id, lang, english, verse_tree, word_tree)
                        VALUES(%s, %s, %s, %s, %s);""",
                        (concord_id, lang, english, json.dumps(verse_tree),
                         json.dumps(word_map)))
    except MySQLdb.Error as ins_err:
        print("rollback due to: {}".format(ins_err))
        db.rollback()
    finally:
        concord_row_key = str(cursor.lastrowid)
        db.commit()

        for trans in trans_map.keys():
            cursor.execute("""SELECT ordered_concord, word_id
                              FROM Word WHERE word = %s; """, (trans,))
            nat_dist = lev_dist(trans, english)
            if cursor.rowcount:
                row = cursor.fetchone()
                ord_con = pickle.loads(row['ordered_concord'])

                found_it = False
                for list_idx, tup in enumerate(ord_con):
                    if nat_dist <= tup[1]:
                        found_it = True
                        ord_con.insert(list_idx, (concord_row_key, nat_dist))
                        break

                if not found_it:
                    ord_con.append((concord_row_key, nat_dist))

                try:
                    cursor.execute("""UPDATE Word SET ordered_concord = %s
                                      WHERE word_id = %s; """,
                                   (pickle.dumps(ord_con,
                                            protocol=pickle.HIGHEST_PROTOCOL),
                                    row['word_id']))
                except MySQLdb.Error as up_err:
                    print("rollback due to: {}".format(up_err))
                    db.rollback()
                finally:
                    db.commit()
            else:
                try:
                    cursor.execute("""INSERT INTO Word(word, ordered_concord)
                                      VALUES(%s, %s);""",
                                   (trans, pickle.dumps(
                                            [(concord_row_key, nat_dist)],
                                            protocol=pickle.HIGHEST_PROTOCOL)))
                except MySQLdb.Error as wordins_err:
                    print("rollback due to: {}".format(wordins_err))
                    db.rollback()
                finally:
                    db.commit()


def update(db, cursor, concord_id, lang, verse_tree, word_map):
    try:
        cursor.execute("""UPDATE Concordance SET
                        verse_tree = %s, word_tree = %s
                        WHERE extern_id = %s AND lang = %s;""",
                        (json.dumps(verse_tree), json.dumps(word_map),
                         concord_id, lang))
    except MySQLdb.Error as ins_err:
        print("rollback due to: {}".format(ins_err))
        db.rollback()
    finally:
        db.commit()


def build_word_tree(root_node, trans_map, book_map, word_map, verse_tree):

    lower_trans_map = set(k.lower() for k in trans_map)
    len_lower_trans_map = len(lower_trans_map)
    default_trans = None
    default_cnt = 0
    for k,v in trans_map.items():
        if v > default_cnt:
            default_cnt = v
            default_trans = k

    for idx, v in enumerate(root_node):
        verse_text = v.xpath('b/a[@title="Biblos Lexicon"]/text()')
        if verse_text:
            verse_text = verse_text[0]
            divider = verse_text.rfind(" ")
            book_id = str(book_map[verse_text[:divider].lower()])
            colon = verse_text.rfind(":")
            chapter_num = verse_text[divider+1:colon]
            verse_num = int(verse_text[colon+1:])

            if book_id not in verse_tree:
                verse_tree[book_id] = {}

            if chapter_num not in verse_tree[book_id]:
                verse_tree[book_id][chapter_num] = []

            if verse_num not in verse_tree[book_id][chapter_num]:
                verse_tree[book_id][chapter_num].append(verse_num)

            version_list = v.xpath('a/text()')
            inner_tags = v.xpath(
                'a/following-sibling::span[@class="itali"]/text()')

            # these are not english, so skip them
            if version_list[0] in ('HEB:', 'GRK:'):
                del version_list[0]

            # make sure we are comparing apples to apples
            version_len = len(version_list)
            highlight_text = None
            if version_len == len(inner_tags):
                lookup = []
                for name in ('NAS:', 'INT:', 'KJV:'):
                    idx = next((idx for idx,it in enumerate(version_list)
                                if it == name), -1)
                    if idx >= 0:
                        lookup.append(inner_tags[idx])

                # now that we have the highlight text, find which word to tag
                success = False
                if lookup and len_lower_trans_map:
                    if len_lower_trans_map == 1:
                        highlight_text = next(iter(lower_trans_map))
                        success = True
                    else:
                        for phrase in lookup:
                            low_high_text = phrase.lower()
                            for word in word_tokenize(low_high_text):
                                if word in lower_trans_map:
                                    success = True
                                    highlight_text = word
                                    break

                            if success:
                                break

                if not success:
                    print("Subbing: "+('|').join(lookup)+" for: "+default_trans)
                    highlight_text = default_trans.lower()

                if highlight_text not in word_map:
                    word_map[highlight_text] = {}

                if book_id not in word_map[highlight_text]:
                    word_map[highlight_text][book_id] = {}

                if chapter_num not in word_map[highlight_text][book_id]:
                    word_map[highlight_text][book_id][chapter_num] = []

                if verse_num not in word_map[highlight_text][book_id][chapter_num]:
                    word_map[highlight_text][book_id][chapter_num].append(verse_num)


def fill_word_tree(url, word_tree, verse_tree, book_map, trans_map):
    page = requests.get(url)
    tree = html.fromstring(page.content)

    verses = tree.xpath('//div[@id="leftbox"]/'
                        'div[@class="padleft"]/p/b/'
                        'a[@title="Biblos Lexicon"]/text()')
    if verses:
        build_word_tree(
            tree.xpath('//div[@id="leftbox"]/div[@class="padleft"]/p'),
            trans_map, book_map, word_tree, verse_tree)

    return tree


def populate(lang_list, book_map, db, is_insert):
    cursor = db.cursor(MySQLdb.cursors.DictCursor)

    for lang in lang_list:
        for concord_id in range(1, 9000):
            page = requests.get(f"http://biblehub.com/{lang}/{concord_id}.htm")
            tree = html.fromstring(page.content)

            translit = tree.xpath('//div[@id="leftbox"]'
                                  '/div[@class="padleft"]/span[@class="tophdg"]'
                                  '/text()[starts-with(., "Transliteration:")]'
                                  '/ancestor::span'
                                  '/following-sibling::text()[1]')
            shortdef = tree.xpath('//div[@id="leftbox"]'
                                  '/div[@class="padleft"]/span[@class="tophdg"]'
                                  '/text()[starts-with(., "Short Definition:")]'
                                  '/ancestor::span'
                                  '/following-sibling::text()[1]')
            translation = tree.xpath('//div[@id="leftbox"]'
                                     '/div[@class="padleft"]/span[@class="hdg"]'
                                     '/text()[starts-with(., "NASB Translation")]'
                                     '/ancestor::span'
                                     '/following-sibling::text()[1]')

            if translit or shortdef:
                trans_map = {}
                if translation:
                    trans_list = translation[0].split(",")
                    for trans in trans_list:
                        word_tup = prettyify(trans, "", True)
                        if word_tup[0]:
                            if word_tup[0] in trans_map:
                                trans_map[word_tup[0]] += word_tup[1]
                            else:
                                trans_map[word_tup[0]] = word_tup[1]
                else:
                    word_tup = prettyify(shortdef[0] if shortdef else "",
                                         translit[0] if translit else "")
                    trans_map[word_tup[0]] = 9999 # a very big number

                word_tree = {}
                verse_tree = {}
                print(f"extern_id is: {concord_id}   lang is: {lang}")
                tree = fill_word_tree(f"http://biblehub.com/"
                                      f"{lang}/strongs_{concord_id}.htm",
                                      word_tree, verse_tree, book_map, trans_map)

                if word_tree:
                    english = prettyify(shortdef[0] if shortdef else "",
                                        translit[0] if translit else "")[0]

                    anc_list = tree.xpath('//div[@id="centbox"]/'
                        'div[@class="padcent"]/'
                        'a[following::div[@class="vheading2"]]/@href')
                    for anc in anc_list:
                        fill_word_tree(f"http://biblehub.com"+anc, word_tree,
                                       verse_tree, book_map, trans_map)

                    if is_insert:
                        insert(db, cursor, concord_id, lang, english,
                               verse_tree, trans_map, word_tree)
                    else:
                        update(db, cursor, concord_id, lang, verse_tree,
                               word_tree)

            else:
                print(f"no more concordance info at id: {concord_id}")
                break

    cursor.close()


def init():
    approved_replace = open("approved-replace.txt","r")
    for line in approved_replace:
        line_parts = line.strip().split("' => '")
        known_replace[line_parts[0][1:]] = line_parts[1][:-1]
    approved_replace.close()

    db = MySQLdb.connect(host=os.environ['LOCAB_DB_HOST'],
                         user=os.environ['LOCAB_DB_USER'],
                         passwd=os.environ['LOCAB_DB_PWORD'],
                         db='bman7777$LampLight_v1', charset="utf8")

    # cache off all books of bible into map for quick access
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("""SELECT book_id, title from Book;""")
    book_map = {}
    row = cursor.fetchone()
    while row is not None:
        book_map[row['title'].lower()] = row['book_id']
        row = cursor.fetchone()
    cursor.close()
    book_map["songs"] = book_map["song of solomon"]
    book_map["psalm"] = book_map["psalms"]

    # loop through all concordances of all languages and scape the relevant info
    populate(['hebrew', 'greek'], book_map, db, False)

    db.close()

init()
unknown_replace.close()
