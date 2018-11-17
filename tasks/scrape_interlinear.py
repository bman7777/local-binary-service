from lxml import html
import requests
import os
import MySQLdb
import pickle
import string
import sys
import unicodedata

from difflib import SequenceMatcher
from nltk import word_tokenize


def get_marked_words(in_text, phrase_concord_map):
    # initialize a marked word list that can
    # track phrases that are claimed
    marked_words = []
    for word in word_tokenize(in_text):
        marked_words.append({'word':word, 'concord':None, 'start': False})

    # search for exact matches from the longest strings to the shortest
    unused_concord_phrase = []
    for phr_top in phrase_concord_map:
        for item in phr_top[1]:
            search_list = word_tokenize(item[0])

            # find the longest word in the list
            longest_word = None
            longest_wd_len = 0
            phr_wd_idx = -1
            for idx, search in enumerate(search_list):
                if len(search) > longest_wd_len:
                    longest_word = search
                    longest_wd_len = len(search)
                    phr_wd_idx = idx

            # try to find this word in the marked list
            highest_affinity = 0.0

            if longest_word:
                mark_wd_idx = -1
                tie_list = []
                for idx, mark in enumerate(marked_words):
                    if not mark['concord']:
                        this_affinity = SequenceMatcher(None,
                            mark['word'].lower(), longest_word).ratio()
                        if this_affinity > highest_affinity:
                            highest_affinity = this_affinity
                            mark_wd_idx = idx
                            tie_list = [mark_wd_idx]
                        elif this_affinity == highest_affinity:
                            tie_list.append(idx)

                # look left and right to break ties
                if len(tie_list) > 1:
                    best_sibling_affinity = 0.0
                    for tie in tie_list:
                        left_affinity = 0.0
                        right_affinity = 0.0
                        if tie > 0 and phr_wd_idx > 0:
                            mark = marked_words[tie-1]
                            if not mark['concord']:
                                left_affinity = SequenceMatcher(None,
                                    mark['word'].lower(),
                                    search_list[phr_wd_idx-1]).ratio()

                        if tie < (len(marked_words) - 1) and phr_wd_idx < (len(search_list) - 1):
                            mark = marked_words[tie + 1]
                            if not mark['concord']:
                                right_affinity = SequenceMatcher(None,
                                    mark['word'].lower(),
                                    search_list[phr_wd_idx + 1]).ratio()

                        if left_affinity > right_affinity:
                            if left_affinity > best_sibling_affinity:
                                best_sibling_affinity = left_affinity
                                mark_wd_idx = tie
                        else:
                            if right_affinity > best_sibling_affinity:
                                best_sibling_affinity = right_affinity
                                mark_wd_idx = tie

            # if this word is worthy, mark it and surrounding words
            if highest_affinity > 0.75:
                marked_words[mark_wd_idx]['concord'] = item[1]
                low_search_list = search_list[0:phr_wd_idx]
                low_start_idx = mark_wd_idx
                for lower_idx in range(mark_wd_idx - 1, mark_wd_idx - phr_wd_idx - 1, -1):
                    if lower_idx < 0 or marked_words[lower_idx]['concord']:
                        break

                    good_affinity = 0.0
                    phr_match_wd_idx = -1
                    for lower_phr_idx in range(len(low_search_list) - 1, -1, -1):
                        this_affinity = SequenceMatcher(None,
                            low_search_list[lower_phr_idx],
                            marked_words[lower_idx]['word'].lower()).ratio()
                        if this_affinity > good_affinity:
                            good_affinity = this_affinity
                            phr_match_wd_idx = lower_phr_idx

                        if good_affinity >= 1.0:
                            break

                    if good_affinity > 0.60:
                        marked_words[lower_idx]['concord']  = item[1]
                        low_search_list.pop(phr_match_wd_idx)
                        low_start_idx = lower_idx
                    else:
                        break

                marked_words[low_start_idx]['start'] = True

                high_search_list = search_list[phr_wd_idx+1:]
                for higher_idx in range(mark_wd_idx + 1, mark_wd_idx + (len(search_list) - phr_wd_idx)):
                    if higher_idx >= len(marked_words) or marked_words[higher_idx]['concord']:
                        break

                    good_affinity = 0.0
                    phr_match_wd_idx = -1
                    for higher_phr_idx in range(0, len(high_search_list)):
                        this_affinity = SequenceMatcher(None,
                            high_search_list[higher_phr_idx],
                            marked_words[higher_idx]['word'].lower()).ratio()
                        if this_affinity > good_affinity:
                            good_affinity = this_affinity
                            phr_match_wd_idx = higher_phr_idx

                        if good_affinity >= 1.0:
                            break

                    if good_affinity > 0.60:
                        marked_words[higher_idx]['concord']  = item[1]
                        high_search_list.pop(phr_match_wd_idx)
                    else:
                        break

            else:
                unused_concord_phrase.append(phr_top[1])

    return marked_words


def populate(db, cursor, book_map, book_type):
    punc_table = str.maketrans({key: None for key in "-.?![]"})
    concord_cursor = db.cursor(MySQLdb.cursors.DictCursor)
    lang = 'hebrew' if book_type == 'old' else 'greek'

    row = cursor.fetchone()
    row_idx = 0
    while row:
        book_name = row['title'].lower()
        if book_name in book_map:
            book_name = book_map[book_name]

        page = requests.get(f"http://biblehub.com/interlinear/"
                            f"{book_name}/"
                            f"{row['chapter']}-{row['verse_num']}.htm")
        page.encoding = 'utf-8'
        tree = html.fromstring(page.content)

        if book_type == 'old':
            word_root = tree.xpath('//table[@class="tablefloatheb"]')
        else:
            word_root = tree.xpath('//table[@class="tablefloat"]')

        if not word_root:
            if (book_name == "3_john" and
                row['chapter'] == 1 and row['verse_num'] == 15):
                row = cursor.fetchone()
                continue

            sys.exit("Cannot read!!!!!! "+row['title']+"  "+
                  str(row['chapter'])+":"+str(row['verse_num']))

        phrase_concord_map = []
        for elem in word_root:

            if book_type == 'old':
                int_concord = elem.xpath('.//span[@class="strongs"]/a/text()')
            else:
                int_concord = elem.xpath('.//span[@class="pos"]/a/text()')

            int_eng = elem.xpath('.//span[@class="eng"]/text()')

            for concord in int_concord:
                if concord.isdigit():
                    good_phrase = None
                    for phrase in int_eng:
                        tmp_word = unicodedata.normalize("NFKD", phrase)
                        tmp_word = tmp_word.strip().translate(punc_table)
                        if tmp_word:
                            good_phrase = tmp_word.lower()
                            break

                    if good_phrase:
                        len_gp = len(good_phrase)
                        if not phrase_concord_map:
                            phrase_concord_map.append(
                                (len_gp, [(good_phrase, int(concord))]))
                        else:
                            found_it = False
                            for idx, phr in enumerate(phrase_concord_map):
                                if len_gp > phr[0]:
                                    found_it = True
                                    phrase_concord_map.insert(idx,
                                        (len_gp,
                                         [(good_phrase, int(concord))]))
                                    break
                                elif len_gp == phr[0]:
                                    found_it = True
                                    phr[1].append(
                                        (good_phrase, int(concord)))
                                    break

                            if not found_it:
                                phrase_concord_map.append((len_gp,
                                    [(good_phrase, int(concord))]))

        marked_words = get_marked_words(row['text'], phrase_concord_map)

        # build a result string that contains spans over the phrases
        result_str = ""
        was_spanning = False
        concord_set = set()
        closing_quote_str = "”’"
        opening_quote_str = "“‘'\""
        quote_str = opening_quote_str + closing_quote_str
        for idx, mk in enumerate(marked_words):
            if mk["start"]:
                concord_cursor.execute(""" SELECT concord_id FROM
                                           Concordance
                                           WHERE extern_id = %s and
                                           lang = %s;""",
                                       (mk['concord'], lang))
                if was_spanning:
                    result_str += "</span>"

                if ((result_str[-1:] not in quote_str) and
                    (mk['word'][0] not in quote_str) and
                    (was_spanning or (idx > 0))):
                    result_str += " "

                concord_id = concord_cursor.fetchone()
                if concord_id:
                    concord_id = concord_id["concord_id"]
                concord_set.add(concord_id)
                result_str += ("<span class='kw' id='concord-"+
                               str(concord_id)+"'>")
                result_str += mk['word']
                was_spanning = True
            else:
                if not mk["concord"] and was_spanning:
                    result_str += "</span>"
                    if ((mk['word'][0] not in string.punctuation) and
                        (mk['word'][0] not in quote_str)):
                        result_str += " "
                    was_spanning = False
                    result_str += mk['word']
                elif mk['word'][0] in string.punctuation:
                    result_str += mk['word']
                else:
                    if ((result_str[-1:] not in opening_quote_str) and
                        (mk['word'][0] not in closing_quote_str)):
                        result_str += " "
                    result_str += mk['word']

        if was_spanning:
            result_str += "</span>"

        try:
            concord_cursor.execute("""UPDATE Verse SET hypertext = %s,
                                      concord_set = _binary %s
                                      WHERE hash = %s;""",
                                   (result_str, pickle.dumps(concord_set,
                                    protocol=pickle.HIGHEST_PROTOCOL),
                                    row['hash']))
        except MySQLdb.Error as ins_err:
            print("rollback {}:{} due to: {}".format(row['chapter'],
                row['verse_num'], ins_err))
            db.rollback()
        finally:
            db.commit()
            print(row['title']+"  "+str(row['chapter'])+":"+
                  str(row['verse_num']))

        row = cursor.fetchone()
        row_idx += 1
    concord_cursor.close()

def init():
    num_args = len(sys.argv)
    if num_args < 1:
        print("Need a type 'old'/'new' to run this")
        exit()

    db = MySQLdb.connect(host=os.environ['LOCAB_DB_HOST'],
                         user=os.environ['LOCAB_DB_USER'],
                         passwd=os.environ['LOCAB_DB_PWORD'],
                         db='bman7777$LampLight_v1', charset="utf8")

    # cache off all books of bible into map for quick access
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    if num_args == 5:
        cursor.execute("""SELECT * FROM Verse
                          JOIN Book ON Verse.book_id = Book.book_id
                          WHERE
                          Book.title = %s AND Verse.chapter = %s AND
                          Verse.verse_num = %s;""",
                          (sys.argv[2], sys.argv[3], sys.argv[4]))
    else:
        cursor.execute("""SELECT * FROM Verse
                          JOIN Book ON Verse.book_id = Book.book_id
                          WHERE
                          Book.book_id >= 64 and Book.book_id <= 66 and
                          Book.type = %s;""", (sys.argv[1],))

    book_map = {}
    book_map["1 samuel"] = "1_samuel"
    book_map["2 samuel"] = "2_samuel"
    book_map["1 kings"] = "1_kings"
    book_map["2 kings"] = "2_kings"
    book_map["1 chronicles"] = "1_chronicles"
    book_map["2 chronicles"] = "2_chronicles"
    book_map["song of solomon"] = "songs"
    book_map["1 corinthians"] = "1_corinthians"
    book_map["2 corinthians"] = "2_corinthians"
    book_map["1 thessalonians"] = "1_thessalonians"
    book_map["2 thessalonians"] = "2_thessalonians"
    book_map["1 timothy"] = "1_timothy"
    book_map["2 timothy"] = "2_timothy"
    book_map["1 peter"] = "1_peter"
    book_map["2 peter"] = "2_peter"
    book_map["1 john"] = "1_john"
    book_map["2 john"] = "2_john"
    book_map["3 john"] = "3_john"

    # loop through all concordances of all languages and scape the relevant info
    populate(db, cursor, book_map, sys.argv[1])

    cursor.close()
    db.close()

init()
