
from collections import defaultdict
import os
import MySQLdb
import operator
import pickle
import unicodedata
import re
import string


def is_meaningful(words):
    num_words = 0
    for w in words:
        if w.lower() not in ('the', 'and', 'is', 'was', 'there', 'he', 'from', 'of', 'in', 'then', 'to', 'not', 'a', 'it', 'for', 'will', 'who'):
            num_words += 1
            if num_words >= 2:
                return True

    return False


def merge_into(src_dict, file_name):
    if not os.path.exists(file_name) or os.path.getsize(file_name) <= 0:
        pickle.dump( src_dict.copy(), open( file_name, "wb" ) )
    else:
        with open(file_name, 'rb') as file_object:
            content = pickle.load(file_object)

            for phrase, vrs_tree in src_dict.items():
                for book, chap in vrs_tree.items():
                    for chp_num, verses in chap.items():
                        if book not in content[phrase]:
                            content[phrase][book] = {}
                        book_dict = content[phrase][book]
                        if chp_num not in book_dict:
                            book_dict[chp_num] = verses
                        else:
                            book_dict[chp_num] = list(set(book_dict[chp_num] + verses))

        pickle.dump( content, open( file_name, "wb" ) )


def find_fragments(db, db_row, primary_dict):
    table = str.maketrans({key: None for key in string.punctuation})
    fixed_str = unicodedata.normalize('NFKD', db_row['text']).encode(
        'ascii', 'ignore').decode()

    num_found = 0
    for part in fixed_str.split('"'):
        for subpart in re.split('[!0-9]*,[!0-9]*', part):
            spacedparts = [str(e.translate(table)) for e in subpart.split(" ") if e]
            num_spacedparts = len(spacedparts)
            if num_spacedparts >= 2:
                for idx in range(num_spacedparts-1):
                    for num_words in range(2, num_spacedparts - idx):
                        if is_meaningful(spacedparts[idx:idx+num_words]):
                            phrase = (" ".join(spacedparts[idx:idx+num_words])).strip()
                            ltr = phrase[0].lower()
                            if ltr not in primary_dict:
                                primary_dict[ltr] = defaultdict(dict)

                            str_book = str(db_row['book_id'])
                            str_chap = str(db_row['chapter'])
                            if str_book not in primary_dict[ltr][phrase]:
                                primary_dict[ltr][phrase][str_book] = {}
                            book_dict = primary_dict[ltr][phrase][str_book]
                            if str_chap not in book_dict:
                                book_dict[str_chap] = []
                            combined = book_dict[str_chap] + [db_row['verse_num']]
                            book_dict[str_chap] = list(set(combined))

                            if len(primary_dict[ltr]) > 2500:
                                merge_into(primary_dict[ltr], 'phrases/'+ltr+'.txt')
                                primary_dict[ltr].clear()

                            num_found += 1

    return num_found


def print_tops():
    for ltr in '0123456789abcdefghijklmnopqrstuvwxyz':
        file_name = 'phrases/'+ltr+'.txt'
        if os.path.exists(file_name) and os.path.getsize(file_name) > 0:
            with open(file_name, 'rb') as file_object:
                content = pickle.load(file_object)
                max_info = max(content.items(), key=operator.itemgetter(1))
                print("starting with: "+ltr+" => "+max_info[0]+" ("+str(max_info[1])+")")


def insert_phrases(db, cursor):
    for ltr in '0123456789abcdefghijklmnopqrstuvwxyz':
        file_name = 'phrases/'+ltr+'.txt'
        if os.path.exists(file_name) and os.path.getsize(file_name) > 0:
            with open(file_name, 'rb') as file_object:
                content = pickle.load(file_object)

                for phr, verse_tree in content.items():
                    try:
                        cursor.execute("""INSERT INTO Phrase(phrase, verse_tree) values(%s, %s);""", (phr, str(verse_tree)))
                    except MySQLdb.Error as up_err:
                        print("rollback due to: {}".format(up_err))
                        db.rollback()
                    finally:
                        db.commit()


def init():

    db = MySQLdb.connect(host=os.environ['LOCAB_DB_HOST'],
                         user=os.environ['LOCAB_DB_USER'],
                         passwd=os.environ['LOCAB_DB_PWORD'],
                         db='bman7777$LampLight_v1', charset="utf8")

    # cache off all books of bible into map for quick access
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("""SELECT text, book_id, chapter, verse_num from Verse;""")

    row = cursor.fetchone()
    num_found = 0
    print_digit = 0
    primary_dict = {}
    while row is not None:
        num_found += find_fragments(db, row, primary_dict)
        row = cursor.fetchone()
        if (num_found / 100) >= print_digit:
            print(str(num_found)+" phrases found so far")
            print_digit += 1

    for ltr, phrase_dict in primary_dict.items():
        if phrase_dict:
            merge_into(phrase_dict, 'phrases/'+ltr+'.txt')

    insert_phrases(db, cursor)

    print("Total found: "+str(num_found))
    cursor.close()

    # print_tops()

    db.close()

init()