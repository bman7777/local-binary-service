import requests
import os
import json
import MySQLdb


def get_word_info(word):
    resp = requests.get(f"https://wordsapiv1.p.mashape.com/words/{word}",
                        headers={'X-Mashape-Key': os.environ['WORDS_API_PWORD'],
                                 'X-Mashape-Host': 'wordsapiv1.p.mashape.com'} )

    if resp.status_code == 200:
        info = json.loads(resp.text)
        return True, info.get('frequency', 0.0), info.get('results', [])
    else:
        print(f"Could not find info for {word}")
        return False, 0.0, []

def insert_word_info(db, word_id, frequency, details):
    word_cursor = db.cursor(MySQLdb.cursors.DictCursor)

    # fixup details to only have the stuff we care about
    fixed_details = []

    for det in details:
        fixed = {}
        if "partOfSpeech" in det:
            fixed['pos'] = det["partOfSpeech"]

        if "definition" in det:
            fixed['def'] = det["definition"]

        if "synonyms" in det:
            fixed['syn'] = det["synonyms"]

        if "similarTo" in det:
            fixed['sim'] = det["similarTo"]

        if "also" in det:
            fixed['also'] = det["also"]

        if fixed:
            fixed_details.append(fixed)

    try:
        word_cursor.execute("""UPDATE Word SET frequency = %s, details = %s
                               WHERE word_id = %s;""",
                               (frequency, json.dumps(fixed_details), word_id))
    except MySQLdb.Error as ins_err:
        print("rollback {} due to: {}".format(word_id, ins_err))
        db.rollback()
    finally:
        db.commit()

    word_cursor.close()

def init():
    db = MySQLdb.connect(host=os.environ['LOCAB_DB_HOST'],
                         user=os.environ['LOCAB_DB_USER'],
                         passwd=os.environ['LOCAB_DB_PWORD'],
                         db='bman7777$LampLight_v1', charset="utf8")

    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("""SELECT word, word_id from Word
                      WHERE frequency is NULL OR details is NULL;""")

    skip_words = []
    skip_words_iob = open("unknown-wordlookup.txt","r")
    for line in skip_words_iob:
        skip_words.append(line.strip())
    skip_words_iob.close()

    row_idx = 0
    row = cursor.fetchone()
    while row:
        if row["word"] not in skip_words:
            success, frequency, detail = get_word_info(row["word"])
            if success:
                insert_word_info(db, row["word_id"], frequency, detail)

            row_idx += 1
            if row_idx > 2400:
                break

        row = cursor.fetchone()

    cursor.close()
    db.close()

init()