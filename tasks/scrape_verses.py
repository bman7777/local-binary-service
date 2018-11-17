import requests
import os
import json
from lxml import html
import sys
import MySQLdb

num_args = len(sys.argv)
if num_args < 3:
    print("Need book short name and chapter range to run this")
    exit()
elif num_args > 2 and not sys.argv[2].isdigit():
    print("param 2 must be a number")
    exit()
elif num_args > 3 and not sys.argv[3].isdigit():
    print("param 3 must be a number")
    exit()


def insert_verse(db, cursor, book_id, chapter, verse, text):
    hash_val = (book_id << 16) | (chapter << 8) | verse

    try:
        cursor.execute("""INSERT INTO Verse
                        (hash, book_id, chapter, verse_num, text)
                        VALUES(%s, %s, %s, %s, %s);""",
                        (hash_val, book_id, chapter, verse, text))
    except MySQLdb.Error as ins_err:
        print("rollback {}:{} due to: {}".format(chapter, verse, ins_err))
        db.rollback()
    finally:
        db.commit()
        print(str(chapter)+":"+str(verse)+" "+text)


def collect(db, book_id, short_bookname, start_chapter, end_chapter):
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    for chapter in range(start_chapter, end_chapter):
        req = requests.get("https://bibles.org/chapters/eng-NASB:"+
                           short_bookname + "." + str(chapter)+".js",
                           auth=(os.environ['BIBLES_ORG_API_KEY'],
                                 os.environ['BIBLES_ORG_API_PASS']))
        j = json.loads(req.text)
        tree = html.fromstring(j['chapters']['0']['chapter']['text'])

        verses = tree.xpath('//div[@class="chapter"]'
                            '//span[starts-with(@class, "v")]')

        running_text = ""
        running_vnum = 0

        for v in verses:
            v_num = v.xpath('.//sup[starts-with(@class, "v")]/text()')

            if not v_num:
                running_text += "\n"+v.text_content()
            else:
                if running_text and running_vnum:
                    insert_verse(db, cursor, book_id, chapter,
                                 running_vnum, running_text)

                assert(v_num[0].isdigit())
                running_vnum = int(v_num[0])
                running_text = v.text_content()[len(v_num[0]):]

        if running_text and running_vnum:
            insert_verse(db, cursor, book_id, chapter,
                         running_vnum, running_text)

def init():
    short_bookname = sys.argv[1]
    db = MySQLdb.connect(host=os.environ['LOCAB_DB_HOST'],
                             user=os.environ['LOCAB_DB_USER'],
                             passwd=os.environ['LOCAB_DB_PWORD'],
                             db='bman7777$LampLight_v1', charset="utf8")

    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("""SELECT book_id from Book where short_title=%s;""",
                   (short_bookname,))
    book_id = cursor.fetchone()["book_id"]
    cursor.close()

    if book_id:
        book_map = {}
        book_map["Ex"] = "Exod"
        book_map["1King"] = "1Kgs"
        book_map["2King"] = "2Kgs"
        book_map["1Chn"] = "1Chr"
        book_map["2Chn"] = "2Chr"
        book_map["Est"] = "Esth"
        book_map["Pslm"] = "Ps"
        book_map["Jrmh"] = "Jer"
        book_map["Lmnt"] = "Lam"
        book_map["Obdh"] = "Obad"
        book_map["Jnh"] = "Jonah"
        book_map["Mch"] = "Mic"
        book_map["Nahm"] = "Nah"
        book_map["Habk"] = "Hab"
        book_map["Hagg"] = "Hag"
        book_map["Malc"] = "Mal"
        book_map["Rmns"] = "Rom"
        book_map["Phlp"] = "Phil"
        book_map["1Ths"] = "1Thess"
        book_map["2Ths"] = "2Thess"
        book_map["Ttus"] = "Titus"
        book_map["Phil"] = "Phlm"
        book_map["Hbrw"] = "Heb"
        book_map["Jam"] = "Jas"
        book_map["1Ptr"] = "1Pet"
        book_map["2Ptr"] = "2Pet"
        book_map["1Jhn"] = "1John"
        book_map["2Jhn"] = "2John"
        book_map["3Jhn"] = "3John"

        if short_bookname in book_map:
            short_bookname = book_map[short_bookname]

        collect(db, book_id, short_bookname, int(sys.argv[2]),
                (int(sys.argv[3]) if num_args > 3 else int(sys.argv[2])) + 1)
    else:
        print("Invalid book")

    db.close()

init()