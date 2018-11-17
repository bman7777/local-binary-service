from lxml import html
import math
import requests
import os
import MySQLdb
import sys


# globals
known_replace = {}
ancient_heb = {}

def populate(db, cursor, lang):
    concord_cursor = db.cursor(MySQLdb.cursors.DictCursor)

    for idx in range(cursor.rowcount):
        this_row = cursor.fetchone()

        description = None
        english = None
        str_extern = str(this_row["extern_id"])

        if lang == 'hebrew' and not description:
            if str_extern not in ancient_heb:
                page_limit = int(math.ceil(this_row["extern_id"] / 500) * 500)
                page = requests.get(f"http://www.ancient-hebrew.org/m/"
                                    f"dictionary/{page_limit}.html")
                page.encoding = 'utf-8'
                tree = html.fromstring(page.content)

                concord_node = tree.xpath('//table/tr/td/a')
                for node in concord_node:
                    con_id = node.xpath(".//@name")
                    if con_id:
                        english_text = node.xpath(".//following-sibling::text()[4]")
                        desc_text = node.xpath(".//following-sibling::text()[5]")

                        ancient_heb[str(con_id[0])] = (english_text[0].strip().split(" (")[0],
                             desc_text[0].strip())

            if str_extern in ancient_heb:
                english = ancient_heb[str_extern][0]
                description = ancient_heb[str_extern][1]

        if lang == "greek" and not description:
            page = requests.get(f"https://studybible.info/strongs/G{str_extern}")
            page.encoding = 'utf-8'
            tree = html.fromstring(page.content)

            english_node = tree.xpath(
                '//section/article/dl/dd[@title="Dodson brief definition"]'
                '/text()')
            if english_node:
                english_words = english_node[0].strip().split(" ")
                if len(english_words) == 1:
                    english = english_words[0]

            description_node = tree.xpath(
                '//section/article/dl/dd[@title="Dodson longer definition"]'
                '/text()')
            if description_node:
                description = description_node[0].strip()

        if not description and str_extern in known_replace:
            description = known_replace[str_extern]

        if not description:
            page = requests.get(f"http://biblehub.com/{lang}/"
                                f"{this_row['extern_id']}.htm")
            page.encoding = 'utf-8'
            tree = html.fromstring(page.content)
            span_name = 'tophdg' if lang == 'greek' else 'hdg'
            description = tree.xpath('//div[@id="leftbox"]'
                                     '/div[@class="padleft"]/span[@class="'+
                                     span_name+'"]'
                                     '/text()[starts-with(., "Definition")]'
                                     '/ancestor::span'
                                     '/following-sibling::text()[1]')[0]

        if description:
            try:
                if english:
                    concord_cursor.execute("""UPDATE Concordance
                                              SET description = %s,
                                              english = %s
                                              WHERE concord_id = %s;""",
                                            (description, english,
                                             this_row['concord_id']))
                else:
                    concord_cursor.execute("""UPDATE Concordance
                                              SET description = %s
                                              WHERE concord_id = %s;""",
                                            (description, this_row['concord_id']))
            except MySQLdb.Error as ins_err:
                print("rollback due to: {}".format(ins_err))
                db.rollback()
            finally:
                print("committing extern_id: "+str(this_row["extern_id"]))
                db.commit()

    concord_cursor.close()

def init():
    num_args = len(sys.argv)
    if num_args < 1:
        print("Need a type 'hebrew'/'greek' to run this")
        exit()

    approved_replace = open(f"{sys.argv[1]}_definition_replace.txt","r")
    for line in approved_replace:
        line_parts = line.strip().split(" => ")
        if len(line_parts) == 2:
            known_replace[line_parts[0]] = line_parts[1]
    approved_replace.close()

    db = MySQLdb.connect(host=os.environ['LOCAB_DB_HOST'],
                         user=os.environ['LOCAB_DB_USER'],
                         passwd=os.environ['LOCAB_DB_PWORD'],
                         db='bman7777$LampLight_v1', charset="utf8")

    # cache off all books of bible into map for quick access
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("""SELECT * FROM Concordance WHERE lang = %s;""",
                   (sys.argv[1], ))

    # loop through all concordances of all languages and scape the relevant info
    populate(db, cursor, sys.argv[1])

    cursor.close()
    db.close()

init()
