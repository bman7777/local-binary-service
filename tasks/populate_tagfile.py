from nltk import word_tokenize, pos_tag, ne_chunk, tree2conlltags
import nltk.data
import os
import sys
import MySQLdb
import unicodedata

END_TAG = "<END>"

def process_ner(iob_list_pos, iob_list_word, iob_list_iob,
                approved, log_unapproved):

    num_words = len(iob_list_word)
    sent_range = iter(range(num_words))
    for idx in sent_range:
        word = iob_list_word[idx]

        # is the word in a root lookup?
        last_viable_idx = None
        if word in approved:

            # advance the state machine
            last_viable_entity = approved[word]['entity']
            next_dict = approved[word]['children']

            # keep track of the tail of the run
            if END_TAG in next_dict:
                last_viable_idx = idx

            # loop to find the end of the tail
            for next_idx in range(idx+1, num_words):
                next_word = iob_list_word[next_idx]
                if next_word in next_dict:
                    ent = next_dict[next_word]['entity']
                    next_dict = next_dict[next_word]['children']
                    if END_TAG in next_dict:
                        last_viable_idx = next_idx
                        last_viable_entity = ent
                else:
                    break

        if last_viable_idx != None:
            ent_parts = last_viable_entity.split("-")

            # set the iob for the range
            for iob_idx in range(idx, last_viable_idx + 1):
                if len(ent_parts) <= 1:
                    iob_list_iob[iob_idx] = last_viable_entity
                else:
                    prefix = "I-" if (iob_idx + 1) > last_viable_idx else "B-"
                    iob_list_iob[iob_idx] = prefix + ent_parts[1]

                if iob_idx < last_viable_idx:
                    next(sent_range)
        else:
            if iob_list_iob[idx] != 'O' or word.isupper():
                txt = word+" => "+iob_list_iob[idx]

                # keep track of unapproved words so we can deal with them by the
                # most common ones primarily.  One-offs aren't as concerning.
                if txt in log_unapproved:
                    log_unapproved[txt] += 1
                else:
                    log_unapproved[txt] = 1

            iob_list_iob[idx] = "O"


def process_sent(sent, bible_tags, approved, log_unapproved):
    tree = ne_chunk(pos_tag(word_tokenize(sent)))
    iob_list_pos = []
    iob_list_word = []
    iob_list_iob = []
    for idx,iob in enumerate(tree2conlltags(tree)):
        word = iob[0].replace("*", "")
        if word.startswith("b'"):
            word = word[2:]

        for w in word.split("\\n"):
            if w:
                iob_list_pos.append(iob[1])
                iob_list_word.append(w)
                iob_list_iob.append(iob[2])

    # fix-up runs of multiple words with proper iob
    process_ner(iob_list_pos, iob_list_word, iob_list_iob,
                approved, log_unapproved)

    for idx,pos in enumerate(iob_list_pos):
        bible_tags.write(pos+", "+ iob_list_word[idx]+", "+
                         iob_list_iob[idx]+"\n")

    bible_tags.write("\n\n")


def collect(db, limit, bible_tags, approved, new_old):

    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')

    cursor.execute("""SELECT Verse.text from Verse JOIN Book on
                      Book.book_id = Verse.book_id WHERE Book.type = %s
                      LIMIT %s;""", (new_old, limit,))
    row = cursor.fetchone()

    log_unapproved = {}
    buffer_sent = ""
    while row:
        if buffer_sent:
            buffer_sent += " "
        buffer_sent += str(unicodedata.normalize('NFKD', row['text']).encode(
                           'ascii', 'ignore'))

        # <NUMBER> shekels
        # Check=> Israel, Will

        sent = tokenizer.tokenize(buffer_sent)
        for idx in range(0, len(sent)-1):
            process_sent(sent[idx], bible_tags, approved, log_unapproved)

        buffer_sent = sent[len(sent)-1]
        row = cursor.fetchone()

    if buffer_sent:
        process_sent(buffer_sent, bible_tags, approved, log_unapproved)

    for item in log_unapproved:
        if log_unapproved[item] > 2:
            print(item)

    cursor.close()



def init():
    old_limit = int(sys.argv[1])
    new_limit = int(sys.argv[2]) if len(sys.argv) > 2 else 0

    db = MySQLdb.connect(host=os.environ['LOCAB_DB_HOST'],
                         user=os.environ['LOCAB_DB_USER'],
                         passwd=os.environ['LOCAB_DB_PWORD'],
                         db='bman7777$LampLight_v1', charset="utf8")

    approved = {}
    approved_iob = open("approved-iob.txt","r")
    for line in approved_iob:
        line_parts = line.strip().split(" => ")

        spaced_words = line_parts[0].strip().split(" ")
        prev_dict = None
        for word in spaced_words:
            if prev_dict != None:
                if word not in prev_dict:
                    prev_dict[word] = {'children':{}, 'entity':line_parts[1]}
                prev_dict = prev_dict[word]['children']
            else:
                if word not in approved:
                    approved[word] = {'children':{}, 'entity':line_parts[1]}
                prev_dict = approved[word]['children']

        prev_dict[END_TAG] = {}

    approved_iob.close()

    bible_tags = open("bible-tags.txt","w")
    if old_limit > 0:
        collect(db, old_limit, bible_tags, approved, 'old')

    if new_limit > 0:
        collect(db, new_limit, bible_tags, approved, 'new')
    bible_tags.close()

    db.close()

init()
