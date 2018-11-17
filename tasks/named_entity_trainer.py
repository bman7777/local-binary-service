import os
import pickle
from collections import Iterable
from nltk import conlltags2tree
from nltk.tag import ClassifierBasedTagger
from nltk.chunk import ChunkParserI
import string
from nltk.stem.snowball import SnowballStemmer


def read_sentences(corpus_root):

    with open(corpus_root, 'rb') as file_handle:
        file_content = file_handle.read().decode('utf-8').strip()
        annotated_sentences = file_content.split('\n\n')
        for annotated_sentence in annotated_sentences:
            annotated_tokens = [seq for seq in annotated_sentence.split('\n') if seq]

            standard_form_tokens = []

            for idx, annotated_token in enumerate(annotated_tokens):
                annotations = annotated_token.split(',')
                if len(annotations) == 3:
                    word, tag, ner = annotations[1].strip(), annotations[0].strip(), annotations[2].strip()
                    standard_form_tokens.append((word, tag, ner))

            if standard_form_tokens:
                # Make it NLTK Classifier compatible - [(w1, t1, iob1), ...] to [((w1, t1), iob1), ...]
                # Because the classfier expects a tuple as input, first item input, second the class
                yield [((w, t), iob) for w, t, iob in standard_form_tokens]


def features(tokens, index, history):
    """
    `tokens`  = a POS-tagged sentence [(w1, t1), ...]
    `index`   = the index of the token we want to extract features for
    `history` = the previous predicted IOB tags
    """

    # init the stemmer
    stemmer = SnowballStemmer('english')

    # Pad the sequence with placeholders
    tokens = [('[START2]', '[START2]'), ('[START1]', '[START1]')] + list(tokens) + [('[END1]', '[END1]'), ('[END2]', '[END2]')]
    history = ['[START2]', '[START1]'] + list(history)

    # shift the index with 2, to accommodate the padding
    index += 2

    word, pos = tokens[index]
    prevword, prevpos = tokens[index - 1]
    prevprevword, prevprevpos = tokens[index - 2]
    nextword, nextpos = tokens[index + 1]
    nextnextword, nextnextpos = tokens[index + 2]
    previob = history[index - 1]
    contains_dash = '-' in word
    contains_dot = '.' in word
    allascii = all([True for c in word if c in string.ascii_lowercase])

    allcaps = word == word.capitalize()
    capitalized = word[0] in string.ascii_uppercase

    prevallcaps = prevword == prevword.capitalize()
    prevcapitalized = prevword[0] in string.ascii_uppercase

    nextallcaps = prevword == prevword.capitalize()
    nextcapitalized = prevword[0] in string.ascii_uppercase

    return {
        'word': word,
        'lemma': stemmer.stem(word),
        'pos': pos,
        'all-ascii': allascii,

        'next-word': nextword,
        'next-lemma': stemmer.stem(nextword),
        'next-pos': nextpos,

        'next-next-word': nextnextword,
        'nextnextpos': nextnextpos,

        'prev-word': prevword,
        'prev-lemma': stemmer.stem(prevword),
        'prev-pos': prevpos,

        'prev-prev-word': prevprevword,
        'prev-prev-pos': prevprevpos,

        'prev-iob': previob,

        'contains-dash': contains_dash,
        'contains-dot': contains_dot,

        'all-caps': allcaps,
        'capitalized': capitalized,

        'prev-all-caps': prevallcaps,
        'prev-capitalized': prevcapitalized,

        'next-all-caps': nextallcaps,
        'next-capitalized': nextcapitalized,
    }

class NamedEntityChunker(ChunkParserI):
    def __init__(self, train_sents, **kwargs):
        assert isinstance(train_sents, Iterable)

        self.feature_detector = features
        self.tagger = ClassifierBasedTagger(
            train=train_sents,
            feature_detector=features,
            **kwargs)

    def parse(self, tagged_sent):
        chunks = self.tagger.tag(tagged_sent)

        # Transform the result from [((w1, t1), iob1), ...]
        # to the preferred list of triplets format [(w1, t1, iob1), ...]
        iob_triplets = [(w, t, c) for ((w, t), c) in chunks]

        # Transform the list of triplets to nltk.Tree format
        return conlltags2tree(iob_triplets)

if os.path.isfile("cached_training.txt"):
    print("loading from cache...")
    with open('cached_training.txt', 'rb') as input:
        chunker = pickle.load(input)
else:
    reader = read_sentences('common-tags.txt')
    training_samples = list(reader)

    reader = read_sentences('bible-tags.txt')
    training_samples += list(reader)

    chunker = NamedEntityChunker(training_samples)

    with open('cached_training.txt', 'wb') as output:
        pickle.dump(chunker, output, pickle.HIGHEST_PROTOCOL)

if False:
    from nltk import pos_tag, word_tokenize
    print(chunker.parse(pos_tag(word_tokenize("Is Jesus Christ coming on monday or just going to New York?"))))
    print(chunker.parse(pos_tag(word_tokenize("Is Jesus coming on monday or just going to New York?"))))
    print(chunker.parse(pos_tag(word_tokenize("The healer and king is Jesus."))))
    print(chunker.parse(pos_tag(word_tokenize("This is a test Brian Manson is doing to see if this works."))))
    print(chunker.parse(pos_tag(word_tokenize("How do I pray?"))))
    print(chunker.parse(pos_tag(word_tokenize("I was travelling to San Diego last week."))))
    print(chunker.parse(pos_tag(word_tokenize("Should Bob forgive Jim?"))))
    print(chunker.parse(pos_tag(word_tokenize("For God so loved the world, that He gave His only begotten Son, that whoever believes in Him shall not perish, but have eternal life."))))
