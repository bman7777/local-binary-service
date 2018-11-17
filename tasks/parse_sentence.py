import string
from pattern.en import parsetree

def remove_punctuation(value):
    result = ""
    for c in value:
        # If char is not punctuation, add it to the result.
        if c not in string.punctuation:
            result += c
    return result

def print_parts(sent):
    s = parsetree(sent, relations=True, lemmata=True)
    for sent in s:
        for rel in sent.relations:  # SBJ / VP / OBJ
            print("rel is:"+rel+"  ----  "+str(sent.relations[rel]))
        for pnp in sent.pnp:
            print("pnp is:"+str(pnp))
        print(sent.constituents(pnp=True))

    # for idx, tree in enumerate(parser.parse(word_tokenize(sent))):
    #     newtree = ParentedTree.convert(tree)

    #     for child_tree in newtree.subtrees(filter=lambda x: x.parent()):
    #         if child_tree.parent() == newtree.root():
    #             out_tree = []

    #             for leaf_node in child_tree.pos():
    #                 if leaf_node[1] not in ("DT", "VBZ", "IN", "WP", "WRB", "CC"):
    #                     leaf_word = remove_punctuation(leaf_node[0])
    #                     if len(leaf_word):
    #                         first_letter_pos = leaf_node[1][:1].lower()
    #                         if first_letter_pos in ('v', 'n'):
    #                             lemma = wordnet_lemmatizer.lemmatize(
    #                                             leaf_word, pos=first_letter_pos)
    #                         else:
    #                             lemma = wordnet_lemmatizer.lemmatize(leaf_word)

    #                         out_tree.append(lemma+" ("+leaf_node[1]+")")

    #             if out_tree:
    #                 print(out_tree)


print("=========================================")
print_parts(u"This is a test, and I hope this works.")
print("=========================================")
print_parts(u"I have a car.")
print("=========================================")
print_parts(u"In the beginning, God created the heavens and the earth.")
print("=========================================")
print_parts(u"Brian Manson is the writer of this example.")
print("=========================================")
print_parts(u"What day is it?")
print("=========================================")
print_parts(u"Does God think I am good?")
print("=========================================")
print_parts(u"How many things will fit in this jar?")
print("=========================================")
print_parts(u"I don't know anybody that has walked on the moon.")
print("=========================================")
