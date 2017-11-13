## %load data/bible+shakes.nopunc.txt

# %load code\MRWordFreqCount.py
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import pandas as pd
import numpy as np

WORD_RE = re.compile(r"[\w']+")


class MRWordOrderedBigramCount(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_max_word)
        ]


    def mapper_get_words(self, _, line):
        lastword=""
        for word in WORD_RE.findall(line):
            words=[]
            if lastword !="" and word !="":
                words.append(lastword)
                words.append(word)
                words=sorted(words)
                yield (words[0].lower(), words[1].lower()), 1
            lastword=word
            
    def combiner_count_words(self, word, counts):
        yield (word, sum(counts))

    def reducer_count_words(self, word, counts):
        # send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is so we can easily use Python's max() function.
        #data=pd.DataFrame({'word':word,'count':sum(counts)})
        yield None, (sum(counts), word)
    
    # discard the key; it is just None
    def reducer_find_max_word(self, _, word_count_pairs):
        # each item of word_count_pairs is (count, word),
        # so yielding one results in key=counts, value=word
        yield max(word_count_pairs)


if __name__ == '__main__':
    MRWordOrderedBigramCount.run()