## %load data/bible+shakes.nopunc.txt

# %load code\MRWordFreqCount.py
from mrjob.job import MRJob
from mrjob.step import MRStep
import pandas as pd
import numpy as np
import re

WORD_RE = re.compile(r"[\w']+")
data=pd.DataFrame({'word':[],'count':[]})



class MRWordTrigramCount(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_max_word)
        ]

    def mapper_get_words(self, _, line):
        firstword=""
        secondword=""
        for word in WORD_RE.findall(line):
            words=[]
            if firstword !="" and secondword !="" and word!="":
                words.append(firstword)
                words.append(secondword)
                words.append(word)
                words=sorted(words)
                yield (words[0].lower(), words[1].lower(), words[2].lower()), 1
                #yield (firstword.lower(), secondword.lower(), word.lower()), 1
            firstword=secondword
            secondword=word
            
    def combiner_count_words(self, word, counts):
        yield word, sum(counts)

    def reducer_count_words(self, word, counts):
        #data.loc[data.shape[0]]=[word,sum(counts)]
        #yield sorted(zip(word, counts), reverse=True)[:3]
        yield None, (sum(counts), word)
        
        
    def reducer_find_max_word(self, _, word_count_pairs):
        yield max(word_count_pairs)


if __name__ == '__main__':
    MRWordTrigramCount.run()