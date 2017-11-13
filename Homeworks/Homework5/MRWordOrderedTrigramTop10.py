## %load data/bible+shakes.nopunc.txt

# %load code\MRWordFreqCount.py
from mrjob.job import MRJob
from mrjob.step import MRStep
import pandas as pd
import numpy as np
import re

from stemming.porter2 import stem

WORD_RE = re.compile(r"[\w']+")

class MRWordOrderedTrigramTop10(MRJob):
    def steps(self):
            return [
                MRStep(mapper=self.mapper,
                    reducer=self.reducer),
                MRStep(reducer = self.secondreducer)
                ]

    def mapper(self,_,line):
        firstword=""
        secondword=""
        for word in WORD_RE.findall(line):
            words=[]
            if firstword !="" and secondword !="" and word!="":
                words.append(firstword)
                words.append(secondword)
                words.append(word)
                words=sorted(words)
                newWord=words[0].lower(),words[1].lower(), words[2].lower()
                yield (newWord), 1
                #yield (firstword.lower(), secondword.lower(), word.lower()), 1
            firstword=secondword
            secondword=word
        
    def reducer(self, key, values): 
        yield None, ('%08d'%int(sum(values)),key)

    def secondreducer(self, key, values):   
        self.aList= []    
        for v in values:
            self.aList.append(v)
        count = len(self.aList)
        for m in range(count-10,count):
            yield self.aList[m]


if __name__ == '__main__':
    MRWordOrderedTrigramTop10.run()