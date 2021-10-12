from mrjob.job import MRJob
from mrjob.job import MRStep
import re

WORD_RE=re.compile(r"[\w']+")


class MRWordFrequencyCount(MRJob):

# key 는 쓰지 않을것이라 _ , line을 value로 받을것 
    def mapper(self, _,line):
        for word in WORD_RE.findall(line):
            yield (word.lower(),1)       # 앞의 것이 key, 뒤의 것이 value  각 단어별로 1을 부여한다. 

    def combiner(self,word,counts):      # mapper안에서 local 에서 들어온 것들을 한번 합쳐주는용도 
        yield (word,sum(counts))        

    def reducer(self,word,counts):       # reducer에서 들어온 key들을 모두 합치는 것 
        yield(word,sum(counts))    


if __name__ == '__main__':               # 드라이버 생성 -> 실행하게 해줌 
    MRWordFrequencyCount.run()
