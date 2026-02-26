from mrjob.job import MRJob
from mrjob.step import MRStep
import string

class WordCount(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.map_function,
                   reducer=self.reduce_function)
        ]
    def map_function(self, _, line):
        line = line.translate(str.maketrans('', '', string.punctuation))
        words = line.lower().split()

        for word in words:
            if word.isalpha():
                yield word, 1

    def reduce_function(self, word, counts):
        yield word, sum(counts)

if __name__ == '__main__':
    WordCount.run()
