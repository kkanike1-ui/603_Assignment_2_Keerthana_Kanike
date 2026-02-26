from mrjob.job import MRJob
from mrjob.step import MRStep
from spellchecker import SpellChecker
import string


class NonEnglish(MRJob):
    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.map_function,
                   reducer=self.reduce_function)
        ]
    def mapper_init(self):
        self.spell = SpellChecker()

    def map_function(self, _, line):
        line = line.translate(str.maketrans('', '', string.punctuation))
        words = line.lower().split()

        for word in words:
            if word.isalpha() and word not in self.spell:
                yield word, 1

    def reduce_function(self, word, counts):
        yield word, sum(counts)

if __name__ == '__main__':
    NonEnglish.run()
