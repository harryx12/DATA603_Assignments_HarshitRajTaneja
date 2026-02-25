from mrjob.job import MRJob
from spellchecker import SpellChecker 
import re

WORD_RE = re.compile(r"[\w']+")

class MRNonEnglishCount(MRJob):
    def mapper_init(self):
        self.spell = SpellChecker()

    def mapper(self, _, line):
        words = WORD_RE.findall(line)
        # Filters for words not recognized by the English dictionary
        misspelled = self.spell.unknown(words)
        for word in misspelled:
            yield (word.lower(), 1)

    def reducer(self, word, counts):
        yield (word, sum(counts))

if __name__ == '__main__':
    MRNonEnglishCount.run()
