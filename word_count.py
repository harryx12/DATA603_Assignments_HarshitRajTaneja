from mrjob.job import MRJob
import re

# Regex to capture words and ignore punctuation
WORD_RE = re.compile(r"[\w']+")

class MRWordCount(MRJob):
    def mapper(self, _, line):
        # Extracts each word and sends it to the reducer with a count of 1
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1)

    def reducer(self, word, counts):
        # Sums the counts for each unique word
        yield (word, sum(counts))

if __name__ == '__main__':
    MRWordCount.run()
