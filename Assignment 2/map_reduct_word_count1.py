
# Install below, if necessary.

# !pip install pypdf
# !pip install mrjob
# !pip install pyenchant

from mrjob.job import MRJob
from mrjob.step import MRStep

class MRWordCount(MRJob):
    
    def mapper(self, _, line):
        replace_list = ['"','“',  # unwanted punctuation list
                        '”', '“', 
                        '”', '’s', 
                        '!', '?', 
                        '.', ')', 
                        '(', ' ', 
                        '!', ',',
                        ';', "'s"] 
        for word in line.split():
            word = word.replace('\u2014',"") 
            word = word.replace('\u2019',"'") # convert back to an apostrophe
            
            for item in replace_list: # removes punctuation
                word = word.replace(item,'') 
                
            if word == '': # skip blank words
                continue
            
            yield word.lower(), 1

    def reducer(self, word, counts):
        yield word, sum(counts)

if __name__ == '__main__':
    MRWordCount.run()
