import sys
import json

def hw():
    print 'Hello, world!'

def load_afin(file_name):
    scores = {}
    with open(file_name) as afinefile:
        for line in afinefile:
	    term, score = line.split("\t")
	    scores[term] = int(score)
    return scores
     
def lines(fp):
    print str(len(fp.readlines()))

def main():
    sent_file = open(sys.argv[1])
    tweet_file = open(sys.argv[2])
    hw()
    lines(sent_file)
    lines(tweet_file)

if __name__ == '__main__':
    main()
