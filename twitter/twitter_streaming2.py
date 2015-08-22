#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import re
import json

import argparse

#Variables that contains the user credentials to access Twitter API 

api_key = "H7VlQDgzYjlGZStNd0B7bOF6F"
api_secret = "J9jBnRYYmDDPZ1LyGLTaQ31FqcIbozzL1YKBV8ewJugjGwTjJC"
access_token_key = "1255611608-x2RqeKiTM3dHWTyRPAyVzYhYowkCodBud1Q0l1o"
access_token_secret = "u02RYfMVAy8prhsewrEdU21wtDCK1PC3ZhSIrQ7RJMVwe"


access_token = access_token_key
access_token_secret = access_token_secret
consumer_key = api_key
consumer_secret = api_secret

#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def old_on_data(self, data):
        print data
        return True
    
    def on_data(self, data):
        try:
            one_tw = json.loads(data)
	    if one_tw['lang'] == 'en':
	        print  {'created_at': one_tw['created_at'],
	            'text': one_tw['text'],
	            'screen_name': one_tw['user']['screen_name']
		    }
	except:
		#print data
		pass
	return True
    
    def on_error(self, status):
        print status

def get_args():
    """
    Returns the command line argumenst which allow you to specify the search keyword
    """

    parser = argparse.ArgumentParser(description='Retrieve data from twitter.')
    parser.add_argument('--search_key', type=str, help='comma separated string, the search key.')

    return parser.parse_args()

def main():
    _args = get_args()
    track = _args.search_key
    track = track.split(',')
    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    stream.filter(track = track)

if __name__ == '__main__':
    main()

