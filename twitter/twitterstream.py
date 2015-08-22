import oauth2 as oauth
import urllib2 as urllib
import argparse

# See assignment1.html instructions or README for how to get these credentials

api_key = "H7VlQDgzYjlGZStNd0B7bOF6F"
api_secret = "J9jBnRYYmDDPZ1LyGLTaQ31FqcIbozzL1YKBV8ewJugjGwTjJC"
access_token_key = "1255611608-x2RqeKiTM3dHWTyRPAyVzYhYowkCodBud1Q0l1o"
access_token_secret = "u02RYfMVAy8prhsewrEdU21wtDCK1PC3ZhSIrQ7RJMVwe"

_debug = 0

oauth_token    = oauth.Token(key=access_token_key, secret=access_token_secret)
oauth_consumer = oauth.Consumer(key=api_key, secret=api_secret)

signature_method_hmac_sha1 = oauth.SignatureMethod_HMAC_SHA1()

http_method = "GET"


http_handler  = urllib.HTTPHandler(debuglevel=_debug)
https_handler = urllib.HTTPSHandler(debuglevel=_debug)

'''
Construct, sign, and open a twitter request
using the hard-coded credentials above.
'''
def twitterreq(url, method, parameters):
  req = oauth.Request.from_consumer_and_token(oauth_consumer,
                                             token=oauth_token,
                                             http_method=http_method,
                                             http_url=url, 
                                             parameters=parameters)

  req.sign_request(signature_method_hmac_sha1, oauth_consumer, oauth_token)

  headers = req.to_header()

  if http_method == "POST":
    encoded_post_data = req.to_postdata()
  else:
    encoded_post_data = None
    url = req.to_url()

  opener = urllib.OpenerDirector()
  opener.add_handler(http_handler)
  opener.add_handler(https_handler)

  response = opener.open(url, encoded_post_data)

  return response

def fetchsamples():
  url = "https://stream.twitter.com/1/statuses/sample.json"
  parameters = []
  response = twitterreq(url, "GET", parameters)
  for line in response:
    print line.strip()

def fetch_key_tweet(skey):
  url = "https://api.twitter.com/1.1/search/tweets.json?q=" + skey.lower()
  parameters = []
  response = twitterreq(url, "GET", parameters)
  for line in response:
    print line.strip()

def get_args():
    """
    Returns the command line argumenst which allow you to specify the search keyword
    """

    parser = argparse.ArgumentParser(description='Retrieve data from twitter.')
    parser.add_argument('--search_key', type=str, help='a string, the search key.')

    return parser.parse_args()

if __name__ == '__main__':
  _args = get_args()
  if not _args.search_key is None :
      fetchsamples()
  else:
      fetch_key_tweet(_args.search_key) 
