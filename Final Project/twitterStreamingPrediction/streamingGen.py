
import socket
import sys
import requests
import requests_oauthlib
import json

# Replace the values below with yours
ACCESS_TOKEN = "886704367-uvfXUtiTqECbwylLoYDX2gSxtPjShim3rumBeIK6"
ACCESS_SECRET = "FVELnnYT8C4Ot8aSXSV3VDBRWdcLRrW1bGrYIIJRSESja"
CONSUMER_KEY = "hgvcc3izw2Bxxeqf70wLHS2zh"
CONSUMER_SECRET = "tvcBrASL39QVPHI0emiJM8kYKOjvKnRzrBXgYYNanrwQxpj8bk"
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)

def get_tweets():
    # Query formation
	url = 'https://stream.twitter.com/1.1/statuses/filter.json'
	query_data = [('language', 'en'), ('locations', '-130,-20,100,50'),('track','#')]
	query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    # Query request as a stream of tweets
	response = requests.get(query_url, auth=my_auth, stream=True)
	print(query_url, response)
	return response

def parseSend(http_resp, tcp_connection):
  for line in http_resp.iter_lines():
    try:
        # Parse JSON response from Twitter API
        tweet = json.loads(line)
        # Extract Text, Username and Name
        tweet_text = tweet['text']
        tweet_user = tweet['user']['screen_name']
        tweet_name = tweet['user']['name']
        print("Tweet Text: " + tweet_text)
        print("Tweet Username: " + tweet_user)
        print("Tweet Name: " + tweet_name)
        print ("------------------------------------------")
        # Add line break and send to socket
        text = tweet_text + '\n'
        tcp_connection.send(text.encode('utf-8'))
    except:
        e = sys.exc_info()
        print(e)

# Socket Setup
TCP_IP = "localhost"
TCP_PORT = 9009
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
# Listening if anyone connected
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected...Starting getting tweets.")
resp = get_tweets()
parseSend(resp, conn)