from slistener import SListener
import time, tweepy, sys, os

## authentication

consumer_key ="your consumer key"

consumer_secret = "your consumer secret"

access_token = "your access token"

access_token_secret = "your access token secret"

# OAuth process, using the keys and tokens

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)

auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)


def main():
    track = [''] 
    listen = SListener(api, 'tweets')
    stream = tweepy.Stream(auth, listen)

    print "Streaming started, plez filter..."
    

    try: 
        stream.filter( locations=[-180,-90,180,90], languages=['en'])
    

    except:
        print "retrying!"
        time.sleep(90)
	os.system("sudo python /home/ubuntu/InsightEatSleepTweetRepeat/KafkaIngestion/getstreamdata.py")

if __name__ == '__main__':
    main()
