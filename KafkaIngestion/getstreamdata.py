from slistener import SListener
import time, tweepy, sys, os

## authentication

consumer_key ="1i94gO1Fdm47BL8nnDttB9Nfz"

consumer_secret = "srMkOvjYFDyfqInmJuK5p1p4s3k7MRPJWnNPGAjierbERJ0lnd"

access_token = "52691683-9MfkoxrOR5xIv5gFVjkWGewDYc3PMy6lqlmH7UIEx"

access_token_secret = "uFse01zJv608JeCEFJnSpV0gSqAaghimU0K6P7N9wtmgl"

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
	os.system("sudo python /home/ubuntu/Project_Insight_EatTweetSleepRepeat/KafkaIngestion/getstreamdata.py")

if __name__ == '__main__':
    main()
