from slistener import SListener
import time, tweepy, sys, os

## authentication
username = 'barsha05' ## put a valid Twitter username here
password = '' ## put a valid Twitter password here
consumer_key ="1i94gO1Fdm47BL8nnDttB9Nfz"

consumer_secret = "srMkOvjYFDyfqInmJuK5p1p4s3k7MRPJWnNPGAjierbERJ0lnd"

access_token = "52691683-9MfkoxrOR5xIv5gFVjkWGewDYc3PMy6lqlmH7UIEx"

access_token_secret = "uFse01zJv608JeCEFJnSpV0gSqAaghimU0K6P7N9wtmgl"

# OAuth process, using the keys and tokens

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)

auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

def reconnectTwitter(self):
	
		consumer_key ="1i94gO1Fdm47BL8nnDttB9Nfz"

		consumer_secret = "srMkOvjYFDyfqInmJuK5p1p4s3k7MRPJWnNPGAjierbERJ0lnd"

		access_token = "52691683-9MfkoxrOR5xIv5gFVjkWGewDYc3PMy6lqlmH7UIEx"

		access_token_secret = "uFse01zJv608JeCEFJnSpV0gSqAaghimU0K6P7N9wtmgl"

		# OAuth process, using the keys and tokens

		auth = tweepy.OAuthHandler(consumer_key, consumer_secret)

		auth.set_access_token(access_token, access_token_secret)
	
		api = tweepy.API(auth)

		main()

def main():
    track = ['movie']
 
    listen = SListener(api, 'myprefix')
    stream = tweepy.Stream(auth, listen)

    print "Streaming started..."
    

    try: 
        stream.filter(track = track)
    

    except:
        print "retrying!"
        time.sleep(90)
	os.system("sudo python /home/ubuntu/StreamData/getstreamdata.py")

if __name__ == '__main__':
    main()
