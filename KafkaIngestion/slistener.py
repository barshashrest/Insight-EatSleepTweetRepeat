#all definitions from tweepy.org. Modifications made (eg: kafka producer and error handling)

from tweepy import StreamListener
import json, time, sys
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer


class SListener(StreamListener):

    def __init__(self, api = None, fprefix = 'streamer', address = '52.8.153.198'):
        self.api = api or API()
        self.counter = 0
        self.fprefix = fprefix
        self.output  = open(fprefix + '.' 
                            + time.strftime('%Y%m%d-%H%M%S') + '.json', 'w')
        
	
        self.delout  = open('delete.txt', 'a')
	self.client = KafkaClient(address)
        self.producer = SimpleProducer(self.client)

	print "hello"
    def on_data(self, data):
	

	
        	if  'in_reply_to_status' in data:
            		self.on_status(data)
        	elif 'delete' in data:
            		delete = json.loads(data)['delete']['status']
       			if self.on_delete(delete['id'], delete['user_id']) is False:
                		return False
        	elif 'limit' in data:
            		if self.on_limit(json.loads(data)['limit']['track']) is False:
                		return False
        	elif 'warning' in data:
            		warning = json.loads(data)['warnings']
            		print warning['message']
            		return false

 		print data 	
 		
 		#produce messages (i.e. data)
		self.producer.send_messages('movietweetstest4', data)

    def on_status(self, status):

		time.sleep(1)
        	self.output.write(status + "\n")

 		self.counter += 1

  
     		if self.counter >= 1000:
            		self.output.close()
            		self.output = open(self.fprefix + '.' 
                               + time.strftime('%Y%m%d-%H%M%S') + '.json', 'w')
            		self.counter = 0

        	return

    def on_delete(self, status_id, user_id):
        self.delout.write( str(status_id) + "\n")
        return

    def on_limit(self, track):
        sys.stderr.write(str(track) + "\n")
        return

    def on_error(self, status_code):
        sys.stderr.write('Error: ' + str(status_code) + "\n")

	time.sleep(15)
        os.system("sudo python /home/ubuntu/InsightEatSleepTweetRepeat/KafkaIngestion/getstreamdata.py")
        return False

    def on_timeout(self):
        sys.stderr.write("Timeout, sleeping for 60 seconds...\n")
        time.sleep(60)
        return 
