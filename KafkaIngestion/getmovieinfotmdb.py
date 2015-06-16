import json, time, sys
import urllib2
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer

class ProducerTMDB():

	def __init__(self, address = '52.8.172.49'):
		self.client = KafkaClient(address)
		self.producer = SimpleProducer(self.client)


	def getnowplayingmoviesTMDB(self):
		while True:
			time.sleep(1)
			request = urllib2.Request('http://api.themoviedb.org/3/movie/now_playing?api_key=696cda7c7d99060a5e8ccfa9a35d3b5d')	
			response = urllib2.urlopen(request)
			nowplayingmovies = response.read()
			self.producer.send_messages('nowplayingmovies', nowplayingmovies)


	def getupcomingmoviesTMDB(self):
		while True:
			time.sleep(1)
			upcomingrequest = urllib2.Request('http://api.themoviedb.org/3/movie/upcoming?api_key=696cda7c7d99060a5e8ccfa9a35d3b5d')
			response = urllib2.urlopen(upcomingrequest)
			upcomingmovies = response.read()
			self.producer.send_messages('upcomingmovies', upcomingmovies)

def main():
	prod = ProducerTMDB()
		
	prod.getnowplayingmoviesTMDB()
	prod.getupcomingmoviesTMDB()

if __name__ == "__main__":
	main()
