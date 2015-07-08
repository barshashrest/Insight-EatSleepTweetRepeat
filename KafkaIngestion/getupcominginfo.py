#!/usr/bin/env python

import json, time, sys, yaml
import urllib2
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer
from datetime import datetime

class ProducerTMDB():

	def __init__(self, address = '52.8.153.198'):
		self.client = KafkaClient(address)
		self.producer = SimpleProducer(self.client)


	def getupcomingmoviesTMDB(self):
		
		time.sleep(1)
		request = urllib2.Request('http://api.themoviedb.org/3/movie/upcoming?api_key=696cda7c7d99060a5e8ccfa9a35d3b5d')	
		response = urllib2.urlopen(request)
		nowplayingmovies = response.read()
 		jsonload = json.loads(nowplayingmovies)
		datevalue = datetime.now()
		date = {'date' : str(datevalue)}
		listNP = jsonload["results"]
		listNP.append(date)
		stringlistNP = json.dumps(listNP)
		self.producer.send_messages('nowplayingmovies2', stringlistNP)



def main():
	prod = ProducerTMDB()
		
	prod.getupcomingmoviesTMDB()

if __name__ == "__main__":
	main()
