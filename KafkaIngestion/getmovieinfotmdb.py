import json, time, sys, yaml
import urllib2
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer
from datetime import datetime

class ProducerTMDB():

	def __init__(self, address = '52.8.153.198'):
		self.client = KafkaClient(address)
		self.producer = SimpleProducer(self.client)


	def getnowplayingmoviesTMDB(self):
		
		time.sleep(1)
		request = urllib2.Request('http://api.themoviedb.org/3/movie/now_playing?api_key=696cda7c7d99060a5e8ccfa9a35d3b5d')	
		response = urllib2.urlopen(request)
		nowplayingmovies = response.read()
 		jsonload = json.loads(nowplayingmovies)
		datevalue = datetime.now()
		date = {'date' : str(datevalue)}
		listNP = jsonload["results"]
		listNP.append(date)
		stringlistNP = json.dumps(listNP)
		#jsonload.push(date);
						
			#jsonload = json.loads(nowplayingmovies)
			#jsonloads = yaml.load(nowplayingmovies)
			#results = jsonload['results']
			#print jsonloads	
			#for oneload in results:
				#print type(oneload)
				#print oneload
				#print jsonload['results']		

				
				#print oneload['results'][0]
			#	print oneload
			#	if oneload is None:
			#		print "none"
			#	if oneload['original_title']:
			#		print oneload['original_title']
			#		NPmovies = oneload['original_title']
			#		results = oneload['results']
				#	for key, value in results.iteriterms():
				#		if key == "original_title":
				#			self.producer.send_messages('NPmovies', value)
				#title = item['results'][0]['original_title']				 
		self.producer.send_messages('nowplayingmovies2', stringlistNP)


        #def getupcomingmoviesTMDB(self):
	#	while True:
	#		time.sleep(1)
	#		upcomingrequest = urllib2.Request('http://api.themoviedb.org/3/movie/upcoming?api_key=696cda7c7d99060a5e8ccfa9a35d3b5d')
	#		response = urllib2.urlopen(upcomingrequest)
	#		upcomingmovies = response.read()
	#		self.producer.send_messages('upcomingmovies', upcomingmovies)

def main():
	prod = ProducerTMDB()
		
	prod.getnowplayingmoviesTMDB()
	#prod.getupcomingmoviesTMDB()

if __name__ == "__main__":
	main()
