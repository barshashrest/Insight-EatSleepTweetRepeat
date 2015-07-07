import os
from os import path
import time
import json
import csv
from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer

class ConsumeMovieTweets(object):
	"""Kafka consumer for movie tweets. The functions will consume messages, then feed into HDFS after a batch file created exceed 20MB (even though each batch can put upto 128MB of data)
"""

	def __init__(self, address, group, topic):

		self.client = KafkaClient(address)
		self.consumer = SimpleConsumer (self.client, group, topic, max_buffer_size= 1310720000)
		self.hadoop_path = "/Watching/HadoopCached"
		self.topic = topic
		self.group = group
		self.tempfilepath = None
		self.tempfile=None
		self.blockcount=0
		self.cached_path= "/Watching/HadoopHistory"
		self.csvfile = None
		self.csvfilepath= None

	def consumetopic(self, outputdirectory):

		#timestamp to know when data was consumed by Kafka
		timestamp = time.strftime('%Y%m%d%H%M%S')

		#open file to write, this is temporary file made which is the intermediate which is the batch file that gets flushed to HDFS 
		self.tempfilepath = "%s/kafka_%s_%s_%s.json" % (outputdirectory,
                                                         self.topic,
                                                         self.group,
                                                         timestamp)
        	self.tempfile = open(self.tempfilepath,"w")
		#self.csvfilepath = "%s/kafka_%s_%s_%s.csv" % (outputdirectory,
                             #                            self.topic,
                              #                           self.group,
                               #                          timestamp)
		
		
		while True:	
	
			try:
			# get 1000 tweets at a time to be consumed by Kafka
		    		consumedmessages = self.consumer.get_messages(count=1000, block = False) 	
		        	for consumedmessage in consumedmessages:
					self.tempfile.write(consumedmessage.message.value)
				#print("here at least")
					

				#	filecsv = json.loads(tempfile)
#					print(consumedmessage.message.value)		    
					#print ("here??")
					
		    			if self.tempfile.tell() > 60000000:
						#print os.path.getSize(tempfile)	
						print("sending")
						time.sleep(1)
						print (self.tempfile.tell())
						self.sendtohdfs(outputdirectory)

		    				self.consumer.commit()
						
			except:
		    		self.consumer.seek(0,2)

	def sendtohdfs(self,outputdirectory):
		#self.tempfile = json.loads(self.tempfile)
		#print("loaded tempfile")
		#f = csv.writer(open(self.csvfilepath, "wb+"))
		#print("at sendtohedfs")
		#f.writerow(["id_str","text" ])
		#for x in x:
    		#	f.writerow([x["id_str"], 
                #	x["text"]])

		"""Send 5 KB file to hdfs"""
		self.tempfile.close()
		#f.close()

		#print ("csv file closed")
		timestamp = time.strftime('%Y%m%d%H%M%S')

		hadoop_path = "%s/%s_%s_%s.json" % (self.hadoop_path, self.group,
                                               self.topic, timestamp)	
		cached_path = "%s/%s_%s_%s.json" % (self.cached_path, self.group,
                                               self.topic, timestamp)
		print "Block {}: Flushing 60MB file to HDFS => {}".format(str(self.blockcount),
                                                                  hadoop_path)    
		self.blockcount+=1

		#place blocked messages to history folder
		#os.system("pkexec visudo  hdfs dfs -put %s %s" % (self.tempfilepath,
                 #                                       hadoop_path))
		os.system("sudo -u ubuntu /usr/local/hadoop/bin/hdfs dfs -put %s %s" % (self.tempfilepath,
                                                       hadoop_path))

		os.system("sudo -u ubuntu /usr/local/hadoop/bin/hdfs dfs -put %s %s" % (self.tempfilepath,
                                                       cached_path))
		#remove the temporary file
		os.remove(self.tempfilepath)
		#os.remove(self.csvfilepath)
		timestamp = time.strftime('%Y%m%d%H%M%S')
		self.tempfilepath = "%s/kafka_%s_%s_%s.json" % (outputdirectory,
                                                         self.topic,
                                                         self.group,
                                                         timestamp)	
		self.csvfilepath = "%s/kafka_%s_%s_%s.json" % (outputdirectory,
                                                         self.topic,
                                                         self.group,
                                                         timestamp)
		self.tempfile = open(self.tempfilepath, "w")

if __name__ == '__main__':

	print "Messages are being consumed"
	cons = ConsumeMovieTweets(address="localhost:9092", group="hdfs", topic="movietweetstest4")
    	cons.consumetopic("/home/ubuntu/WhatAreYouWatching/KafkaIntoHdfs")
		
	#/usr/local/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic upcomingmovies

	#/usr/local/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic movietweets
