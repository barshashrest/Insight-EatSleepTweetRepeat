# sc is an existing SparkContext.
from pyspark.sql import SQLContext
from pyspark import SparkContext
from cassandra.cluster import Cluster
from pyspark import SparkConf
from cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection
from cqlengine.management import sync_table
from datetime import datetime
from cqlengine import *
import uuid
#import Timeuuid
sc = SparkContext("spark://ip-172-31-4-243:7077", "sparksqlex")
sqlContext = SQLContext(sc)

#defining a simple model
class TweetsTable(Model):
  moviename= columns.Text(primary_key = True)
  ts = columns.TimeUUID(primary_key = True, clustering_order="DESC")
  texturl = columns.Text()  
  tweettext = columns.Text() 
  #texturl = columns.Text()
  tweetid = columns.Integer()
  #ts = columns.TimeUUID(primary_key = True, clustering_order="DESC")
  def __repr__(self):
   	return '%s %s %s %d' % (self.moviename, self.tweettext, self.texturl, self.tweetid,self.ts)

class TweetsTable2(Model):
  moviename= columns.Text(primary_key = True)
  ts = columns.TimeUUID()
  texturl = columns.Text()
  tweettext = columns.Text()
  #texturl = columns.Text()
  tweetid = columns.Integer()
  #ts = columns.TimeUUID(primary_key = True, clustering_order="DESC")
  def __repr__(self):
        return '%s %s %s %d' % (self.moviename, self.tweettext, self.texturl, self.tweetid,self.ts)


connection.setup(['127.0.0.1'], "movietweets")
sync_table(TweetsTable2)
TweetsTable2.create(moviename = "ww", tweettext = "hdhs", ts= str(uuid.uuid1()), texturl= "whatever", tweetid=1)
#sync_table(TweetsTable2)
#table for cassandra
#class NPmovies(Model):
# Connect to the demo keyspace on our cluster running at 127.0.0.1
#connection.setup(['127.0.0.1'], "movietweets")
#sync_table(TweetsTable)
tweetspath = "hdfs://ec2-52-8-153-198.us-west-1.compute.amazonaws.com:9000/Watching/HadoopCached"
nowplayingpath = "hdfs://ec2-52-8-153-198.us-west-1.compute.amazonaws.com:9000/Watching/TMDB/NP/HadoopCached"
people = sqlContext.jsonFile(tweetspath)
nowplaying = sqlContext.jsonFile(nowplayingpath)
#upcoming = sqlContext.jsonFile(upcomingpath)


# Register this SchemaRDD as a table.
people.registerTempTable("people")
#upcoming.registerTempTable("upcoming")
nowplaying.registerTempTable("nowplaying")

# SQL statements can be run by using the sql methods provided by sqlContext.
tweets = sqlContext.sql("SELECT id, id_str, text, favorite_count, retweet_count, user.followers_count FROM people")

#get the results, popularity and release date from the table
sync_table(TweetsTable)

nowplayingmovies = sqlContext.sql("SELECT results.original_title, results.release_date, results.popularity, results.vote_average FROM nowplaying")
#get the results, popularity and release date from the table
#nowplayingresults= sqlContext.sql("SELECT results.original_title from nowplaying")
#collect all the results. BE CAREFUL HERE!!!! SYSTEM MIGHT COLLAPSE
collectionofmovieresults = nowplayingmovies.collect()

textmapping = tweets.map(lambda p:  ""+p.text)
followersmapping = tweets.map(lambda a: "" + a.user.followers_count) 

collecttexts = textmapping.collect()

TweetsTable.create(moviename = "yoohoo", tweettext = "hdhs", ts= str(uuid.uuid1()), texturl= "whatever", tweetid="1")

for onemovieresult in collectionofmovieresults:
	for i in range(0, len(onemovieresult[0])):
		#print ('great')
		moviename1=onemovieresult[0][i]
		moviename1 = moviename1.lower()
		print moviename1
		for texts in collecttexts:
			print texts.encode('utf-8')
			#encodetext = texts.encode('utf-8')
			if 'a' in texts:
				print('helloooooooooo  sjnfjsnfjs jfkfjsnf')
 #       			TweetsTable.create(moviename = moviename1, tweettext = texts, ts= str(uuid.uuid1()), texturl= "whatever", tweetid="1")		
		
#		NPmovies.create(movienameNP = onemovieresult[0][i], releasedateNP = onemovieresult[1][i])
	#for movieuni in onemovieresult[0]:
#		print movieuni
	#	string onemovie = ''.join(movieuni)
#		NPmovies.create(movienameNP = movieuni, releasedateNP = releasedate)
	#	NPmovies.create(movienameNP = movieuni)
	#for releasedate in onemovieresult[1]:
	#	print releasedate
	#	NPmovies.update(releasedateNP = releasedate)

	#for popularity in onemovieresult[2]:
		
	#	print popularity
	#	NPmovies.create(popularityNP = popularity)

#textmapping = tweets.map(lambda p: "Text: " + p.text)

#collecttexts = textmapping.collect()

#for texts in collecttexts:
 #       TweetsTable.create(text = texts)
	#for voteaverage in onemovieresult[3]:
	#	print voteaverage
	#	NPmovies.create(voteavgNP = voteaverage)






#textmapping = tweets.map(lambda p: "Text: " + p.text)

#collecttexts = textmapping.collect()

#for texts in collecttexts:
#	TweetsTable.create(text = texts)
#for eachtext in collection:
 # print eachtext.encode('utf-8')

#df = tweets
#df.show()

#sync_table(TweetsTable)




# Alternatively, a SchemaRDD can be created for a JSON dataset represented by
# an RDD[String] storing one JSON object per string.
#anotherPeopleRDD = sc.parallelize([
 # '{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}'])
#anotherPeople = sqlContext.jsonRDD(anotherPeopleRDD)

#CREATE TABLE retweets( moviename text, userid int, userURL text, RTURL1 text, RTURL2 text, RTURL2id int, PRIMARY KEY (moviename, userid));


