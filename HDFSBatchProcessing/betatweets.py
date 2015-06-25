
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

sc = SparkContext("spark://ip-172-31-4-243:7077", "sparksqlex2")
sqlContext = SQLContext(sc)

class Retweetsbeta(Model):
  moviename = columns.Text(primary_key = True)
  ts = columns.TimeUUID(primary_key= True, clustering_order = "DESC")
  texturl = columns.Text()
  tweettext = columns.Text()
  tweetid = columns.Text()
  retweetcount1 = columns.Text()
  def __repr__(self):
    return '%s %d' % (self.moviename, self.ts, self.texturl, self.tweettext, self.tweetid, self.retweetcount1)

# Connect to the demo keyspace on our cluster running at 127.0.0.1
connection.setup(['127.0.0.1'], "movietweets")

# Sync your model with your cql table
sync_table(Retweetsbeta)

tweetspath = "hdfs://ec2-52-8-153-198.us-west-1.compute.amazonaws.com:9000/Watching/HadoopCached"
nowplayingpath = "hdfs://ec2-52-8-153-198.us-west-1.compute.amazonaws.com:9000/Watching/TMDB/NP/HadoopCached"


people = sqlContext.jsonFile(tweetspath)
nowplaying = sqlContext.jsonFile(nowplayingpath)

# Register this SchemaRDD as a table.
people.registerTempTable("people")
#upcoming.registerTempTable("upcoming")
nowplaying.registerTempTable("nowplaying")

# SQL statements can be run by using the sql methods provided by sqlContext.
tweets = sqlContext.sql("SELECT id, id_str, text, favorite_count, retweet_count, user.followers_count FROM people").collect()
#should have {id, id_str, text, fav count, retweet, followers}

nowplayingmovies = sqlContext.sql("SELECT results.original_title, results.release_date, results.popularity, results.vote_average FROM nowplaying")

collectionofmovieresults = nowplayingmovies.collect()

#textmapping = tweets.map(lambda p:  p.text)
#collecttexts = textmapping.collect()

for onemovieresult in collectionofmovieresults:
        for i in range(0, len(onemovieresult[0])):
                #print ('great')
                movienameupper=onemovieresult[0][i]
                movienamelower = movienameupper.lower()
                print movienameupper

                for texts in tweets.text:
			if ((texts is None) or len(texts)==0):
				print('oh man')
			else:
				if movienameupper in texts or movienamelower in texts:
		                	print('here!!!!!')	
					Retweetsbeta.create(moviename=movienameupper,  ts= str(uuid.uuid1()), texturl=tweets.entities.url, tweettext=tweets.text, tweetid=tweets.id_str, retweetcount1=tweets.retweet_count)
# Create a row of user info for Bob
#Tweets.create(moviename='Bob',  ts= str(uuid.uuid1()), texturl='Austin', tweettext='bob@example.com', tweetid='1')

