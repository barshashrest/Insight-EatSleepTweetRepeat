# sc is an existing SparkContext.
from pyspark.sql import SQLContext
from pyspark import SparkContext
from cassandra.cluster import Cluster
from pyspark import SparkConf
from cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection
from cqlengine.management import sync_table
import uuid
sc = SparkContext("spark://ip-172-31-4-243:7077", "sparksqlex")
sqlContext = SQLContext(sc)

#defining a simple model
class TweetsTable(Model):
  moviename = columns.Text(primary_key = True)
  ttext = columns.Text() 
  texturl = columns.Text()
  tweetid = columns.Integer()
  ts = columns.TimeUUID()
  def __repr__(self):
   	return '%s %s %s %d' % (self.moviename, self.ttext, self.texturl, self.tweetid, self.ts)
#table for cassandra
class NPmovies(Model):
  movienameNP = columns.Text(primary_key=True)
  releasedateNP = columns.Text()
  popularityNP = columns.Integer()
  voteavgNP = columns.Integer()
  def __repr__(self):
	return '%s %s %d %d' % (self.movienameNP, self.releasedateNP, self.popularityNP, self.voteavgNP)   

# Connect to the demo keyspace on our cluster running at 127.0.0.1
connection.setup(['127.0.0.1'], "movietweets")

#make the table in the space
sync_table(NPmovies)
#CREATE KEYSPACE movietweets
 # WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };
	
#conf = (SparkConf()
 #        .setMaster("local")
  #       .setAppName("My app")
   #      .set("spark.executor.memory", "1g"))

#cassandraContext = CassandraSQLContext(conf)


#session = cluster.connect('example')
#cassandraContext = CassandraSQLContext()

# A JSON dataset is pointed to by path.
# The path can be either a single text file or a directory storing text files.
#path = "/home/ubuntu/JSONfiles/hdfs_movietweetstest4_20150612185132.json"
tweetspath = "hdfs://ec2-52-8-153-198.us-west-1.compute.amazonaws.com:9000/Watching/HadoopCached"
nowplayingpath = "hdfs://ec2-52-8-153-198.us-west-1.compute.amazonaws.com:9000/Watching/TMDB/NP/HadoopCached"
upcomingpath = "hdfs://ec2-52-8-153-198.us-west-1.compute.amazonaws.com:9000/Watching/TMDB/UP/HadoopCached"

# Create a SchemaRDD from the file(s) pointed to by path
people = sqlContext.jsonFile(tweetspath)
nowplaying = sqlContext.jsonFile(nowplayingpath)
upcoming = sqlContext.jsonFile(upcomingpath)
#result = session.execute("select * from people")
#print result.str_id

# The inferred schema can be visualized using the printSchema() method.
#people.printSchema()
#upcoming.printSchema()
#nowplaying.printSchema()

# Register this SchemaRDD as a table.
people.registerTempTable("people")
upcoming.registerTempTable("upcoming")
nowplaying.registerTempTable("nowplaying")

# SQL statements can be run by using the sql methods provided by sqlContext.
tweets = sqlContext.sql("SELECT id, id_str, text, favorite_count, retweet_count, user.followers_count FROM people")

#get the results, popularity and release date from the table


nowplayingmovies = sqlContext.sql("SELECT results.original_title, results.release_date, results.popularity, results.vote_average FROM nowplaying")
#get the results, popularity and release date from the table
#nowplayingresults= sqlContext.sql("SELECT results.original_title from nowplaying")
#collect all the results. BE CAREFUL HERE!!!! SYSTEM MIGHT COLLAPSE
collectionofmovieresults = nowplayingmovies.collect()

for onemovieresult in collectionofmovieresults:
#	print onemovieresult[0]#, onemovieresult[1], onemovieresult[2], onemovieresult[3]
#	stringonemovie = ''.join(onemovieresult[0])
	#NPmovies.create(movienameNP = stringonemovie)
#	for movieuni, releasedate, popularity, voteaverage in onemovieresult.iteritems():
	for i in range(0, len(onemovieresult[0])):
		moviename=onemovieresult[0][i]
		releasedate=onemovieresult[1][i]
		popularity  = onemovieresult[2][i]
		voteavg = onemovieresult[3][i]
		
		NPmovies.create(movienameNP = onemovieresult[0][i], releasedateNP = onemovieresult[1][i])
		TweetsTable.create(movienameNP = "well this one works", ttext = 'we', texturl='w', tweetid=1, ts= str(uuid.uuid1()))
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

	#for voteaverage in onemovieresult[3]:
	#	print voteaverage
	#	NPmovies.create(voteavgNP = voteaverage)






textmapping = tweets.map(lambda p: "Text: " + p.text)

collecttexts = textmapping.collect()

#for texts in collecttexts:
#	TweetsTable.create(movienameNP = "well this one works", ttext = texts, texturl='w', tweetid=1, ts= str(uuid.uuid1()))
#for eachtext in collection:
 # print eachtext.encode('utf-8')

df = tweets
#df.show()

sync_table(TweetsTable)




# Alternatively, a SchemaRDD can be created for a JSON dataset represented by
# an RDD[String] storing one JSON object per string.
#anotherPeopleRDD = sc.parallelize([
 # '{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}'])
#anotherPeople = sqlContext.jsonRDD(anotherPeopleRDD)

#CREATE TABLE retweets( moviename text, userid int, userURL text, RTURL1 text, RTURL2 text, RTURL2id int, PRIMARY KEY (moviename, userid));


