from pyspark.sql import SQLContext, Row
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
import pytz
import datetime
from datetime import timedelta 
import time

sc = SparkContext("spark://ip-172-31-4-243:7077", "sparksqlex2")
sqlContext = SQLContext(sc)



# Connect to the demo keyspace on our cluster running at 127.0.0.1
connection.setup(['127.0.0.1'], "movietweets")


#get data from hdfs
tweetspath = "hdfs://ec2-52-8-153-198.us-west-1.compute.amazonaws.com:9000/Watching/HadoopCached"
nowplayingpath = "hdfs://ec2-52-8-153-198.us-west-1.compute.amazonaws.com:9000/Watching/TMDB/NP/HadoopCached"


people = sqlContext.jsonFile(tweetspath)
nowplaying = sqlContext.jsonFile(nowplayingpath)

# Register this SchemaRDD as a table.
people.registerTempTable("people")

nowplaying.registerTempTable("nowplaying")

# SQL statements can be run by using the sql methods provided by sqlContext.
tweets = sqlContext.sql("SELECT created_at, id, id_str, text, favorite_count, retweet_count, user.followers_count FROM people" )#.collect()
#should have {id, id_str, text, fav count, retweet, followers}

nowplayingmovies = sqlContext.sql("SELECT original_title, release_date, popularity, vote_average, date FROM nowplaying")
nowplayingmovies.registerTempTable("nowplayingmovies")

#get date for the API. Date has been added into the JSON API of TMDB manually and needs to be extracted.
dateRDD = sqlContext.sql("select date from nowplayingmovies where date!='null'")
datelist = dateRDD.collect()
dateonly=''
year=0
month=0
day=0
for d in datelist:
	dateonly = d[0]
	dateonly = dateonly.encode('ascii','ignore')
	dateonly = dateonly[0:10]
	year = int(dateonly[0:4])
	month=int(dateonly[5:7])
	day = int(dateonly[8:10])
	



#get all movies in the list
movienames = nowplayingmovies.map(lambda p: p.original_title)


#collect all the movie names
movie_list = movienames.collect()

#add the date column into the TMDB database table
def add_date(row):
		date_added = dateonly
		year_added = year
		month_added=month
		day_added = day
		return(row.asDict()["original_title"], row.asDict()["release_date"], row.asDict()["popularity"], row.asDict()["vote_average"], date_added, year_added, month_added, day_added )

#final RDD with now playing movies and date
nowplaying_with_date = nowplayingmovies.map(add_date).coalesce(40)

#dataframe for nowplaying and upcoming movies
NPdf = sqlContext.createDataFrame(nowplaying_with_date, ['original_title', 'release_date', 'popularity', 'voteaverage', 'date_added', 'year_added', 'month_added', 'day_added'])

#add movie name as column to the tweets database so as to query for only the tweets with the movie name
def add_movie_col(row):
	movie_name='null'
	for movie in movie_list:
		if movie is not None:
			if row.asDict()["text"] is not None:
				if movie in row.asDict()["text"]:
					movie_name=movie
					break		
			else:
				row.asDict()["text"] = ''
	return(row.asDict()["created_at"], row.asDict()["text"], movie_name)

#add the date to the rdd of tweets	
for movie in movie_list:
	tweet_with_movie_rdd = tweets.map(add_movie_col).coalesce(40)
	

#create dataframe with the following columns
tweet_with_movie_df = sqlContext.createDataFrame(tweet_with_movie_rdd, ['created_at', 'text', 'movie_name'])         

# filter from table such that all the remaining tweets are tweets explicity about the movies in the movielist 
tweet_with_movie_df= tweet_with_movie_df.filter(tweet_with_movie_df['movie_name']!='null')

tweet_with_movie_df.registerTempTable("tweetandmovie")



#add date to the tweets based on when the tweet was generated. This is extracted from the 'created_at' column of the tweets
def add_date_of_tweet(row):
	dateonly = 'null'
	if row.asDict()["created_at"] is not None:
		dateonly = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(row.asDict()["created_at"],'%a %b %d %H:%M:%S +0000 %Y'))
		dateonly= dateonly[0:10]
	return(row.asDict()["created_at"], row.asDict()["text"], row.asDict()["movie_name"], dateonly)

#rdd of my tweet query
tweet_with_movie_and_date_rdd = tweet_with_movie_df.map(lambda row: add_date_of_tweet(row)).coalesce(40)

#dataframe of tweet query
tweetmoviedatedf=  sqlContext.createDataFrame(tweet_with_movie_and_date_rdd,['created_at', 'text', 'movie_name', 'dateonly'])

#table with 'created_at', 'text' 'moviename' and 'dateonly' from tweets
tweetmoviedatedf.registerTempTable("tweetmoviedate")


maintweettable = tweetmoviedatedf.groupBy('dateonly','movie_name').count()
maintweettable.registerTempTable("regtweet")
NPdf.registerTempTable("regNPdf")

#final table where tweets table was joined with TMDB table where moviename and data were matched
final= sqlContext.sql("""SELECT * FROM regtweet JOIN regNPdf ON regtweet.movie_name = regNPdf.original_title AND regtweet.dateonly=regNPdf.date_added""")
final.registerTempTable("final_table")
final_collect = final.collect()


final.printSchema()

#create the cassandra table
class correlationtable7(Model):
           moviename = columns.Text(primary_key = True)
           release_date = columns.Text()
           year = columns.Integer(primary_key= True, clustering_order = "ASC")
           month = columns.Integer(primary_key= True, clustering_order = "ASC")
           day = columns.Integer(primary_key= True, clustering_order = "ASC")
           totaltweets = columns.Integer()
           popularity = columns.Integer()
           voteaverage = columns.Integer()
           votecount = columns.Integer()
  
           def __repr__(self):
                 return '%s %s %d %d %d %d %d %d' % (self.moviename, self.release_date, self.year, self.month, self.day, self.totaltweets, self.popularity, self.voteaverage, self.votecount) 
 
connection.setup(['127.0.0.1'], "movietweets")
sync_table(correlationtable7)

#put all rows from final_collect to cassandra 
for row in final_collect:
         
	correlationtable7.create(moviename=row[1].encode('ascii','ignore'), release_date=row[4].encode('ascii','ignore'), year=row[8], month=row[9], day=row[10], totaltweets=row[2], popularity = row[5], voteaverage=row[6], votecount=row[11])
