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
#sqlContext = HiveContext(sc)
#sqlContext.sql("CREATE TABLE IF NOT EXISTS src (id int, text STRING)")

class Correlationtable4(Model):
  moviename = columns.Text(primary_key = True)
  year = columns.Integer(primary_key= True, clustering_order = "DESC")
  month = columns.Integer(primary_key= True, clustering_order = "DESC")
  day = columns.Integer(primary_key= True, clustering_order = "DESC")	
  totaltweets = columns.Integer()
  totalretweets1 = columns.Integer()
  popularity = columns.Integer()
  voteaverage = columns.Integer()
  #retweetcount1 = columns.Text()
  def __repr__(self):
    return '%s %d' % (self.moviename, self.ts, self.totaltweets, self.totalretweets1, self.popularity, self.voteaverage)

# Connect to the demo keyspace on our cluster running at 127.0.0.1
connection.setup(['127.0.0.1'], "movietweets")

# Sync your model with your cql table
sync_table(Correlationtable4)

tweetspath = "hdfs://ec2-52-8-153-198.us-west-1.compute.amazonaws.com:9000/Watching/HadoopCached"
nowplayingpath = "hdfs://ec2-52-8-153-198.us-west-1.compute.amazonaws.com:9000/Watching/TMDB/NP/HadoopCached"

#hdfs_nowplayingmovies_20150617094655_moviesNP.json
people = sqlContext.jsonFile(tweetspath)
nowplaying = sqlContext.jsonFile(nowplayingpath)

# Register this SchemaRDD as a table.
people.registerTempTable("people")
#upcoming.registerTempTable("upcoming")
nowplaying.registerTempTable("nowplaying")

# SQL statements can be run by using the sql methods provided by sqlContext.
tweets = sqlContext.sql("SELECT created_at, id, id_str, text, favorite_count, retweet_count, user.followers_count FROM people" )#.collect()
#should have {id, id_str, text, fav count, retweet, followers}

nowplayingmovies = sqlContext.sql("SELECT original_title, release_date, popularity, vote_average, date FROM nowplaying")
nowplayingmovies.registerTempTable("nowplayingmovies")

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
	
#collectionofmovieresults = nowplayingmovies.collect()



movienames = nowplayingmovies.map(lambda p: p.original_title)
#tweets.groupBy("created_at").count().show()
#tag

#movie_list = movienames.collect()
movie_list = movienames.collect()
def add_date(row):
		date_added = dateonly
		year_added = year
		month_added=month
		day_added = day
		return(row.asDict()["original_title"], row.asDict()["release_date"], row.asDict()["popularity"], row.asDict()["vote_average"], date_added, year_added, month_added, day_added )

nowplaying_with_date = nowplayingmovies.map(add_date).coalesce(40)

NPdf = sqlContext.createDataFrame(nowplaying_with_date, ['original_title', 'release_date', 'popularity', 'voteaverage', 'date_added', 'year_added', 'month_added', 'day_added'])

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

	
#for x in range(0, len(movie_list)):
#	eachmovie =  movie_list[x]
#	var1 = eachmovie
#	var = '"%'+eachmovie + '%"'
 #       tweets = sqlContext.sql("SELECT created_at, id, id_str, text, favorite_count, retweet_count, user.followers_count FROM people where text LIKE " + var )
for movie in movie_list:
	tweet_with_movie_rdd = tweets.map(add_movie_col).coalesce(40)
	
#tweet_with_movie_and_date_rdd = tweets.map(add_movie_col)

tweet_with_movie_df = sqlContext.createDataFrame(tweet_with_movie_rdd, ['created_at', 'text', 'movie_name'])         
#tweet_with_movie_df.filter(tweet_with_movie_df['movie_name']!='Dope').take(10)

tweet_with_movie_df= tweet_with_movie_df.filter(tweet_with_movie_df['movie_name']!='null')

tweet_with_movie_df.registerTempTable("tweetandmovie")




def add_date_of_tweet(row):
	dateonly = 'null'
	if row.asDict()["created_at"] is not None:
		dateonly = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(row.asDict()["created_at"],'%a %b %d %H:%M:%S +0000 %Y'))
		dateonly= dateonly[0:10]
	return(row.asDict()["created_at"], row.asDict()["text"], row.asDict()["movie_name"], dateonly)


tweet_with_movie_and_date_rdd = tweet_with_movie_df.map(lambda row: add_date_of_tweet(row)).coalesce(40)
tweetmoviedatedf=  sqlContext.createDataFrame(tweet_with_movie_and_date_rdd,['created_at', 'text', 'movie_name', 'dateonly'])
tweetmoviedatedf.registerTempTable("tweetmoviedate")

maintweettable = tweetmoviedatedf.groupBy('dateonly','movie_name').count()
maintweettable.registerTempTable("regtweet")
NPdf.registerTempTable("regNPdf")


final= sqlContext.sql("""SELECT * FROM regtweet JOIN regNPdf ON regtweet.movie_name = regNPdf.original_title AND regtweet.dateonly=regNPdf.date_added""")
final.registerTempTable("final_table")
final_collect = final.collect()
#final_table = sqlContext.sql("""SELECT * FROM regtweet JOIN regNPdf ON regtweet.movie_name = regNPdf.original_title AND regtweet.dateonly=regNPdf.date_added""").show()
#final_table.registerTempTable("final_table")

print (type(final))
final.printSchema()
#final_table.show()

class correlationtable5(Model):
           moviename = columns.Text(primary_key = True)
           release_date = columns.Text()
           year = columns.Integer(primary_key= True, clustering_order = "DESC")
           month = columns.Integer(primary_key= True, clustering_order = "DESC")
           day = columns.Integer(primary_key= True, clustering_order = "DESC")
           totaltweets = columns.Integer()
           popularity = columns.Integer()
           voteaverage = columns.Integer()
   #retweetcount1 = columns.Text()
           def __repr__(self):
                 return '%s %s %d %d %d %d %d %d' % (self.moviename, self.release_date, self.year, self.month, self.day, self.totaltweets, self.popularity, self.voteaverage)
 
 
connection.setup(['127.0.0.1'], "movietweets")
sync_table(correlationtable5)
for row in final_collect:
        #print (row[0]) 
     	correlationtable5.create(moviename=row[1].encode('ascii','ignore'), release_date=row[4].encode('ascii','ignore'), year=row[8], month=row[9], day=row[10], totaltweets=row[2], popularity = row[5], voteaverage=row[6])
    	print row[0] 
#maintweettableRDD = sqlContext.sql("SELECT * from maintweettable")
#NPdfRDD = sqlContext.sql("SELECT * from NPdf")

#test_final = maintweettableRDD.join(NPdfRDD, maintweettableRDD('movie_name')==NPdfRDD('original_title') and maintweettableRDD('dateonly')==NPdfRDD('date_added'))

#final_table = maintweettable.join(NPdf, maintweettable.movie_name==NPdf.original_title and maintweettable.dateonly==NPdf.date_added)
#final_table.registerTempTable("final_table")
final.take(1)
print("why")


#td = sqlContext.sql("select * from maintweettable");

	#add movie title  and date
#This works , DO NOT CHANGE
#tweet_with_movie_df.filter(tweet_with_movie_df['movie_name']!='Dope').take(10)

#tweet_with_movie_df= tweet_with_movie_df.filter(tweet_with_movie_df['movie_name']!='null')

#tweet_with_movie_df.registerTempTable("tweetandmovie")		
#eg1= sqlContext.sql("select * from tweetmoviedatedf where dateonly == '2015-06-23'")
#eg1.show()

#eg2=sqlContext.sql("select * from NPdf where date_added == '2015-06-23'")
#eg2.show()

#tweet_with_movie_df.select('movie_name' ).show()
#tweet_with_movie_df.groupBy('movie_name').count().show()
#tweet_with_movie_df.groupBy("movie_name").count().show()


#maintable = tweetmoviedatedf.join(NPdf, tweetmoviedatedf.movie_name==NPdf.original_title and tweetmoviedatedf.dateonly==NPdf.date_added, "left_outer")

#maintable.registerTempTable("maintable")




#ex = sqlContext.sql("select * from maintable where popularity!='null'")
#ex.show()
#ex.printSchema()

#ex2 = maintable.filter(maintable['dateonly']=='2015-06-23')
#ex2.show()
#print(ex2.take(1))
#print(tweetmoviedatedf.take(500))

#maintable_grouped_by_movie = maintable.groupBy("original_title").count().show()

#main_table_collect = sqlContext.sql("SELECT original_title, popularity, voteaverage, release_date, date_added from table")

#main_table_collect.show()
# COUNT(*) AS cnt FROM maintable WHERE original_title <> '' GROUP BY date_added ORDER BY cnt DESC LIMIT 10").collect()

#for collection in main_table_collect.collect():
#	print collection


#print(maintable_grouped_by_movie.take(100))
#print(tweetmoviedatedf.take(1000))
#print(tweetmoviedatedf.take(5))
#print(NPdf.take(5))
#ex3 = maintable.filter(maintable['popularity']!='null')
#ex3.show()
#eg1= tweetmoviedatedf.filter(tweetmoviedatedf["dateonly"]=='2015-06-23')
#eg1.show()

#eg2= NPdf.filter(NPdf["date_added"]=='2015-06-23')
#eg2.show()
#for moviename in movie_list():
#	for x in range(0, len(moviename))n
	#for i in range(0, len(moviename)):
#		eachmovie =  moviename[0][x]
#		
#		var = '"%'+eachmovie + '%"'
#		tweets = sqlContext.sql("SELECT created_at, id, id_str, text, favorite_count, retweet_count, user.followers_count FROM people where text LIKE " + var )
#tweet_with_movie_rdd = tweets.map(add_movie_col)
#		time.sleep(0.05)
#		print tweets
#	for tweet in tweets:tweet_with_movie_rdd = tweets.map(add_movie_col)
#		if moviename in tweet[3]:
 #   iscript type="text/javascript" src="/js/themes/gray.js"></script>
			


#textmapping = tweets.map(lambda p:  p.text)
#collecttexts = textmapping.collect()

#tweets.show()

#movie_only_table = tweets.map(lambda a: a.text)
#for movie_only


#movie_only_table.show()

##j=0
#
#for onemovieresult in collectionofmovieresults:
# 	
#         for i in range(0, len(onemovieresult[j])):	
# 		totalretweets = 0
#                 #print ('great')
#                movienameupper=onemovieresult[j][i]
#                movienamelower = movienameupper.lower()
##                 #print movienameupper
# 		counttweets=0
# 		countretweets = 0
# 		todaydate = '2015-06-17'
# 		xincr = '2015'
# 		yincr = '06'
# 		zincr = 17
# 		for i in range (0,3):
# 			counttweets=0
## 			#print(todaydate)
# 	                for tweet in tweets:
# 				if tweet[0] is None:
# 					print('')
# 				else:		
## 
## 					#get date
# 					d = datetime.datetime.strptime(tweet[0], '%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=pytz.UTC)
# 					dateonly = d.strftime('%Y-%m-%d')	
# 					#print (dateonly)
# 					yearint = int(dateonly[0:4])
# 					monthint = int(dateonly[5:7])
# 					dayint = int(dateonly[8:10])
# 					#if datetime.datetime(yearint, monthint, dayint)  > datetime.datetime(xincr, yincr, zincr):
# 					#	getnextdate = datetime.datetime.now() + datetime.timedelta(days=1)
# 					#	todaydate = getnextdate.strftime("%Y-%m-%d")
# 					#	xincr = todaydate[0:4]
# 					#	yincr = todaydate[5:7]
# 					#	zincr = todaydate[8:10]	
# 				#		break;
# 					#todaydate = '2015-06-18'
# 					# tweet[3] holds the text as described from the table above
# 					if ((tweet[3] is None) or len(tweet[3])==0):
# 						print('')
# 					else:  
## 						
# 						if movienameupper in tweet[3] or movienamelower in tweet[3]:
# 							print ('')
## 							#print (todaydate + "and "+dateonly)
# 						
# 						#	print (dateonly == todaydate)
# 		 					if dateonly == todaydate:
# 			               		#		print('movie is there for '+ todaydate+ '!!!!!')
# 								counttweets +=1	
# 								totalretweets+= tweet[5]
# 			#if dateonly has surpassed todaydatei  
# #			break
## 								print (tweet[5])
# 					Correlationtable4.create(moviename = movienameupper, year = yearint, month=monthint, day=zincr, totaltweets = counttweets, totalretweets1 = totalretweets, popularity = int(onemovieresult[2][i]), voteaverage = int(onemovieresult[3][i]))		
# 			zincr+=1
## 			
# 			todaydate = '2015-06-' + str(zincr)
#			#Retweetsbeta.create(moviename=movienameupper,  ts= str(uuid.uuid1()), texturl=tweets.entities.url, tweettext=tweets.text, tweetid=tweets.id_str, retweetcount1=tweets.retweet_count)
## Create a row of user info for Bob
##Tweets.create(moviename='Bob',  ts= str(uuid.uuid1()), texturl='Austin', tweettext='bob@example.com', tweetid='1')
#
