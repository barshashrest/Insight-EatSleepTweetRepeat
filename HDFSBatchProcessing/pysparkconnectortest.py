def saveToCassandra():
	from pyspark.sql import SQLContext, Row
	from pyspark import SparkContext
	from cassandra.cluster import Cluster
	from pyspark import SparkConf
	from cqlengine import columns
	from cqlengine.models import Model
	from cqlengine import connection
	from cqlengine.management import sync_table
	#from cqlengine import *
	import datetime
	from datetime import timedelta
	import successcorrelationtable

	sc = SparkContext("spark://ip-172-31-4-243:7077", "sparksqlex2")
	sqlContext = SQLContext(sc)
#sqlContext = HiveContext(sc)
#sqlContext.sql("CREATE TABLE IF NOT EXISTS src (id int, text STRING)")

	class Correlationtable5(Model):
  	  moviename = columns.Text(primary_key = True)
	  release_date = columns.Text()
  	  get_year = columns.Integer(primary_key= True, clustering_order = "DESC")
  	  get_month = columns.Integer(primary_key= True, clustering_order = "DESC")
  	  get_day = columns.Integer(primary_key= True, clustering_order = "DESC")
  	  totaltweets = columns.Integer()
  	  popularity = columns.Integer()
  	  voteaverage = columns.Integer()
  #retweetcount1 = columns.Text()
  	def __repr__(self):
	    	return '%s %d' % (self.moviename, self.release_date, self.get_year, self.get_month, self.get_day, self.totaltweets, self.popularity, self.voteaverage)


	connection.setup(['127.0.0.1'], "movietweets")

	sync_table(correlationtable5)
	for row in successcorrelationtable.final_table:
        	try:
           		correlationtable5.create(moviename=row[1], release_date=row[4], get_year=row[8], get_month=row[9], get_day=row[10], totaltweets=row[2], popularity = row[5], voteaverage=row[7])
        	except: # e.g., if d is not a dictionary
           		pass
	
saveToCassandra()
#rddofdictionaries.foreachPartition(lambda d_iter: saveToCassandra(d_iter)).show()
