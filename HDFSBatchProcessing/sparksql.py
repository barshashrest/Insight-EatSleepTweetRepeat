# sc is an existing SparkContext.
from pyspark.sql import SQLContext
from pyspark import SparkContext
from cassandra.cluster import Cluster
from pyspark import SparkConf
from cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection
from cqlengine.management import sync_table

sc = SparkContext("spark://ip-172-31-10-125:7077", "sparksqlex")
sqlContext = SQLContext(sc)

#defining a simple model
class TweetsTable(Model):
  text = columns.Text(primary_key= True)
  tweetid = columns.Integer()
  def __repr__(self):
   	return '%s %d' % (self.tweetID, self.text)

# Connect to the demo keyspace on our cluster running at 127.0.0.1
connection.setup(['127.0.0.1'], "movietweets")

	
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
path = "hdfs://ec2-52-8-172-49.us-west-1.compute.amazonaws.com:9000/Watching/HadoopCached"
# Create a SchemaRDD from the file(s) pointed to by path
people = sqlContext.jsonFile(path)

#result = session.execute("select * from people")
#print result.str_id

# The inferred schema can be visualized using the printSchema() method.
people.printSchema()
# root
#  |-- age: IntegerType
#  |-- name: StringType

# Register this SchemaRDD as a table.
people.registerTempTable("people")

# SQL statements can be run by using the sql methods provided by sqlContext.
tweets = sqlContext.sql("SELECT text, id_str FROM people")
textmapping = tweets.map(lambda p: "Text: " + p.text)
collection = textmapping.collect()

for eachtext in collection:
  print eachtext.encode('utf-8')

df = tweets
#df.show()

sync_table(TweetsTable)
# Alternatively, a SchemaRDD can be created for a JSON dataset represented by
# an RDD[String] storing one JSON object per string.
#anotherPeopleRDD = sc.parallelize([
 # '{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}'])
#anotherPeople = sqlContext.jsonRDD(anotherPeopleRDD)

CREATE TABLE retweets( moviename text, userid int, userURL text, RTURL1 text, RTURL2 text, RTURL2id int, PRIMARY KEY (moviename, userid));


