# sc is an existing SparkContext.
from pyspark.sql import SQLContext
from pyspark import SparkContext
sc = SparkContext("spark://ip-172-31-10-125:7077", "sparksqlex")
sqlContext = SQLContext(sc)

# A JSON dataset is pointed to by path.
# The path can be either a single text file or a directory storing text files.
#path = "/home/ubuntu/JSONfiles/hdfs_movietweetstest4_20150612185132.json"
path = "hdfs://ec2-52-8-172-49.us-west-1.compute.amazonaws.com:9000/Watching/HadoopCached"
# Create a SchemaRDD from the file(s) pointed to by path
people = sqlContext.jsonFile(path)

# The inferred schema can be visualized using the printSchema() method.
people.printSchema()
# root
#  |-- age: IntegerType
#  |-- name: StringType

# Register this SchemaRDD as a table.
people.registerTempTable("people")

# SQL statements can be run by using the sql methods provided by sqlContext.
tweets = sqlContext.sql("SELECT text, id_str FROM people")

df = tweets
df.show()
# Alternatively, a SchemaRDD can be created for a JSON dataset represented by
# an RDD[String] storing one JSON object per string.
#anotherPeopleRDD = sc.parallelize([
 # '{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}'])
#anotherPeople = sqlContext.jsonRDD(anotherPeopleRDD)
