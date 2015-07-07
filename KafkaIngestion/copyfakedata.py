import os
import time
import glob

class CopyData(object):

	def __init__(self):

		self.hadoop_path = "/Watching/HadoopCached"

	def copydata(self):
		for i in range(0,1):
			path = '/home/ubuntu/InsightEatSleepTweetRepeat/KafkaIngestion/fakedatacollection'

			for filename in glob.glob(os.path.join(path, '*.json')):

				timestamp = time.strftime('%Y%m%d%H%M%S')
                		self.tempfilepath = "/copydata_%s.json" %  (timestamp)
				with open (filename) as f:

					self.tempfile = open(self.tempfilepath, "w")

				
					for line in f:
						self.tempfile.write(line)
						if self.tempfile.tell()>100000000:
							self.sendtohdfs()
					f.close()
	def sendtohdfs(self):


		self.tempfile.close()
		timestamp = time.strftime('%Y%m%d%H%M%S')
		
		print ("Shoving to HDFS")
		hadoop_path = "%s/_copydata27June_%s.json" % (self.hadoop_path,  timestamp)
		os.system("sudo -u ubuntu /usr/local/hadoop/bin/hdfs dfs -put %s %s" % (self.tempfilepath, hadoop_path))
		os.remove(self.tempfilepath)
		self.tempfilepath = "/copydata_%s.json" %  (timestamp)
		self.tempfile = open(self.tempfilepath, "w")


if __name__ == '__main__':
		cd = CopyData()
		cd.copydata() 	
