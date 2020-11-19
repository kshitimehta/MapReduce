import io
import string
from pyspark import SparkContext, SparkConf
import os




if __name__ == "__main__":

	print("Setting the HADOOP_HOME environment variable.... ")
	os.environ['HADOOP_HOME'] = "C:\\winutils"
	print(os.environ['HADOOP_HOME'])
	print("Running Word Count Test Application on PySpark.... ")
	conf = SparkConf().setAppName("Spark Word Count")
	sc = SparkContext(conf=conf)
  
	print("Reading the inputfile.....")
	LineData = sc.textFile("..\\inputfile2.txt").flatMap(lambda line: [i for i in line.split(" ") if i!=""])
	print("Spark running map and reduce...")
	wordFrequencies = LineData.map(lambda word: (''.join([i for i in word.lower() if(i.isalnum() or i==" ")]) , 1)).reduceByKey(lambda v1,v2:v1 +v2)
	res = wordFrequencies.collect()
  	
	print("Writing the spark output to file....")
	with io.open("..\\outputspark_WordCount.txt",'w',encoding="utf-8") as f:
		f.write(str(res))
	print("################################## Word Count Test Completed. ##################################")

  
