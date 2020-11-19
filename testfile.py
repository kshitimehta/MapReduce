import os
import time
import glob
from threading import Thread
from killProcesses import kill_Proc

# changing the main directory to src (source)
os.chdir("src")
print("Compiling files")
f = os.listdir("mapReduce")
for x in f:
	if '.java' in x:
		os.system("javac mapReduce\\"+x)

print("Files Compiled")
print("Creating jar")
os.system("jar cvfe Main.jar Main mapReduce\\*.class")
print("jar created")
print(" Executing jar")
# Running the map reduce code and tests for 3 applications
print("Starting execution for  3 applications....")
for i in range(1,4):
	Thread(target = kill_Proc).start()
	num = str(i)
	# chaning the config file for the corresponding application
	config_file = open("..\\app.config","w")
	content = "app.inputfilename=..\\\\inputfile"+num+".txt\n"+\
	"app.outputfilename=..\\\\outputfile"+num+"\\\\\n"+\
	"app.N=3\n"+\
	"app.class_name=UDF"+num
	config_file.write(content)
	config_file.close()
	# os.chdir("src")
	print("Running Application "+num)
	
	#os.system("pwd")
	Thread(target = os.system("java mapReduce.Main")).start()
	
	time.sleep(10)
	#os.chdir(os.path.pardir)

	# Deleting intermediate files after every map reduce execution for an application
	print("Deleting the intermediate files on disk...")
	files = glob.glob('tmp\\*.txt', recursive = True)

	for f in files:
	    try:
	        os.remove(f)
	    except OSError as e:
	        print("Error: %s : %s" % (f, e.strerror))

print("######################## Map Reduce Completed for all Applications ####################")

# Running time series on python
os.system("py ..\\sensors_spark.py")
# Running inverted index on python
os.system("py ..\\invertedIndexSpark.py")
# Running word count on pyspark
os.system("py ..\\sparktest.py")
# Testing all applications with the python outputs
print("Executing Automated Testing.....")
os.system("py ..\\comparison.py")
print("##########################################################################")