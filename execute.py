import os
import time
import glob
os.chdir("src")
print("Running Standalone Application")
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
#os.system("pwd")
os.system("java mapReduce.Main")
time.sleep(10)
#os.chdir(os.path.pardir)

# Deleting intermediate files after every map reduce execution for an application
print("Deleting teh intermediate files on disk...")
files = glob.glob('tmp\\*.txt', recursive = True)

for f in files:
    try:
        os.remove(f)
    except OSError as e:
        print("Error: %s : %s" % (f, e.strerror))
print("######################## Map Reduce Completed for all Applications ####################")

print("##########################################################################")