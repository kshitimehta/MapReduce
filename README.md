# MapReduce-kshitimehta

**Collaborator : Shubhankar Ajit Kothari**

Language : Java (version 11.0.8),

Python (version 3.8.5), 

Spark (version 3.0.1)

OS : Windows 10

## User Input : 

For automated tests for 3 applications, running testfile.py should prove sufficient.

During automated testing, the processes for all three applications will be killed at a point of time and restarted.

Code :

```
py testfile.py
```

This command will execute the testing for the applications by comparing the spark/python outputs with the output we obtain from our system and also test if the workers are rebooted after being killed by invoking kill_Proc in killProcesses.py

***In our design document, we have mentioned an edge case where fault tolerance fails to work if the user kills one of the processes just before socket connections are established for both worker and master and when the socket connection is ended. If FT fails to work in your system please change the sleep timer in killProcesses.py by a few milliseconds.***

To make changes to the app.config file, the user needs to go to testfile.py and make the changes in the following format without any quotations marks in the values. Here is an example:
```
app.inputfilename = inputfile1.txt

app.outputfilename = outputfile1

app.N = 3

app.class_name = UDF1
```

***Since the code is expected to run on a different system than ours, we have given relative path names for the files in the code with respect to the code directory***

**In `sparktest.py`, we are explicitly setting the HADOOP_HOME environment, if you already have this set, please comment this line of code or set your HADOOP_HOME to where your winutils.exe exists.**

For running code on custom input and output for 1 application follow steps below,

Create their udf java file by implementing UDF interface and save it in src/mapReduce/

Update app.config file with input file, output file, N and UDF class name

1. Provide configuration details in app.config. The format for the config file we chose was of the form - 

app.inputfilename = the file location

app.outputfilename = the output file location

app.N = the number of mappers and reducers

app.class_name = the name of the UDF class

**All values must be without any quotation marks.**

2. For file paths please make sure you write in complete file path.

3. For outputfilename configuration, please provide existing directory path location.

4. For inputfilename configuration, please provide a .txt file.

5. The application we have chosen are the time series, inverted index and word count examples.

Run the following command on the command line:


py execute.py


## Execution : 

OS : Windows

Once the above steps are completed, please execute execute.py to run the system for one application:

```
py execute.py
```

testfile.py will initiate automated testing for running the map reduce library in a multiprocessed manner and will simultaneously test for fault tolerance by killing processes invoked by another script (killProcesses.py) called in the same file.

## Application Explanation :

**1. Application 1: Time Series**

Code: 

Mapreduce code in UDF1.java class

Python code in sensors_spark.py

Parameters: 

`start time
end time
window size`

Input file (inputfile1.txt): 

`time stamp (in seconds)   \t list of sensors in the form s0, s1`

Output file (code output- outputfile1/, python output- outputspark_sensors.txt):

`sensor(s0)    \t  list of time stamp gaps from the start to end in the window size.`


This application has data corresponding to sensors and their trigger time stamps. We want to extract the gaps where the sensors are not triggered. The user specifies the start, end and window of the triggers for the sensors to be monitored. If a particular time stamp is missing in the window mentioned, we want to return it with it's corresponding sensor. Therefore the final output is each sensor with its corresponding time stamp gaps in the window specified.

This is useful to know when the sensors fail to initiate the trigger and helps in monitoring the sensors. 

**2. Application 2: Inverted Index**

Code: 

Mapreduce code in UDF2.java class

Python code in invertedIndexSpark.py

Input file (inputfile2.txt):

`document(id)      \t  content in form of words (separated by commas)`

Output file (code output- outputfile2/, python output- outputspark_index.txt):

`word(1)     \t list of corresponding document(ids)`

This application takes as data document ids and the corresponding content in words. We want to build an inverted index from the data we have. We process it with map reduce to give us as output words and their corresponding list of document ids they occur in. This is a very common application used to build search engines. 

This helps in looking up the words in a particular document easily.

**3. Application 3: Word Count**

Code: 

Mapreduce code in UDF3.java class

Python code in sparktest.py

Input file (inputfile3.txt):

`words separated by spaces and in form of paragraphs`

Output file (code output- outputfile3/, python output- outputspark_WordCount.txt):

`word      \t corresponding frequency of occurence in the file`

This application takes as input a file of words separated by spaces. We extract information in a way to obtain the frequencies of each unique word in the document. We use map reduce to form this key value pair list. This application helps in understanding the importance of ranking by frequency. This use case is used by Google as part of their ranking algorithm in search engines. 

The importance of a word in a document helps in returning efficient documents while searching.

## Execution Explanation


### Execution of testfile.py :

1. Compiles the java files. 

	- For automated testing, all three udfs (one for each applications) are compiled at the start.
	
2. Updates app.config to the specific inputfile, udf and outputfile. 

3. Executes the Main java in a multi threaded manner for 

	- Call the Master Class and initiate the process.
	
4. Executes the kill_Proc method in killProcesses.py concurrently in another thread for


	- Purposely killing the processes to test for fault tolerance
	
5. Executes the single threaded or spark application of solving the same problem.

6. Executes codes to compare the map reduce output and spark/single threaded output.

   (In this step, there is some cosmetic post processing done to the outputs of the files like, empty string removal, space removal, output frills like brackets etc removal which are related to the quirks processing by pyspark)	

	-  The output will give an indication : “the outputs are the same”, if our application works correctly.

7. Repeat Steps 1-6 for each application.


### Overall Internal Execution Plan :

There are 4 main components to the code.

1. Master Process - The Master process reads the configurations and initiates the workers to complete map or reduce tasks. It initiates threads to listen to worker task status and updates status data structures using sockets. Based on the updates, if mapper successfully completes, the Master executes the reducer part and exits.

2. Worker Process - The Worker Process completes tasks assigned by the Master Process. The worker updates the master on whether the code ran successfully or if there is an error before exiting.

3. Communication Threads - The master and the workers communicate with each other through sockets. The master listens to the sockets for updates and the worker sends periodic updates. Each Master-Worker pair communicates through a unique port.

4. Status Report Data Structures - These data structures are used to keep track of active workers, inactive workers, end status of workers and number of successful completions for the master. They help the master process to shift from mapper to reducer or end the code.



### Internal Execution in the Master -

1. Initiate Master Class.
2. Read "app.config" - for parameters, initiate ports and singleton classes.
3. Set workType = 'mapper'.
4. Check port availability
6. Initiate processes with workerId, Inputfile, workType, port.
7. Initiate communication thread for worker.
8. Update status data structures like activeworkers maps, inactive worker sets.
9. Repeat steps 4 to 8 for each of the n processes.
10. Listen on worker threads for task completion, errors etc.
11. Restart process without error codes.
12. If all tasks are completed, repeat steps  3 to 10 with workType = 'reducer'
13. Exit.

### Internal Execution of Worker -

1. Initiate Worker class and read provided system arguments.
2. Initiate status report data structures.
3. Initiate communication thread with master on given port. 
	3.1 Ping status report data structures for execution update. if any update send to master else send predefined static value.
4. Initiate mapper/reducer process.
5. Close thread on completion.
6. Exit.




