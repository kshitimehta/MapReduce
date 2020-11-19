import io
import string
from numpy import loadtxt
from pyspark import SparkContext, SparkConf

print("Running Time Series Test Application on Python.... ")
if __name__ == "__main__":    
  list_time = []
  a = {} 
  start = 0
  end = 5
  window = 1
  res = {}

  y = list(range(start,end+1,window))
  y = list(str(val) for val in y)
  print("Reading the inputfile...")
  file = open("inputfile_big.txt","r")
  
  for line in file:
    # striping the leading and trailing spaces
      line = line.rstrip()
      # splitting the key and value by tab
      row = line.split("\t")
      time_stamp = row[0]
      sensors = row[1:]
      # updating the hashmap with keys as sensors and values as time stamps
      for each in sensors:
          list_time=[]
          if each in a:
              list_time = a.get(each)
          list_time = list(set(list_time))
          list_time.append(time_stamp)
          a.update({each:list(set(list_time))})
  
  # storing sensors as keys and time stamp gaps as values in hashmap
  for k,v in a.items():
      gaps = [x for x in y if x not in v]
      res.update({k:gaps})
  
  print("Writing data to spark output file...")
  with io.open("outputspark_sensors_big.txt",'w',encoding="utf-8") as f:
      for k,v in res.items():
          f.write(str(k)+"\t"+str(v))
          f.write("\n")
  print("################################## Time Series Test Completed. ##################################")
