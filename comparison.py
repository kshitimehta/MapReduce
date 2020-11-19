import io
import ast
import os
import numpy as np
import re

def comparison():
    
    # Application 1
    print("Comparing Application 1: Time Series Data\n")

    #Spark output transformed into a hashmap
    print("Reading the spark output....")
    sparkfile = open("..\\outputspark_sensors.txt","r")

    sensors_output = {}
    sensors_spark = {}
    for row in sparkfile:
        #print(row)
        gap_list=[]
        pairs = row.split("\t")
        #print(pairs)
        key = pairs[0]
        gaps = pairs[1]
        #print(gaps)
        for gap in gaps.rstrip():
            if gap!="[" and gap!="]" and gap!="'" and gap!="," and gap!=" " and gap!="\n":
                gap_list.append(gap)
        sensors_spark.update({key:gap_list})

    print("Reading the (MapReduce) code output....")
    #code ouptut
    dir = os.listdir("..\\outputfile1/")
    for file in dir:
        with open("..\\outputfile1/"+file,"r") as f:
            y1 = f.readlines()
            for y in y1:
                y = re.sub("\n","",y)
                y = y.strip(",")
                j = y.split("\t")
                sensor = j[0]
                time = j[1].split(",")
                sensors_output.update({sensor:time})

    print("Executing Comparison for the outputs from spark and the code...")
    if sensors_spark == sensors_output:
        print("The outputs are the same")
    else:
        print("The outputs don't match")
        for key in sensors_spark:
            if key in sensors_output and sensors_spark[key]!=sensors_output[key]:
                print("For key ", key)
                print("Spark values ", sensors_spark[key])
                print("Code output values ",sensors_output[key])
                
        if list(sensors_spark.keys()) != list(sensors_output.keys()):
            print("Keys are not the same, the difference between the spark output and code output...")
            print(np.setdiff1d(list(sensors_spark.keys()),list(sensors_output.keys())))
            
    print("################################## Application 1 Completed. ##################################")
    ################################## 

    #Application 2

    print("\nComparing Application 2: Word Count Data\n")

    print("Reading the spark output....")
    sparkfile = open("..\\outputspark_WordCount.txt","r")
    #f = open("comparisonFile.txt","w")

    sparkoutput = {}
    codeoutput = {}
    sparklist=sparkfile.read()
    sparklist = ast.literal_eval(sparklist) 
    #print(sparklist[:10])
    for tuples in sparklist:
            k,v = tuples
            k = k.strip("'")
            if k in sparkoutput:
                sparkoutput[k]+=int(v)
            else:
                sparkoutput[k]=int(v)
   
    print("Reading the (MapReduce) code output....")
    #code outpu
    dir = os.listdir("..\\outputfile2/")    
    for i in dir:
        with open("..\\outputfile2/"+i,"r") as f:
            y1 = f.readlines()
            for y in y1:
                j = y.split("\t")
                j[1] =  j[1].replace("\n","")
              
                if j[0] in codeoutput:
                    codeoutput[j[0]] += int(j[1])
                else:
                    codeoutput[j[0]] = int(j[1])

    print("Executing Comparison for the outputs from spark and the code...")
    if sparkoutput == codeoutput:
        print("The outputs are the same")
    else:
        print("The outputs don't match")
        for key in sparkoutput:
            if key in codeoutput and sparkoutput[key]!=codeoutput[key]:
                print("For key ", key)
                print("Spark output values ",sparkoutput[key])
                print("Code output values ",codeoutput[key])
                
        if list(sparkoutput.keys()) != list(codeoutput.keys()):
            print("Keys are not the same, the difference between the spark output and code output...")
            print(np.setdiff1d(list(sparkoutput.keys()),list(codeoutput.keys())))
            print(np.setdiff1d(list(codeoutput.keys()),list(sparkoutput.keys())))

    print("################################## Application 2 Completed. ##################################")
      ##################################      

    # Application 3
    print("\nComparing Application 3: Inverted Index\n")

    #Spark output transformed into a hashmap
    print("Reading the spark output....")
    sparkfile = open("..\\outputspark_index.txt","r")
    sensors_output = {}
    sensors_spark = {}
    for row in sparkfile:
        #print(row)
        doc_list=[]
        pairs = row.split("\t")
        #print(pairs)
        key = pairs[0].lstrip()
        docs = pairs[1].replace("[","").replace("]","").replace("'","")
        for doc in [i.lstrip().rstrip() for i in docs.split(",")]:
            if doc!="[" and doc!="]" and doc!="'" and doc!="\n":
                doc_list.append(doc.rstrip())
        sensors_spark.update({key:set(doc_list)})
  

    #code ouptut
    print("Reading the (MapReduce) code output....")
    dir = os.listdir("..\\outputfile3/")
    for file in dir:
        with open("..//outputfile3/"+file,"r") as f:
            y1 = f.readlines()
            for y in y1:
                y = re.sub("\n","",y)
                y = y.strip(",")
                j = y.split("\t")
                word = j[0]
                doc_list = [i.lstrip().rstrip() for i in j[1].replace("[","").replace("]","").split(",")]
                sensors_output.update({word:set(doc_list)})


    print("Executing Comparison for the outputs from spark and the code...")
    if sensors_spark == sensors_output:
        print("The outputs are the same")
    else:
        print("The outputs don't match")
        for key in sensors_spark:
            if key in sensors_output and sensors_spark[key]!=sensors_output[key]:
                print("For key ", key)
                print("Spark output values",sensors_spark[key])
                print("Code output values ",sensors_output[key])
                
        if list(sensors_spark.keys()) != list(sensors_output.keys()):
            print("Keys are not the same, the difference between the spark output and code output...")
            print(np.setdiff1d(list(sensors_spark.keys()),list(sensors_output.keys())))
    print("################################## Application 3 Completed. ##################################")
    print("Comparison completed for all applications")


if __name__ == "__main__":
    comparison()


        
    


    
