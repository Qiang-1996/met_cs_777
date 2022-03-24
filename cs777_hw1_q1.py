from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext

# Define two functions for data cleaning.
def isfloat(value):
  try:
    float(value)
    return True
  except:
    return False

def correctRows(row):
  if (len(row)==17):
    if (isfloat(row[5]) and isfloat(row[11])):
      if(float(row[4])> 60 and float(row[5])>0.10 and float(row[11])> 0.10 and float(row[16])> 0.10):
        return row

# This is a simple Python script that accepts two command line parameters.
# First parameter sys.argv[1] is the input file, and the second sys.argv[2] is the output file
# By overwrinting the code in this block, I can create the python script for the assignment.
# This is the python script for assignment 1, question 1.
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <file> <output> ", file=sys.stderr)
        exit(-1)
        
    sc = SparkContext()
# Load the data and split the data by ","
    df = sc.textFile(sys.argv[1]).map(lambda x: x.split(","))
# Data cleaning
    taxiRDD = df.filter(correctRows)

# Code to get the top 10 active taxis.
    activeTaxi_RDD = taxiRDD.map(lambda x:(x[0],x[1]))\
        .distinct()\
        .map(lambda x:(x[0],1))
    active_count = activeTaxi_RDD.reduceByKey(lambda x,y: x+y)
    Top10_active_taxi = active_count.top(10, lambda x:x[1])
# Save the result.
    sc.parallelize(Top10_active_taxi).saveAsTextFile(sys.argv[2])
    
    sc.stop()