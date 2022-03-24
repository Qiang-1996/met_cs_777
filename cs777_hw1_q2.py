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
# This is the python script for assignment 1, question 2.
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <file> <output> ", file=sys.stderr)
        exit(-1)
        
    sc = SparkContext()
# Load the data and split the data by ","
    df = sc.textFile(sys.argv[1]).map(lambda x: x.split(","))
# Data cleaning
    taxiRDD = df.filter(correctRows)


# Code to get the top 10 best drivers.
# Create two key-value maps one is (taxi,trip_time) and another is (taxi,money)
# Use reduce function to get the total time and total money for each taxi.
    driver_trip_minute = taxiRDD.map(lambda x:(x[1],float(x[4])/60)).reduceByKey(lambda x,y: x+y)
    driver_trip_money = taxiRDD.map(lambda x:(x[1],x[16])).reduceByKey(lambda x,y: float(x)+float(y))
    driver_money_per_minute = driver_trip_minute.join(driver_trip_money)
# Compute the money per minute for each kdriver.
    driver_money_per_minute = driver_money_per_minute.map(lambda x:(x[0],float(x[1][1])/float(x[1][0])))
# Get the top 10 driver
    Top10_Best_Driver = driver_money_per_minute.top(10,lambda x:x[1])

# Save the result.
    sc.parallelize(Top10_Best_Driver).saveAsTextFile(sys.argv[2])
    
    sc.stop()