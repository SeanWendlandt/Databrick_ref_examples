# Databricks notebook source
# Sean Wendlandt 4/5/23 -- Lab 2

sc = spark.sparkContext
list_1 = [x for x in range(100,10000)]
P_RDD1 = sc.parallelize(list_1)

### PRIME EXERCISE ###
# define prime function
def isPrime(x):
    for y in range(2, x):
        if x%y == 0:
            return False
    return True

# print the total number of prime numbers
print(P_RDD1.filter(isPrime).count())


### CELCIUS EXERCISE ###
# creating 1000 random Fahrenheit temperatures between 0..100 degrees F
import random
list_2 = [random.randrange(0, 100) for x in range(1000)]
P_RDD2 = sc.parallelize(list_2)


# convert the full list to Celsius
def Celcius(x):
    return ((x-32)*5)/9

Celcius_RDD = P_RDD2.map(Celcius)

# Example using lambda
Celcius_RDD = P_RDD2.map(lambda x: ((x-32)*5)/9)
Celcius_ABOVE_FREEZE = Celcius_RDD.filter(lambda x: x > 0).persist() 


# print the avg C temperature of all values above freezing
print(round(Celcius_ABOVE_FREEZE.reduce(lambda x, y: x+y)/Celcius_ABOVE_FREEZE.count(),2))


# COMMAND ----------


