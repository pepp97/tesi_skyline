from operator import add
from pyspark.sql import SparkSession
import os
import math
import time


def parallel_reduction(this_skyline):

      ret = []
      all_skylines = sorted(broadcastSky.value)
      for p in this_skyline:
            dominated = False
            for item in all_skylines:
                  if dominated: break
                  if item == p:
                        break
                  discard = 0
                  for i in range(dim):
                        if item[i] <= p[i]:
                              discard += 1
                        elif item[i] > p[i]:
                              break
                  if discard == dim:
                        dominated = True
                        break

            if dominated == False: ret.append(p)


      return tuple(ret)


def compute_sky(input_points):

      points = []
      for x in input_points:
            points.append(x[1])

      sorted_points = sorted(points)
      window = []
      for point in sorted_points:
            to_window = True
            for x in window:
                  if x == point:
                        break
                  dominated = 0
                  for d in range(dim):
                        if point[d] < x[d]:
                              break
                        else:
                              dominated += 1
                  if dominated == dim:
                        to_window = False
                        break
            if to_window: window.append(point)

      return [window]

def compute_partitions(input_points):
      #print('partition')

      p = input_points.strip().split(" ")
      point = []

      for x in p:
            point.append(float(x))
      key = ''

      for dim in range(len(point) - 1):
            x = pow(float(point[dim]), 2)
            x1 = pow(float(point[dim + 1]), 2)
            angle = math.acos(float(point[dim]) / math.sqrt(x + x1))

            part = int(split * angle * 2 / math.pi)
            key += str(part)

      res = []
      res.append(int(key))
      res.append(tuple(point))

      return tuple(res)







BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# create the session
spark = SparkSession.builder \
    .master('local[*]') \
    .config("spark.driver.memory", "15g") \
    .getOrCreate()

rdd = spark.sparkContext.textFile(BASE_DIR + '/tesi/dataset.txt', 8)
t = time.time()
print('start')
dim = 4
split = 5


local_sky=rdd.map(compute_partitions).partitionBy(max(split**(dim-1),8)).mapPartitions(lambda x:compute_sky(x))
#insieme skyline locali per calcolare skyline globale in parallelo, verrà passato come broadcast variable.
all_locals=[]
for x in local_sky.collect():
      all_locals+=x

broadcastSky = spark.sparkContext.broadcast(all_locals)


res=local_sky.map(parallel_reduction).reduce(add)

print(len(res))
print("execution time: "+str(time.time()-t))



