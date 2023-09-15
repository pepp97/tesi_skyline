from operator import add
from pyspark.sql import SparkSession
import time
import os
from pyspark import AccumulatorParam


class SetAccumulator(AccumulatorParam):

    def zero(self, init_value: set):
        return init_value

    def addInPlace(self, v1: set, v2: set):
        return v1.union(v2)

def add_key(x):
      global add_acc
      add_acc.add({x})

def reduce_set(s):
      new_list = []
      for point in sorted(s):
            p1 = []
            for i in range(dim):
                  p1.append((point[i]) / split)
            to_window = True
            for x in new_list:
                  if x == point:
                        break
                  dominated = 0
                  p2 = []
                  for i in range(dim):
                        p2.append((x[i] + 1) / split)
                  for d in range(dim):
                        if p1[d] < p2[d]:
                              break
                        else:
                              dominated += 1
                  if dominated == dim:
                        to_window = False
                        break
            if to_window:
                  new_list.append(point)
      return new_list


def compute_global_skyline(input_points):

      sorted_points = sorted(input_points)
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
      print(time.time()-t)
      return window


def compute_sky(input_points):

      points=input_points[1]

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
      res= [window,input_points[0]]
      return res

def compute_partitions(input_points):

      points = []
      points_list = input_points.strip().split(" ")
      for x in points_list:
            points.append(float(x))

      comb = []
      for i in range(dim):
            comb.append((int(points[i] * split)))
      key = tuple(comb)
      res = []
      res.append(key)
      res.append(tuple(points))

      return res

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# create the session
spark = SparkSession.builder \
    .master('local[*]') \
    .config("spark.driver.memory", "15g") \
    .getOrCreate()

rdd = spark.sparkContext.textFile(BASE_DIR + '/tesi/dataset.txt', 8)
t = time.time()
add_acc = spark.sparkContext.accumulator(set(),SetAccumulator())
print('start')
dim = 4
split = 5


mapped=rdd.map(lambda x: compute_partitions(x))
#compute bit-map
mapped.map(lambda x: add_key(x[0])).collect()
filtered=reduce_set(add_acc.value)
local_skylines=mapped.groupByKey().filter(lambda x: x[0] in filtered).map(compute_sky).reduce(add)

input_points=[]

for elem in local_skylines:
      if type(elem) is  list:
            input_points+=elem

glob=time.time()
res=compute_global_skyline(input_points)
print(len(res))
print("time= " + str(time.time() - t))



