from pyspark.sql import SparkSession
import os
import time
from pyspark import AccumulatorParam
from operator import add


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

def compare_key(k1, k2):
    t1 = list(k1)
    t2 = list(k2)

    for a, b in zip(t1, t2):
          if b <= a: return True
    return False


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

      return window

def compute_partitions(input_points):

    points = []
    point_list = input_points.strip().split(" ")
    for x in point_list:
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
print('start')
add_acc = spark.sparkContext.accumulator(set(),SetAccumulator())
dim = 4
split = 5

mapped=rdd.map(lambda x: compute_partitions(x))
#compute bit-map
mapped.map(lambda x: add_key(x[0])).collect()
filtered=reduce_set(add_acc.value)
local_skylines=mapped.groupByKey().filter(lambda x: x[0] in filtered).map(compute_sky)

all_locals_skylines=local_skylines.reduce(add)
broadcastSky = spark.sparkContext.broadcast(all_locals_skylines)

res=local_skylines.map(parallel_reduction).reduce(add)
print(len(res))

print("time= " + str(time.time() - t))





