from pyspark.sql import SparkSession
import os
import math
import time



def compute_global_skyline(input_points):

    sorted_points = sorted(input_points)
    window = []
    for point in sorted_points:
        to_window = True
        for x in broadcastFilter.value:
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
        if not to_window: continue
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


def compute_sky(input_points):

    points = []
    for x in input_points:
        points.append(x[1])

    min_dist = 10
    selected = ()
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
        if to_window:
            distance = math.dist(point, (0, 0, 0, 0))
            if distance < min_dist:
                min_dist = distance
                selected = point
            window.append(point)

    window.append(selected)
    return [window]



def compute_partitions(input_points):
    points = []
    point_list = input_points.strip().split(" ")
    for x in point_list:
        points.append(float(x))

    comb = ''
    for i in range(dim):
        comb += str(int(points[i] * split))

    key = int(comb)

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
dim = 4
split = 5

local_skyline = rdd.map(compute_partitions).partitionBy(split ** (dim)).mapPartitions(lambda x: compute_sky(x)).collect()

input_points = []
filter = []
#shuffling
for x in local_skyline:
    input_points += x[:len(x) - 1]
    if len(x) > 1:
        filter.append(x[len(x) - 1])

broadcastFilter = spark.sparkContext.broadcast(filter)
result = compute_global_skyline(input_points)
print("time= " + str(time.time() - t))
print(len(result))
