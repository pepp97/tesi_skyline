from pyspark.sql import SparkSession
import os
import math
import time


def compute_global_sky(points):
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


def compute_sky(partition):
    points = []
    for x in partition:
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

    return window


def compute_partitions(input_list):
    points_list = input_list.strip().split(" ")
    points = []
    for x in points_list:
        points.append(float(x))
    key = ''

    for dim in range(len(points) - 1):
        x = pow(float(points[dim]), 2)
        x1 = pow(float(points[dim + 1]), 2)
        angle = math.acos(float(points[dim]) / math.sqrt(x + x1))

        part = int(split * angle * 2 / math.pi)
        key += str(part)

    res = []
    res.append(int(key))
    res.append(tuple(points))

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

local_sky = rdd.map(compute_partitions) \
    .partitionBy(max(split ** (dim - 1), 8)) \
    .mapPartitions(lambda x: compute_sky(x)) \
    .collect()

glob = time.time()
res = compute_global_sky(local_sky)
print('global skyline computation time: ' + str(time.time() - glob))
print('global skyline len: ' + str(len(res)))
print("time= " + str(time.time() - t))
