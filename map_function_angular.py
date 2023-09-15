import math

def map_angular_fun(points):
    p = []
    for x in points:
        p.append(float(x))
    key = []
    partitions_num = 5  # default number of split per dimension for grid partitioning
    for dim in range(len(p) - 1):
        x = pow(float(p[dim]), 2)
        x1 = pow(float(p[dim + 1]), 2)
        angle = math.acos(float(p[dim]) / math.sqrt(x + x1))
        part = int(partitions_num * angle * 2 / math.pi)
        key.append(part)

    p.insert(0, tuple(key))

    return tuple(p)
