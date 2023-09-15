from operator import itemgetter
import math

def red_fun(points):
    dim = 4  # default value for dimension is 4.

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

