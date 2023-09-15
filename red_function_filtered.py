from operator import itemgetter
import math

def red_fun_with_filtering(points):
    dim = 4
    sorted_points = sorted(points)
    window = []
    min_dist=10
    selected=()
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
            distance=math.dist(point,(0,0,0,0))
            if distance<min_dist:
                min_dist=distance
                selected=point
            window.append(point)


    return (window,selected)
