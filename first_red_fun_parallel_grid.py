from operator import itemgetter

def first_red(points_key):
    points = points_key[:len(points_key) - 1]
    key = points_key[len(points_key) - 1]
    comparisons = 0
    dim = 4
    sorted_points = sorted(points, key=itemgetter(0, 1,2,3))
    window = []
    for point in sorted_points:
        to_window = True
        for x in window:
            if x == point:
                break
            dominated = 0
            comparisons += 1
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

