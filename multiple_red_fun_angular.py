from operator import itemgetter

def parallel_reducers_angular(points_tmp):
    ret = []
    result = sorted(points_tmp[1])

    points = points_tmp[0]

    dim = 4
    for p in points:
        dominated = False
        for x in result:
            if dominated: break
            if x == p:
                break

            discard = 0
            for i in range(dim):
                if x[i] <= p[i]:
                    discard += 1
                elif x[i] > p[i]:
                    break
            if discard == dim:
                dominated = True
                break

        if dominated == False: ret.append(p)

    return tuple(ret)





