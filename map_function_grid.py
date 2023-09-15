def map_grid_fun(l):
    p = []

    for x in l:
        p.append(float(x))

    split = 5  # default number of split per dimension for grid partitioning
    dim = 4

    comb = []
    for i in range(dim):
        comb.append(int(p[i] * split))

    key = tuple(comb)
    p.insert(0, key)
    res = tuple(p)

    return res
