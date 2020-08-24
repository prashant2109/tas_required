alist = [12, 11, 13, 5, 3, 6]
for i in range(0, len(alist) - 1):
    smallest = i
    for j in range(i + 1, len(alist)):
        if alist[j] < alist[smallest]:
            smallest = j
    alist[i], alist[smallest] = alist[smallest], alist[i]
