alist = [12, 11, 13, 5, 3, 6]
for i in range(len(alist) - 1, 0, -1):
    for j in range(0, i):
        if alist[j + 1] < alist[j]:
            print 'br', alist
            alist[j], alist[j + 1] = alist[j + 1], alist[j]
            print 'at', alist
