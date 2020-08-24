import itertools, operator
data = [(2, 6), (8, 4), (7, 3)]
result = itertools.starmap(operator.sub, data)
for each in result:
    print(each)
