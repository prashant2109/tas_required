from random import randint
colors = {k: [] for k in 'rgb'}
n = 1
for i in range(n):
    temp = {k: randint(0, 255) for k in 'rgb'}
    for k in temp:
        while 1:
            c = temp[k]
            t = set(j for j in range(c-25, c+25) if 0 <= j <= 255)
            print t 
            if t.intersection(colors[k]):
                temp[k] = randint(0, 255)
            else:
                break
        colors[k].append(temp[k])
print([(colors['r'][i], colors['g'][i], colors['b'][i] ) for i in range(n)])


