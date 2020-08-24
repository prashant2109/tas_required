import heapq
from itertools import chain

def ksmallest(inp):
        
    result = inp[0]
    for row in inp[1:]:
        for ele in row:
            heapq.heappush(result, ele)
    ksmal = heapq.nsmallest(9, result)
    #print heapq.heappushpop(result, 11)
    print type(ksmal)
if __name__ == '__main__':
    inp = [[10, 25, 20, 40], 
           [15, 45, 35, 30], 
           [24, 29, 37, 48], 
           [32, 33, 39, 50]]
    ksmallest(inp)

