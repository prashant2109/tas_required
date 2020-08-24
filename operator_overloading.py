class complex(object):
    def __init__(self, a, b):
        self.a = a
        self.b = b
        
    def __add__(self, other):
        return self.a + other.a, self.b + other.b
        
    def __str__(self):
        return self.a, self.b
    
if __name__ == '__main__':
    obj = complex(1, 2)
    obj2 = complex(2, 3)
    obj3 = obj + obj2
    print obj3

