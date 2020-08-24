class A:  
    def __init__(self):  
        print '__init__.A'
        self.name = 'John'  
        self.age = 23  
  
    def getName(self):  
        print 'A attribute' 
        return
  
class B:  
    def __init__(self):  
        print '__init__.B'
        self.name = 'Richard'  
        self.id = '32'  
  
    def getName(self):  
        print 'B attribute' 
        return
  
  
class C(A, B):  
    def __init__(self):  
        A.__init__(self)  
        B.__init__(self)
        
    def getName(self):
        print 'C attribute'
 

 
C1 = C()  
print(C1.getName())
