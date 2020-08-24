class Person(object):
    def __init__(self, name, age, rid):
        self.name = name
        self.age  = age
        self.rid  = rid
        
    def get_name(self):
        return self.name
    
    def get_age(self):
        return self.age
        
    def get_rid(self):
        print 'PPP', self.rid
        return self.rid

class Student(object):
    def __init__(self, i_d):
        self.Id = i_d 
        
    def get_rid(self):
        print 'SSS', self.Id
        return self.Id
    
class Resident(Student, Person):
    def __init__(self, name, age, rid, Id):
        Person.__init__(self, name, age, rid)
        Student.__init__(self, Id)
        
    def get_rd(self):
        return self.get_rid()
    
r1 = Resident('G', 18, 121323, 404)
#print r1.get_rid()
print r1.get_rd()
        
        
    
