class Node:
    def __init__(self, item):
        self.prev = None
        self.data = item
        self.next = None

class DLL:
    def __init__(self):
        self.head = None  
        self.previous_obj = None 
   
    def add_f(self, new_elem):
        temp = Node(new_elem)
        if self.head == None and self.previous_obj == None:
            self.head == temp
            self.previous_obj = temp
        else:
            self.head = temp
            temp.next = self.previous_obj
            self.previous_obj.prev = temp

            

if __name__ == '__main__':
    mlist = DLL()
    




 
