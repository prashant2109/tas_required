class Node:
    def __init__(self, data):
        self.data = data
        self.next = None
    
class LinkedList:
    def __init__(self): 
        self.head = None

    def add_f(self, elem):
        temp = Node(elem)
        temp.next = self.head
        self.head = temp
    
    def add_m(self, elem, atr_elem):
        current = self.head
        while current != None:
            if current.data == atr_elem:
                temp  = Node(elem)
                temp.next = current.next
                current.next = temp
                break
            else:
                 current = current.next
        return
        
    def add_e(self, elem):
        if self.head in None:   
            self.head = Node(elem)
            return
        current = self.head
        while True:
            if current.next == None:
                temp = Node(elem)
                current.next = temp
                break
            else:
                current = current.next 
        return







