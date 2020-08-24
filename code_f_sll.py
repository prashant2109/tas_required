class Node:
    def __init__(self, data):
        self.item = data
        self.ref  = None
    
class LinkedList:
    def __init__(self):
        self.start_node = None
    
    def traverse_list(self):
        if self.start_node is None:
            print 'List has no elements to display'
            return
        n = self.start_node
        while n:
            print n.item
            n = n.ref 
        
    def insert_at_start(self, data):
        new_node = Node(data)
        new_node.ref = self.start_node     
        self.start_node = new_node
    
    def insert_at_end(self, data):
        new_node = Node(data)
        if self.start_node is None: 
            self.start_node = new_node
            return 
        n = self.start_node
        while n:
            n = n.ref
        n.ref = new_node
        
    def insert_after_another(self, x, data):
        new_node = Node(data)
        n = self.start_node
        while n:
            if n.item == x:
                new_node.ref = n.ref
                n.ref = new_node 
                return
            n = n.ref
            
        
        
            
               
    
