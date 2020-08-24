class Node:
    def __init__(self, data):
        self.pref = None
        self.nref = None
        
class DoubleLinkedList:
    def __init__(self):
        self.start_node = None
    
    def insert_item(self, data):
        if self.start_node is None:
            new_node = Node(data)
            self.start_node = new_node
        
    def insert_at_start(self, data):
        if self.start_node is None:
            new_node = Node(data)
            self.start_node = new_node
            return
        new_node = Node(data)
        new_node.nref = self.start_node
        self.start_node.pref = new_node
        self.start_node = new_node
        
    def insert_at_end(self, data):
        if self.start_node is None:
            new_node = Node(data)
            self.start_node = new_node
            return
        new_node = Node(data)
        n = self.start_node
        while n.nref is not None:
            n = n.nref
        n.nref = new_node
        new_node.pref = n
        
        
        
                 


