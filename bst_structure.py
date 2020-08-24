class Node:
    def __init__(self, data):
        self.left   = None
        self.right  = None
        self.data   = data 
        
    def insert_elems(self, data):
        if self.data:
            if data < self.data:    
                if self.left is None:
                    self.left = Node(data)
                else:
                    self.left.insert_elems(data)
            elif data > self.data:
                if self.right is None:
                    self.right = Node(data)
                else:
                    self.right.insert_elems(data)
        else:
            self.data = data 
        
    def find_elems(self, elem):
        if self.data:
            if elem == self.data:
                return 'Found'
                
            elif elem < self.data:  
                if self.left is None:
                    return str(elem)+ " Not Found "
                return self.left.find_elems(data)
            
            elif elem > self.data:
                if self.right is None:
                    return str(elem) + "Not Found"
                return self.right.find_elems(data)       
        
         
