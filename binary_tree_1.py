class Node:
    def __init__(self, data):
        self.left   = None
        self.right  = None
        self.data   = data
    
    def insert_data(self, dt):
        if self.data:
            if dt < self.data:
                if self.left is None:
                    self.left = Node(dt)
                else:
                    self.left.insert_data(dt)
            
            elif dt > self.data:
                if self.right is None:
                    self.right = Node(dt)
                else:
                    self.right.insert_data(dt)
    

    def inorder_traversal(self, root_node):
        res = []
        if root_node:
            res = self.inorder_traversal(root_node.left)
            res.append(root_node.data) 
            res = res + self.inorder_traversal(root_node.right)
        return res
    
    
if __name__ == '__main__':
    obj = Node(27)
    obj.insert_data(14)
    obj.insert_data(35)
    obj.insert_data(10)
    obj.insert_data(19)
    obj.insert_data(31)
    obj.insert_data(42)
    print obj.inorder_traversal(obj)
