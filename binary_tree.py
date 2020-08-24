class Node(object):
    def __init__(self, val):
        self.l = None
        self.r = None
        self.v = val
    
class Tree(object):
    def __init__(self):
        self.root = None
    
    def add_el(self, vl):
        if self.root != None:
            self.__add_el(vl, self.root)
        else:
            self.root = Node(val)

    
    def __add_el(self, vsl, nd_obj):
        if vsl < nd_obj.v:
            if nd_obj.l not None:
                self.__add_el(vsl, nd_obj.l)
            else:
                nd_obj.l = Node(vsl)
                
        else:
            if nd_obj.r not None:
                self.__add_el(vsl, nd_obj.r)
            else:
                nd_obj.r = Node(vsl)
