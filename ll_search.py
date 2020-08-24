class Node:
    def __init__(self, initdata):
        self.data = initdata
        self.next = None

    def getData(self):
        return self.data
    
    def getNext(self):
        return self.next

    def setData(self, newdata):
        self.data = newdata

    def setNext(self, newnext):
        self.next  = newnext

class UnorderedList:
    def __init__(self):
        self.head = None

    def isEmpty(self):
        return self.head == None

    def add(self, item):
        temp = Node(item)
        temp.setNext(self.head)
        self.head = temp

    def size(self):
        current = self.head
        count = 0
        while current != None:
            count = count + 1
            current = current.getNext()
        return count

    def search(self,item):
        current = self.head
        found = False
        while current != None and not found:
            if current.getData() == item:
                found = True
            else:
                current = current.getNext()
        return found

if __name__ == '__main__':
    #t1 = Node(93)
    #t2 = Node(94)
    #t3 = Node(95)
    #t4 = Node(96)
    #t5 = Node(97)
    #
    #t1.next = t2
    #t2.next = t3
    #t3.next = t4
    #t4.next = t5
    #
    #current = t1
    #while current:
    #    print current.data
    #    current = current.next        

    mlst = UnorderedList()
    mlst.add(23)
    mlst.add(31) 
    mlst.add(50)
    mlst.add(69)
    mlst.search(51) 
