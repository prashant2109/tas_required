import os, sys
def disableprint():
    sys.stdout = open(os.devnull, 'w')
    pass

def enableprint():
    sys.stdout = sys.__stdout__

class B:
    def __init__(self):
        pass
        
    def check_print(self):
        print 'Start'
        import d_print as pyf
        a = pyf.A()
        a.get_func()
        return 

def read_q():
    print 'Check'
    return
if __name__ == '__main__':
    b = B() 
    #disableprint()
    b.check_print()
    enableprint()
    sa = read_q()
