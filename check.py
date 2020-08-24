class DecoratorExample:
  """ Example Class """
  def __init__(self):
    """ Example Setup """
    #print('Hello, World!') 
    pass

  @staticmethod
  def example_function():
    """ This method is a static method! """
    print('I\'m a static method!')
 
de = DecoratorExample()#.example_function()
print de.example_function()
