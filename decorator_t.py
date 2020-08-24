from functools import wraps

def call_counter(func): 
    def wrapper(*args, **kwargs):
        wrapper.calls += 1
        return func(*args, **kwargs)
    wrapper.calls = 0
    return wrapper

@call_counter
def succ(x):
    return x + 1

@call_counter
def ml(x, y=1):
    return x*y +1


def grt(func):
    @wraps(func)
    def function_wrapper(x):
        """ function_wrapper of greeting """
        print("Hi, " + func.__name__ + " returns:")
        return func(x)
    return function_wrapper

@grt
def su(y):
    return y + 's'

print su('hi')
print su.__name__

'''
def deco_para(*args, **kwargs):
    def wrapper(func):
        print kwargs['l']
        return func(*args, **kwargs)
    return wrapper

@deco_para(l='para executed')
def sm(x):  
    return x + 1

print sm(2)
'''
