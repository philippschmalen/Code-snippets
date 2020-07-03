from random import randint
import time


# set sample size
n = 100000

# create list with n samples containing 0 or 1
l = [randint(0, 1) for i in range(n)]

### main timing function
def timing(f):
    def wrap(*args):
        time1 = time.time()
        ret = f(*args)
        time2 = time.time()
        print('{:s} function took {:.3f} ms'.format(f.__name__, (time2-time1)*1000.0))

        return ret
    return wrap

# USAGE of timing() with decorators
@timing
def count_elements_len(numlist):
	return len([i for i in l if i == 1]) 

@timing
def count_elements_count(numlist):
	return l.count(1)

count_elements_count(l)
count_elements_len(l)

print(">>> .count() to get elements count is much faster than using list comprehensions")

