# multi_proc_map.py
#
# https://pythonprogramming.net/values-from-multiprocessing-intermediate-python-tutorial/
#
#In the above case, what we're going to do is 
# first set up the Pool object, which will 
# have 20 processes that we'll allow to do some work.

import multiprocessing
from multiprocessing import Pool

def job(num):
    process_name = multiprocessing.current_process().name
    print (process_name)
    return num * 2
    
if __name__ == '__main__':
    p = Pool(processes=10)
    data = p.map(job, [i for i in range(5)])
    p.close()
    print(data)