#!/usr/bin/env python

import multiprocessing as mp
#import mp.Pool

# To use all available
NP = 8 #mp.cpu_count()

# Here you need to set args, which is a list of tuples, eacj tuple
# containing the information for each call of the function.

#randomly assigned dictionary and tuple
kw = {'val1':'dog','val2':7}

a = (1,2,3)

#Simple function for testing
def f(*args,**kwargs):
    #print kwargs, args
    s1 = kwargs
    s2 = args
    print "#----"
    print s1
    print s2
    print "#----"
    return s1,s2


# One way to get result
p = mp.Pool(processes=NP)

#for k in range(NP):
#    a  = (k+1,k+2,k+3)
#    kw = {'val1':k, 'val2': k+2}
#    p.apply_async(f, a, kw)
#p.close()
#p.join()
#exit()

for k in range(NP):
    a  = (k+1,k+2,k+3)
    kw = {'val1':k, 'val2': k+2}
    mp.Process(target = f, args = a, kwargs = kw).start()
    #p.start()
    #p.join()

exit()
    
results = [p.apply_async(f, a, kw)]
p.close()
p.join()
exit()

for r in results: r.get()

#Another way
p = mp.Process(target = f, args = a, kwargs = kw)
p.start()
p.join()
print p
    
