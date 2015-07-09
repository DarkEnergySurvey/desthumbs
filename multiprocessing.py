import multiprocessing as mp


# To use all available
NP = mp.cpu_count()

# Here you need to set args, which is a list of tuples, eacj tuple
# containing the information for each call of the function.

#randomly assigned dictionary and tuple
kw = {'val1':'dog','val2':7}

a = (1,2,3)

#Simple function for testing
def f(*args,**kwargs):
    print kwargs, args



#One way to get result
p = mp.Pool(processes=NP)
results = [p.apply_async(f, a, kw)]
p.close()
p.join()

for r in results: r.get()

#Another way
p = mp.Process(target = f, args = a, kwargs = kw)
p.start()
p.join()
print p
    