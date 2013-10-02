#!/usr/local/cdat/bin/python

"""Some util methods and classes"""

class Struct(object):
    """This class is used for converting dictionaries into classes.
        Use like this:
            >>> dic = {'a': 1, 'b': 2}
            >>> a = Struct(**dic)
            >>> a.a
            1
            >>> a.b
            2

            Other posible uses are:
            >>> Struct(**{'a' : 1, 'b' : 2})
            >>> Struct(a=1, b=2)"""

    def __init__(self, **entries):
        self.__dict__.update(entries)

    def __getattr__(self, name):
        return None

    def toDict(self):
        """Transfrom this struct to a dictionary."""
        result = {}
        for i in self.__dict__:
            if isinstance(self.__dict__[i], Struct): result[i] = self.__dict__[i].toDict()
            else: result[i] = self.__dict__[i]
        return result    

    def __repr__(self):
        return "<Struct (%s)>" % ",".join([ "=".join([att, str(self.__dict__[att])]) for att in self.__dict__ if not att.startswith('_')])

class StatisticalOperator(object):
    def __init__(self, seed=None):
        import random

        self.rand = random.Random(seed)
     
    def __probToFloat(self, percentage):
        if percentage > 1: return float(percentage)/100.0
        else: return float(percentage)

    def apply(self, function, iterable, probability, result=False):
        probability = self.__probToFloat(probability)
        if result: rArray = []

        for i in iterable:
            if (self.rand.random() <= probability):
                #apply function
                r = function(i)
                if result: rArray.append(r)
            elif result: rArray.append(i)
        if result: return rArray

    def getIndexSample(self, minVal, maxVal, amount=None, density=None):
        if amount and density: raise Exception('You must set only one of either amount or density')
        if not amount and not density: raise Exception('You must set one of either amount or density')

        if density: return self.rand.sample(range(minVal,maxVal+1), int(maxVal * self.__probToFloat(density)))
        else: return self.rand.sample(range(minVal,maxVal+1), amount)

import subprocess
class ParallelProcessor(object):
    """Use subprocess to issue multiple process at once. After the maximal number of
    processes is reached, the call will block and wait for the first one issued to be finished."""
    __def_popen_args = {'stdout':subprocess.PIPE, 'stderr':subprocess.PIPE}
    
    _runnning = None    #process running list => len(_running) <= _max_procs
                        #                     => _running[0] ==
                        #                     subprocess.Popen()
                        #                     => _running[1] == user_object
    
    _finished = None    #finished process list  =>result[0].keys() == [errorcode, stderr, stdout]
                        #                       =>result[1] == user_object
    
    _max_procs = None   #max number of process running at the same time
    
    def __init__(self, max_procs=3):
        """max_procs: maximal number of parallel process"""
        self._running = []
        self._finished = []
        self._max_procs = max_procs
    
    def addProcess(self, command, user_object=None, **popen_args):
        """Issue a new process; blocks until the oldest process finishes
        command := command as it will be issued to subprocess.Popen (arguments list or string if shell=True)
        user_object := (opetional) it allows to attach an object to this call that will get returned when finished
        popen_args := arguments for subproces.Popen. Default: stdout=subprocess.PIPE, stderr=subprocess.PIPE"""
        
        #use default if no other
        if not popen_args:
            popen_args = self.__def_popen_args
        
        if len(self._running) >= self._max_procs:
            #wait for one to be freed
            
            #extract first element
            proc, old_user_object = self._running[0]
            self._running = self._running[1:]
            
            #wait for the process to finish and return results
            returncode = proc.wait();
            stdout, stderr = proc.communicate()
            
            #store as a finished process
            self._finished.append(({'returncode': returncode, 'stderr' : stderr, 'stdout': stdout}, old_user_object))
        
        self._running.append((subprocess.Popen(command, **popen_args), user_object))
        return self
        
    def wait(self):
        """wait for all pending processes to be finished"""
        for proc, old_user_object in self._running:
            ret_val = proc.wait();
            stdout, stderr = proc.communicate()
            
            self._finished.append(({'returncode': ret_val, 'stderr' : stderr, 'stdout': stdout}, old_user_object))
        self._running = []
        
    def getResults(self):
        """returns a shallow copy of the results list"""
        return self._finished[:]
    
    def cleanFinished(self):
        """Cleans the finished list"""
        self._finished = []
        
    def abort(self):
        """Aborts all pending results"""
        #rescue what we can, and abort pending
        for proc, old_user_object in self._running:
            ret_val = proc.poll();
            if ret_val:
                stdout, stderr = proc.communicate()
                self._finished.append(({'returncode': ret_val, 'stderr' : stderr, 'stdout': stdout}, old_user_object))
            else:
                try:
                    proc.terminate()
                except OSError:
                    pass #it might "just" have finished
        self._running = []

if __name__=='__main__':

    def show(evalStr):
        print evalStr
        exec(evalStr)

    show("""
dict = {'a': 1, 'b' : 2}
test = Struct(**dict)
print ">", test.a, test.b, test.c""")
    show("""
test = Struct(**{'a': 1, 'b' : 2})
print ">", test.a, test.b, test.c""")
    show("""
test = Struct(a=1, b=2)
print ">", test.a, test.b, test.c""")
    show("""
s = StatisticalOperator()
orig=[0]*10
print s.apply(lambda x: 1, orig, 50, result=True)
print orig""")
    show("""
s = StatisticalOperator(1)
#should be 473 (because of seed)
r = sum(s.apply(lambda x: 1, [0]*1000, 50, result=True))
print r
assert r == 473
s = StatisticalOperator(1)
#same as above!
r = sum(s.apply(lambda x: 1, [0]*1000, 0.5, result=True))
assert r == 473

s = StatisticalOperator()
#should be about 500, but not exactly (free seed)
r = sum(s.apply(lambda x: 1, [0]*1000, 50, result=True))
print r, "<- about 500?"

#now exagerate and the error should be minimal
r = sum(s.apply(lambda x: 1, [0]*1000000, 0.1, result=True))
print "Error: ", (r/1000000.0)-0.1


""")
    

