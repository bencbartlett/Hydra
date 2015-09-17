# HQueue
# Allows for queueing of remote function calls in the Hydra Computing Framework
# Ben Bartlett

import xmlrpclib
import Queue
#import multiprocessing 
import pickle
from threading import Thread

class HQueue(object):
    '''Creates a processing queue to distribute function calls to the servers'''
    def __init__(self, ordered=False, quiet=True):
        # self.ipA         = pickle.load(open("addresses.dat", 'rb'))
        # self.ipN         = pickle.load(open("nodes.dat", 'rb'))
        self.unprocessed = Queue.Queue()
        self.processed   = Queue.Queue()
        self.ordered     = ordered
        self.quiet       = quiet
        self.taskNum     = 0
        self.nodes       = []
        self.loop        = True
        self.connect()
        if not quiet: print "Initialized."

    def put(self, task, args=None):
        '''Adds a task to the queue.'''
        if self.ordered:
            self.unprocessed.put((task, args, self.taskNum))
            if not self.quiet: print "Put %s, %s to queue." % (task, args)
            self.taskNum += 1
        else: 
            self.unprocessed.put((task, args))
 
    def get(self):
        '''Gets a processed task from the queue. You can use this while the queue is running.'''
        if self.processed.empty():
            return -1
        else: 
            self.taskNum -= 1
            return self.processed.get() 

    def getAll(self):
        '''For use only after processing is finished. Also returns sorted output if ordered=True'''
        if self.ordered:
            results = [None]*self.processed.qsize()
            while not self.processed.empty():
                out, index     = self.processed.get()                
                results[index] = out
            self.taskNum = 0
            # if not self.quiet: print results
            return results
        else: 
            results = []
            while not self.processed.empty():
                results.append(self.processed.get())
            self.taskNum = 0
            return results

    def connect(self):
        self.proxy = xmlrpclib.ServerProxy("http://localhost:20000")
        self.ipA, self.ipN = self.proxy.distributeNodeInfo()
        for address, node in zip(self.ipA, self.ipN):
            if address != '':                
                # Connect to each individual non-empty node                
                self.nodes.append(xmlrpclib.ServerProxy(address+":"+str(node + 19000)))

    def process(self, wait=True):
        '''Start everything processing, wait until finished and return all results.'''
        if not self.quiet: print "Starting processing..."
        threads = []
        for node in self.nodes:
            worker = Thread(target = self.sender, args = (node,))
            threads.append(worker)
            worker.start()
            if not self.quiet: print "Starting thread " + str(worker)
        if wait:
            for t in threads:
                t.join()
            return self.getAll()

    def sender(self, node):
        '''Thread that keeps sending requests until it's told to stop'''
        while self.loop:
            if self.unprocessed.empty(): # Break if there's nothing left to process
                self.loop = False
                break
            else: 
                if self.ordered:
                    f, args, index = self.unprocessed.get()
                    #if not self.quiet: print f, args, index
                    if args == None:
                        f = node.dispatcher(f)#getattr(node, f)()
                    else: 
                        f = node.dispatcher(f, args)#getattr(node, f)(*args)
                else:
                    f, args = self.unprocessed.get()
                    if args == None:
                        f = node.dispatcher(f)#getattr(node, f)()
                    else: 
                        f = node.dispatcher(f, args)#getattr(node, f)(*args)
            if self.ordered:
                self.processed.put((f, index))
            else:
                self.processed.put(f)
        return True


if __name__ == '__main__':
    # Demonstration function
    import time
    q = HQueue(ordered=True, quiet=False)
    for i in range(100):
        q.put("add", args =(i, 1))
    results = q.process()
    print results




