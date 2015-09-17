#
# Hydra Distributed Computing Framework
# Client-side (slave)
# Ben Bartlett
#

import os, sys, time
import shutil
import socket
import hashlib
import xmlrpclib
import zipfile
import multiprocessing as mp
from threading import Thread
from SimpleXMLRPCServer import SimpleXMLRPCServer

masterNodeIP = 'http://' + 'localhost'
endkey       = "&endkey&"
checksumkey  = "&checksum&" # Key to initiate file check
checkRate    = 0.05 # Rate at which servers check for new messages 
chunkSize    = 2**16 
msgSize      = 4096
selfIP       = socket.gethostbyname(socket.gethostname())  
nodeID       = 0            

def startClient():
    '''Returns a Client() instance and the associated XMLRPC server.'''
    # Allow xmlrpclib to support really long integers. Some random bug I found.
    xmlrpclib.Marshaller.dispatch[type(0L)] = lambda _, v, w: w("<value><i8>%d</i8></value>"%v)
    # Initialize 
    host = selfIP
    port = commsPort
    # Start function server
    server = SimpleXMLRPCServer((host, port), logRequests=True, allow_none=True)
    client = Client()
    server.register_instance(client)
    return client, server
    # server.serve_forever()

def shutdown():
    # Time-delayed global shutdown to prevent xmlrpclib errors when calling shutdown
    time.sleep(.5)
    print ""
    sys.stdout.write("Shutting down")
    sys.stdout.flush()
    for i in range(10):
        time.sleep(.25)
        sys.stdout.write(".")
        sys.stdout.flush()
    os._exit(0)

class Client(object): 
    '''Client class to communicate with master node.''' 
    def __init__(self): 
        '''Connect to master node on main port'''
        self.job       = self.getCurrentJob()
        self.status    = "waiting"
        self.substatus = ""
        self.host      = masterNodeIP
        self.port      = commsPort
        self.fport     = filePort

        # # Start comms server
        # self.server = SimpleXMLRPCServer((self.host, self.port), logRequests=True, allow_none=True)
        # self.server.register_introspection_functions()
        # self.server.register_multicall_functions()
        # self.registerCommsFunctions()
        # self.server.serve_forever()


        # self.s         = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # self.s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # self.s.connect((self.host, self.port)) 
        # self.listenLoop()

    def ping(self):
        return True

    # def registerCommsFunctions(self):
    #     '''Registers all invokable communications functions.'''
    #     self.server.register_function(self.getCurrentJob)
    #     self.server.register_function(self.sendStatus)
    #     self.server.register_function(self.getCurrentJob)
    #     self.server.register_function(self.shutdown)

    def getCurrentJob(self):
        '''Gets the current job by looking in the "currentJob" directory.'''
        listdir = filter(lambda f: (not f.startswith('.')) and f.endswith('.zip'), os.listdir('currentJob/'))
        if len(listdir) > 1:
            raise RuntimeError('More than one folder in currentJob directory!')
        elif len(listdir) == 0:
            return ""
        else:
            return listdir[0] 

    def updateJob(self, newjob):
        '''Updates the current job.'''
        self.job = newjob

    def receiveCurrentJob(self):
        '''Fetches the current job file from the master node, deleting any present jobs.'''
        # Clear jobs folder
        shutil.rmtree("currentJob") 
        os.mkdir("currentJob")
        # Fetch new job
        proxy = xmlrpclib.ServerProxy(str(masterNodeIP)+":"+str(self.fport))        
        with open("currentJob/"+self.job+".zip", "wb") as handle:
            handle.write(proxy.sendCurrentJob().data)
        # Extract job
        with zipfile.ZipFile("currentJob/"+self.job+".zip", 'r') as z:
            z.extractall("currentJob/")
        # Copy functions.py file to appropriate directory 
        shutil.copyfile("currentJob/"+self.job+"/functions.py", "functions/functions.py")
        # Tell server to refresh function server
        ready = proxy.refreshFunctionServer()
        return ready

    # def listenLoop(self):
    #     '''Loop until a command is detected.'''
    #     while True:
    #         time.sleep(checkRate)
    #         data = self.s.recv(msgSize).split(endkey)
    #         print data
    #         for cmd in data:
    #             self.commands(cmd)

    # def commands(self, data):
    #     '''Parses all commands passed to it and activates correct function.'''
    #     if data.startswith("openFileTCP"): 
    #         fileName = data.split(":")[1]
    #         self.receiveFile(fileName)
    #     if data == "ping":
    #         self.send("ping")
    #     if data == "requestStatus":
    #         self.sendStatus()
    #     if data == "shutdown":
    #         self.close()
    #         sys.exit() 

    # def send(self, data):
    #     print data
    #     self.s.sendall(data+endkey)

    # def receive(self):
    #     msg = self.s.recv(msgSize).split(endkey)
    #     print msg
    #     return msg

    def sendStatus(self):
        self.send(":".join(["status", self.job, self.status, self.substatus]))

    def sendFile(self, fileName, numTries=0, quiet=True):
        if numTries == 3: # Throw an error after third attempt - something is probably wrong.
            raise RuntimeError('Third attempt to send file %s; aborting.' % fileName)
        self.send("openFileTCP:"+fileName) # Send message to open 18xxx socket for file transfer
        # Open socket and attach
        self.fs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.fs.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.fs.bind(('localhost', self.fport))
        self.fs.listen(1)
        self.fconn, self.faddr = self.fs.accept()
        # Wait for response
        self.readyForTransfer()
        # Send the file
        if not quiet: print "Sending file..."
        f = open(fileName,'rb')
        l = f.read(chunkSize)
        while (l):
            self.fconn.sendall(l)
            l = f.read(chunkSize)
        f.close()
        if not quiet: print "File sent."
        # Send the checksum
        checksum = hashlib.md5(open(fileName, 'rb').read()).digest()
        self.fconn.send(checksumkey)
        self.fconn.send(checksum)
        if not quiet: print "Checksum sent."
        # Close 18xxx port
        self.fconn.close()
        self.fs.close()
        # Get response from client to verify checksum
        response = self.receive()[0]
        if response == "valid":
            if not quiet: print "File %s sent successfully." % fileName
            return 0
        elif response == "error":
            if not quiet: print "There was an error in sending file %s. Retrying..." % fileName
            self.sendFile(fileName, numTries=numTries+1) # Repeat transfer if incorrect

    def receiveFile(self, fileName, quiet=True):
        # Open 18xxx socket for file trasfer
        if not quiet: print "Receiving..."
        self.fs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.fs.connect((self.host, self.fport))
        if not quiet: print "Connected."
        # Send a ready signal on the 18xxx channel
        self.fs.sendall("ready")
        # Open file and read contents from socket, writing to file
        f = open(fileName,'wb')
        l = self.fs.recv(chunkSize)
        while (len(l.split(checksumkey)) == 1):
            f.write(l)
            l = self.fs.recv(chunkSize)
        f.write(l.split(checksumkey)[0])
        f.close()
        # Receive and calculate checksums
        sentChecksum = l.split(checksumkey)[1]
        sentChecksum += self.fs.recv(chunkSize) # Recieve one more chunk just in case it got split
        # Close 18xxx port
        self.fs.close()
        # Compare checksum results and send back on 17xxx channel
        checksum = hashlib.md5(open(fileName, 'rb').read()).digest()
        if checksum != sentChecksum:
            if not quiet:
                print "Error: checksum %s does not equal sent checksum %s"%(checksum, sentChecksum)
            # self.send("error")
            return -1
        else:
            if not quiet: print "Checksums are valid."
            # self.send("valid")
            return 0

    def close(self):
        # self.s.close()
        exitThread = Thread(target = shutdown)
        exitThread.start()
        return True

def runFunctionServer(quiet=True):
    FS = functionServer(quiet=quiet)
    FSserver = SimpleXMLRPCServer((FS.host, FS.port), logRequests=True, allow_none=True)
    FSserver.register_instance(FS) 
    FSserver.serve_forever()

class functionServer(object):
    '''Creates a remote function server to allow functions to be called from the master node
    and executed on all of the child nodes.'''
    def __init__(self, quiet=True):
        # Initially import functions library
        from functions import functions
        self.functions = functions
        # Allow xmlrpclib to support really long integers. Some random bug I found.
        xmlrpclib.Marshaller.dispatch[type(0L)] = lambda _, v, w: w("<value><i8>%d</i8></value>"%v)
        # Initialize 
        self.host = selfIP
        self.port = xmlrpcPort
        # Start function server
        # self.server = SimpleXMLRPCServer((self.host, self.port), logRequests=True, allow_none=True)
        # self.server.register_introspection_functions()
        # self.server.register_multicall_functions()
        # self.server.register_function(self.refreshFunctions)
        # self.refreshFunctions()
        if not quiet: print "Function server at {} on port {}.".format(self.host, self.port)
        # self.server.serve_forever()

    def dispatcher(self, functionName, args=None):
        '''Given functionName as a string, return the corresponding function. 
        Allows for dynamic function registration.'''
        if args == None:
            return getattr(self.functions, functionName)()
        else:
            args
            return getattr(self.functions, functionName)(*args)

    def refreshFunctions(self):
        '''Registers all top-level functions in the code you provide it.'''
        self.functions = reload(self.functions)
        ready = 0
        return ready
        # fnList = [f for _, f in self.functions.__dict__.iteritems() if callable(f)]
        # for f in fnList:
        #     self.server.register_function(f)


if __name__ == '__main__':
    # Node ID retrival
    nodeLocation = str(masterNodeIP)+":20000"
    nodeServer = xmlrpclib.ServerProxy(nodeLocation) 
    nodeID = nodeServer.assignID('http://'+str(selfIP))
    # nodeID = 0
    commsPort = 17000 + nodeID 
    filePort  = 18000 + nodeID 
    xmlrpcPort = 19000 + nodeID

    # Start function server in separate process
    # functionServer()
    functionServerThread = Thread(target = runFunctionServer, args = (False,))
    functionServerThread.daemon = True
    functionServerThread.start() 

    # Start comms server in separate process
    client, commsServer = startClient()
    nodeServer.nodeReady(nodeID)
    commsServer.serve_forever()


    # clientServerThread = Thread(target = startClient)
    # clientServerThread.daemon = True 
    # clientServerThread.start()

    while True: # Loop forever to keep daemon threads alive but be able to exit with ^C
        time.sleep(2)


    #client = Client(host, port)
    #client.close()



