#
# Hydra Distributed Computing Framework
# Server-side (master)
# Ben Bartlett
#

import os, sys
import shutil
import pickle 
import zipfile
import time
import cmd 
import readline
import socket
import xmlrpclib
import threading
import hashlib
import subprocess
import multiprocessing as mp
from FastProgressBar import progressbar
from threading import Thread
from platform import version as pversion
from SimpleXMLRPCServer import SimpleXMLRPCServer
from datetime import datetime
from select import select

# Constant declarations
version     = "v. 1.0"
endkey      = "&endkey&"
checksumkey = "&checksum&" # Key to initiate file check
checkRate   = 0.01 # Rate at which servers check for new messages   
chunkSize   = 2**16 # Transmit this many bytes at a time on the file connections
msgSize     = 4096 # Transmit this many bytes at a time                               
osVersion   = pversion().split(":")[0]
maxNodes    = 2**8 # Maximum nodes that can be connected
nodeIDs     = range(0, maxNodes) # Managing nodes like this helps if nodes become disconnected
selfIP      = socket.gethostbyname(socket.gethostname())
queue       = pickle.load(open("queue/~queue.mpq", 'rb'))

def clearScreen():
    '''Cross-platform "clear" function'''
    os.system('cls' if os.name == 'nt' else 'clear')
    return 0

def humanSize(nbytes):
    '''Quick function to generate human-readable file-sizes.'''
    suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    if nbytes == 0: return '0 B'
    i = 0
    while nbytes >= 1024 and i < len(suffixes)-1:
        nbytes /= 1024.
        i += 1
    f = ('%.2f' % nbytes).rstrip('0').rstrip('.')
    return '%s %s' % (f, suffixes[i])

def queueAdd(jobName):
    global queue
    queue.append(jobName)
    pickle.dump(queue, open("queue/~queue.mpq", "wb"))

def queueRemove(jobName):
    global queue
    queue.remove(jobName)
    pickle.dump(queue, open("queue/~queue.mpq", "wb"))

def exitConsole():
    '''Commands to do on exit'''
    sys.exit()

class Server(object):
    '''Single instance of a server to communicate with a single node.'''
    def __init__(self, host, nodeID):
        '''Initiate connection with node on main port'''
        self.job      = ""
        self.host     = host
        self.port     = nodeID + 17000
        self.fport    = nodeID + 18000
        self.xmlport  = nodeID + 19000
        self.client   = xmlrpclib.ServerProxy(self.host+":"+str(self.port))
        self.fnServer = xmlrpclib.ServerProxy(self.host+":"+str(self.xmlport))
        # Start the job distribution server in a new thread
        jobServerThread        = Thread(target = self.jobServer)
        jobServerThread.daemon = True
        jobServerThread.start()

        # self.s     = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # self.s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # self.s.bind((host, self.port))
        # self.s.listen(1)
        # self.conn, self.addr = self.s.accept()
        # self.requestStatus()


    def pushCurrentJob(self):
        '''Tells the client nodes to request the current job.'''
        self.client.updateJob(self.job)
        ready = self.client.receiveCurrentJob()
        return ready

    def sendCurrentJob(self):
        '''Send the current queued job to client nodes.'''
        with open("queue/"+self.job+".mpj", "rb") as handle:
            return xmlrpclib.Binary(handle.read()) 

    def refreshFunctionServer(self):
        '''Tell function server to refresh contents.'''
        ready = self.fnServer.refreshFunctions()
        return ready

    def jobServer(self):
        '''Permanent XMLRPC server for transferring current job.'''
        self.JS = SimpleXMLRPCServer(("localhost", self.fport), logRequests=False)
        self.JS.register_function(self.sendCurrentJob)
        self.JS.register_function(self.refreshFunctionServer)
        self.JS.serve_forever()

    # def send(self, data):
    #     print data
    #     self.conn.sendall(data+endkey)

    # def receive(self):
    #     msg = self.conn.recv(msgSize).split(endkey)
    #     print msg
    #     return msg

    # def ping(self):
    #     self.send("ping")
    #     self.waitUntil("ping")
    #     return True

    # def waitUntil(self, msg):
    #     while True:
    #         data = self.receive()
    #         for command in data:
    #             if command == msg:
    #                 return True

    # def requestStatus(self):
    #     self.send("requestStatus")
    #     # Wait for response
    #     while True:
    #         data = self.receive()[0]
    #         if data.startswith("status"): break
    #     status = data.split(":")[1:] 
    #     if status[0] != self.job:
    #         self.sendFile("queue/"+self.job+".mpj", saveAs="currentJob/"+self.job+".zip")
    #         # Tell the computer to stop and move to the next job
    #         return 0
    #     if status[1] == "waiting":
    #         # If node is waiting, no data on data queue, so waiting on other nodes to finish
    #         return 0
    #     elif status[1] == "working":
    #         # If node is working, everything is as it should be
    #         if status[2] == "completed": # If completed, look for next item on data stack
    #             return 0
    #         else:
    #             return 0

    # def readyForFileTransfer(self):
    #     while True:
    #         data = self.fconn.recv(msgSize)
    #         if data == "ready":
    #             return True

    # def sendFile(self, fileName, saveAs=None, numTries=0, quiet=True):
    #     if not quiet: print "Starting..."
    #     if numTries == 3: # Throw an error after third attempt - something is probably wrong.
    #         raise RuntimeError('Third attempt to send file %s; aborting.' % fileName)
    #     # See if we should save it as something else:
    #     if saveAs == None:
    #         saveAs = fileName
    #     # Open socket and attach
    #     self.fs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #     self.fs.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    #     self.fs.bind(('localhost', self.fport))
    #     if not quiet: print "Listening..."
    #     self.client.receiveFile(saveAs, False)
    #     time.sleep(1)
    #     self.fs.listen(1)
    #     self.fconn, self.faddr = self.fs.accept()
    #     # Wait for response
    #     self.readyForFileTransfer()
    #     # Send the file
    #     if not quiet: print "Sending file..."
    #     f = open(fileName,'rb')
    #     l = f.read(chunkSize)
    #     while (l):
    #         self.fconn.sendall(l)
    #         l = f.read(chunkSize)
    #     f.close()
    #     if not quiet: print "File sent."
    #     # Send the checksum
    #     checksum = hashlib.md5(open(fileName, 'rb').read()).digest()
    #     self.fconn.send(checksumkey)
    #     self.fconn.send(checksum)
    #     if not quiet: print "Checksum sent."
    #     # Close 18xxx port
    #     self.fconn.close()
    #     self.fs.close()
    #     # Get response from client to verify checksum
    #     response = self.receive()[0]
    #     if response == "valid":
    #         if not quiet: print "File %s sent successfully." % fileName
    #         return 0
    #     elif response == "error":
    #         if not quiet: print "There was an error in sending file %s. Retrying..." % fileName
    #         self.sendFile(fileName, numTries=numTries+1) # Repeat transfer if incorrect

    # def receiveFile(self, fileName, quiet=True):
    #     # Open 18xxx socket for file trasfer
    #     self.fs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #     self.fs.connect((self.host, self.port + 1000))
    #     # Send a ready signal on the 18xxx channel
    #     self.fs.sendall("ready")
    #     # Open file and read contents from socket, writing to file
    #     f = open(fileName,'wb')
    #     l = self.fs.recv(chunkSize)
    #     while (len(l.split(checksumkey)) == 1):
    #         f.write(l)
    #         l = self.fs.recv(chunkSize)
    #     f.write(l.split(checksumkey)[0])
    #     f.close()
    #     # Receive and calculate checksums
    #     sentChecksum = l.split(checksumkey)[1]
    #     sentChecksum += self.fs.recv(chunkSize) # Recieve one more chunk just in case it got split
    #     # Close 18xxx port
    #     self.fs.close()
    #     # Compare checksum results and send back on 17xxx channel
    #     checksum = hashlib.md5(open(fileName, 'rb').read()).digest()
    #     if checksum != sentChecksum:
    #         if not quiet:
    #             print "Error: checksum %s does not equal sent checksum %s"%(checksum, sentChecksum)
    #         self.send("error")
    #         return -1
    #     else:
    #         if not quiet: print "Checksums are valid."
    #         self.send("valid")
    #         return 0

    # def listener(self):
    #     data = self.conn.recv(chunkSize)
    #     if data != '':
    #         print repr(data)
    #     threading.Timer(checkRate, self.listener).start()


class masterServer(object):
    '''Master node controller that tells all Server() objects what to do.'''
    def __init__(self, nodeserver, quiet=False):
        self.quiet = quiet
        self.busy = False
        self.jobsToProcess = 0
        self.updateJob()
        self.NS = nodeserver # Assign node server
        # Start main processing loop in another thread
        self.mainLoopThread = Thread(target = self.mainLoop)
        self.mainLoopThread.daemon = True
        self.mainLoopThread.start()

    def mainLoop(self):
        while True:
            if self.busy == False and (self.jobsToProcess>0 or self.jobsToProcess==-1):
                nNodes = len(filter(lambda x: x != -1, self.NS.nodes))
                if nNodes == 0:
                    print "\nNodeError: no connected slave nodes."
                    print "Stopping processing."
                    sys.stdout.write(">> ") # Print new input character
                    sys.stdout.flush()
                    self.jobsToProcess = 0
                    continue
                if self.getJob() != "":
                    self.doJob() 
                    if self.jobsToProcess > 0: 
                        self.jobsToProcess -= 1 
                else:
                    time.sleep(2.5)
            time.sleep(2.5) # Sleeps are to keep average CPU usage low

    def updateJob(self):
        self.job = self.getJob()
        if not self.quiet: print "\nUpdated current job to {}.".format(self.job)

    def getJob(self):
        # jobQueue = pickle.load(open("queue/~queue.mpq", 'rb'))
        # return jobQueue[0]
        if len(queue)>0: 
            return queue[0]
        else:
            return ""
        # with open("queue/~queue.mpq", "r") as queueFile:
        #     contents = queueFile.read()
        #     if contents != "":
        #         return contents.splitlines()[0]
        #     else:
        #         return ""

    def pushJobs(self):
        servers = filter(lambda x: x != None, self.NS.servers)
        if not self.quiet:
            pbar = progressbar("Pushing to &count& nodes:", len(servers))
            pbar.start()
            i=0
        for node in servers:
            node.job = self.job
            node.pushCurrentJob()
            if not self.quiet:
                i += 1
                pbar.update(i)
        if not self.quiet:
            pbar.finish()

    def doJob(self):
        '''Processing a job. Pushes to nodes, extracts, and runs.'''
        self.busy = True
        self.updateJob() 
        if not self.quiet: print "Starting processing of {}...".format(self.job)
        self.pushJobs() # Push copies of all jobs to all nodes and refresh function servers
        # Clean currentJob folder
        try:
            shutil.rmtree("queue/currentJob")
        except OSError: # Folder is already removed
            pass
        os.mkdir("queue/currentJob")
        # Extract to folder
        with zipfile.ZipFile("queue/"+self.job+".mpj", 'r') as z:
            z.extractall("queue/currentJob")
        # Read from run.txt
        with open("queue/currentJob/"+self.job+"/run.txt", "r") as queueFile:
            contents = queueFile.read()
            commands = contents.splitlines() 
        # Run using subprocess.popen
        success = True
        err = ''
        out = ''
        os.chdir("queue/currentJob/"+self.job) # Change to run directory
        if not self.quiet: print "Running..." 
        for command in commands:
            print "  > Running command {}".format(command)
            proc = subprocess.Popen(command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            proc.wait()
            err += proc.stderr.read()
            out += proc.stdout.read()
            if err != '':
                success = False
        if success:
            if not self.quiet: print "Run successful."
        else:
            if not self.quiet: print "Run unsuccessful; errors were encountered."
        os.chdir("../../..") # Change back to server directory
        # Generate run summary report
        if not self.quiet: print "Processing of {} complete. Generating log...".format(self.job)
        with open("queue/currentJob/"+self.job+"/summary.txt", "w") as f:
            f.write("Run summary of {} completed at {}:\n".format(self.job, str(datetime.now())))
            if success:
                f.write("Run successful. See below for stdout contents.\n")
            else:
                f.write("Run unsuccessful; errors were encountered. See below for stderr and stdout contents.\n")
                f.write("-"*80 + "\n")
                f.write("stderr contents:\n")
                f.write(err)
                f.write("\n")
            f.write("-"*80 + "\n")
            f.write("stdout contents:\n")
            f.write(out)
            f.write("\n")
        # Zip it all back up and put it in the completed folder
        shutil.make_archive("completed/"+self.job, 'zip', "queue/currentJob/"+self.job)
        # Remove the directory
        shutil.rmtree("queue/currentJob")
        # Remove original job archive and update queue
        if not self.quiet: print "Cleaning up..."
        os.remove("queue/"+self.job+".mpj")
        queueRemove(self.job)
        if not self.quiet: print "Done.\n"
        sys.stdout.write(">> ") # Print new input character
        sys.stdout.flush()
        # Ready for next job
        self.busy = False

    def shutdownNodes(self):
        servers = filter(lambda x: x != None, self.NS.servers)
        if len(servers) == 0:
            print "No slave nodes to shut down."
            return 0
        print ""
        pbar = progressbar("Shutting down &count& nodes:", len(servers))
        pbar.start()
        i = 0
        for node in servers:
            node.client.close()
            i += 1
            pbar.update(i)
        pbar.finish()
        print ""
        return 0



def startNodeServer(NS):
    xmlNS = SimpleXMLRPCServer(('localhost', 20000), logRequests=False, allow_none=True)
    xmlNS.register_instance(NS)
    xmlNS.serve_forever()

class nodeServer(object):
    '''Simple XMLRPC server that assigns node IDs when first connecting new nodes 
    and tracks currently connected nodes.'''

    def __init__(self, quiet=True):
        # Allow xmlrpclib to support really long integers. Some random bug I found.
        self.quiet      = quiet
        self.addresses  = [""] * maxNodes
        self.nodes      = [-1] * maxNodes # Global array representing which nodes are connected
        self.servers    = [None] * maxNodes # Actual array of server objects
        self.processes  = [None] * maxNodes # Array of server processes
        if not quiet: print "Initialized."

    def assignID(self, IP):
        '''Gives the client a port in exchange for their IP'''
        index                 = self.nodes.index(-1)
        ID                    = nodeIDs[index]
        self.nodes[index]     = ID  
        self.addresses[index] = str(IP)
        pickle.dump(self.nodes, open("nodes.dat", "wb")) # Save nodes array to a file accessable by CFQueue    
        pickle.dump(self.addresses, open("addresses.dat", "wb"))
        # if not self.quiet: print "Added node at {} with nodeID {}.".format(IP, ID)
        return ID

    def nodeReady(self, nodeID):
        '''Tells master node that a client node is ready and starts corresponding Server() process'''
        time.sleep(1) # Just to make sure the client is started 
        server               = Server(self.addresses[nodeID], self.nodes[nodeID])
        self.servers[nodeID] = server 
        # masterNode.servers[nodeID] = server # Add to master node
        # if not self.quiet: print "Started server {} on process {}.".format(server, serverProcess)
        if not self.quiet: 
            print ""
            print "-> {:8}: Automatically added node {} at {}:"\
                .format(time.strftime('%X'), nodeID, self.addresses[nodeID].split("http://")[1])
            print " "*(5+8) + "Node object: {}".format(server)
            sys.stdout.write(">> ") # Print new input character
            sys.stdout.flush()

    def distributeNodeInfo(self):
        '''Distributes current node information to objects wanting to use it, like distributed queues.'''
        return self.addresses, self.nodes

    def printInfo(self):
        print self.addresses
        print self.nodes
        print self.servers
        print self.processes


class Console(cmd.Cmd):
    '''Interactive command-line interface for the server.'''
    def __init__(self, MS, NS):
        cmd.Cmd.__init__(self)
        print ""
        clearScreen()
        self.MS = MS # Allows the prompt to manipulate the master server
        self.NS = NS
        self.prompt = ">> "
        self.intro = "Hydra Computing Framework "+version+"\n"+\
                     "Lastest build: "+time.ctime(os.path.getmtime("Server.py"))+"\n"+\
                     "OS: "+osVersion+"\n"+\
                     'Type "help" for more information.'

    def do_clear(self, args):
        '''clear: Clears the screen. Equivalent to clear on Unix and cls on Windows.'''
        print ""
        clearScreen()

    def do_verify(self, jobName, quiet=False):
        errorString = ""
        warningString = ""
        if jobName.endswith(".zip"):
            jobName = jobName[:-4]
        try:
            with zipfile.ZipFile("jobs/"+jobName+".zip", 'r') as z:
                os.mkdir("jobs/~verification/")
                z.extractall("jobs/~verification/")
        except IOError:
            if not quiet:
                print "IOError: %s is not a valid job. \n" %jobName + \
                      "Use list_jobs to get a list of current queued and unqueued jobs."
            return False
        # Check for run script, more tests later
        if not os.path.isfile("jobs/~verification/"+jobName+"/run.txt"):
            errorString   += 'Error: %s is invalid. ' % jobName + \
                             'Cause: missing file "run.txt" in top directory.\n'
        # if not os.path.isfile("jobs/~verification/"+jobName+"/cfg.txt"):
        #     warningString += 'Warning: %s contains no cfg.txt file. ' % jobName + \
        #                      'Assuming defaults for all options.\n'
        # if not os.path.isfile("jobs/~verification/"+jobName+"/setup.txt"):
        #     warningString += 'Warning: %s contains no setup.txt file. ' % jobName + \
        #                      'If you require external dependencies, they will not be installed.\n'
        # Clean up folder
        shutil.rmtree("jobs/~verification")
        if errorString != "":
            if not quiet: print errorString
            return False
        else:
            if not quiet and warningString != "": print warningString
            if not quiet: print jobName + " has been validated."
            return True

    def help_verify(self):
        print "\n".join([
        'verify jobName',
        'Verifies a job has the needed structure and generates a filelist. Verified',
        'jobs are stored as .mpj files.',
        '   jobName: string: name of the .zip files containing the job source code.',
        '                    Extension does not need to be included. Does not run lists.',
        '   Example: verify Job1',
        'This function is called automatically on queueing a job.'
        ])

    def do_queue(self, jobNames):
        moved = []
        for jobName in jobNames.split():
            if jobName.endswith(".zip"):
                jobName = jobName[:-4]
            try:
                # Files are renamed to signify they have been validated
                print "Validating %s..." % jobName
                if self.do_verify(jobName):
                    shutil.move("jobs/"+jobName+".zip", "queue/"+jobName+".mpj")
                    moved.append(jobName)
            except IOError:
                print "IOError: %s does not exist. \n" %jobName + \
                      "Use list_jobs to get a list of current queued and dequeued jobs."
            queueAdd(jobName)
        if moved != []:
            print "Successfully added " + ", ".join(moved) + " to the processing queue.\n"

    def help_queue(self):
        print "\n".join([
        'queue jobName',
        'Adds a job to the processing queue. ',
        '   jobName: string: name or list of names of the .zip files containing the',
        '                    job source code. Extension does not need to be included.',
        '   Example: queue Job1 Job2.zip Job3 Job4.zip',
        'Jobs are stored as source files and an execution script in',
        'a .zip file in the "jobs" directory'
        ])

    def do_dequeue(self, jobNames):
        moved = []
        for jobName in jobNames.split():
            if jobName.endswith(".mpj"):
                jobName = jobName[:-4]
            try:
                # Files are renamed for additional ease of differentiation for queued vs.
                # unqueued jobs and to prevent people from messing with them while queued.
                queueRemove(jobName)
                shutil.move("queue/"+jobName+".mpj", "jobs/"+jobName+".zip")
                moved.append(jobName)
            except IOError:
                print "IOError: %s does not exist. \n" %jobName + \
                      "Use list_jobs to get a list of current queued and dequeued jobs."
        if moved != []:
            print "Successfully removed " + ", ".join(moved) + " from the processing queue.\n"

    def help_dequeue(self):
        print "\n".join([
        'dequeue jobName',
        'Removes a job from the processing queue. ',
        '   jobName: string: name or list of names of the .mpj files containing the',
        '                    job source code. Extension does not need to be included.',
        '   Example: dequeue Job1 Job2.mpj Job3 Job4.mpj'])

    def do_start(self,args):
        '''start: Processes jobs as they are added to the queue until "stop" is issued.'''
        print "Now processing jobs.\n"
        self.MS.jobsToProcess = -1 

    def do_stop(self,args):
        '''stop: Stops processing jobs. The current job will finish first, but no further jobs will be started.'''
        if self.MS.job != "":
            print "Processing will be stopped after {} finishes processing.\n".format(self.MS.job)
        else:
            print "Processing stopped."
        self.MS.jobsToProcess = 0

    def do_process(self,args):
        '''process [n=1]: Processes the next n queued jobs.'''
        n = 1
        if args:
            try: 
                n = int(args)
            except ValueError:
                print "ValueError: n must be a positive integer."
                return -1
        self.MS.jobsToProcess = n 
        print "Processing next {} jobs.".format(self.MS.jobsToProcess)
        return 0

    def do_list_jobs(self, args):
        '''list_jobs: Lists the current queued and dequeued jobs. Use -t to sort chronologically.'''
        dequeuedJobs = [f for f in os.listdir("jobs") if f.endswith(".zip")]
        queuedJobs = [f for f in os.listdir("queue") if f.endswith(".mpj")]
        completedJobs = [f for f in os.listdir("completed") if f.endswith(".zip")]
        if args == "-t":
            dequeuedJobs.sort(key=lambda f: os.path.getmtime("jobs/"+f))
            queuedJobs.sort(key=lambda f: os.path.getmtime("queue/"+f))
            completedJobs.sort(key=lambda f: os.path.getmtime("completed/"+f))
        print ""
        print "Queued Jobs:"
        print "{:15}  {:12}  {:16}".format("Job Name:", "Size:", "Added/Completed:")
        print "-"*80
        for f in queuedJobs:
            print "{:15}  {:12}  {:16}".format(f, humanSize(os.path.getsize("queue/"+f)), 
                time.ctime(os.path.getmtime("queue/"+f)))
        print ""
        print "Dequeued Jobs:"
        print "{:15}  {:12}  {:16}".format("Job Name:", "Size:", "Added/Completed:")
        print "-"*80
        for f in dequeuedJobs:
            print "{:15}  {:12}  {:16}".format(f, humanSize(os.path.getsize("jobs/"+f)), 
                time.ctime(os.path.getmtime("jobs/"+f)))
        print ""
        print "Completed Jobs:"
        print "{:15}  {:12}  {:16}".format("Job Name:", "Size:", "Added/Completed:")
        print "-"*80
        for f in completedJobs:
            print "{:15}  {:12}  {:16}".format(f, humanSize(os.path.getsize("completed/"+f)), 
                time.ctime(os.path.getmtime("completed/"+f)))
        print ""


    def do_list_nodes(self, args):
        '''list_nodes: Prints information about the currently connected nodes.'''
        nodes = filter(lambda x: x != -1, self.NS.nodes)
        addresses = [a.split("http://")[1] for a in filter(lambda x: x != '', self.NS.addresses)]
        servers = filter(lambda x: x != None, self.NS.servers) 
        print ""
        print "{} currently connected nodes.".format(len(nodes) + 1)
        print "{:<6}  {:<16}  {:<45}".format("Node:", "IP:", "Object:")
        print "-"*(80)
        print "{:<6}  {:<16}  {:<45}".format("Master", selfIP, self.MS)
        for node, address, server in zip(nodes, addresses, servers):
            print "{:<6}  {:<16}  {:<45}".format(node, address, server)
        print ""

    def do_cmd(self, args):
        subprocess.call(args.split())

    def help_cmd(self):
        print "\n".join([
            "cmd systemCommand:",
            "    Directly executes a shell command. Does not allow changing directory."
            "    Example: cmd ls -l ../Client"])

    def do_status(self, args):
        '''status: Prints current system information.'''
        nodes = filter(lambda x: x != -1, self.NS.nodes)
        dequeuedJobs = [f for f in os.listdir("jobs") if f.endswith(".zip")]
        queuedJobs = [f for f in os.listdir("queue") if f.endswith(".mpj")]
        queuedJobs.sort(key=lambda f: os.path.getmtime("queue/"+f))
        completedJobs = [f for f in os.listdir("completed") if f.endswith(".zip")]
        print ""
        print "System information at {}:\n".format(time.strftime('%X'))
        status = "busy" if self.MS.busy else "idle"
        numJobs = "(infinite)" if self.MS.jobsToProcess==-1 else str(self.MS.jobsToProcess)
        nextJob = queuedJobs[-1] if len(queuedJobs) > 1 else "(none)"
        print "Processing status: {}.  Connected nodes: {}."\
            .format(status, len(nodes)+1)
        print "Current job: {}.  Next job: {}.  Process {} more jobs."\
            .format(self.MS.job, nextJob, numJobs)
        print "Queued jobs: {}.  Dequeued jobs: {}.  Completed jobs: {}."\
            .format(len(queuedJobs), len(dequeuedJobs), len(completedJobs))
        print "\nUse list_jobs, list_nodes for additional information.\n"

    def do_about(self, args):
        '''about: Displays framework information.'''
        print "Hydra Computing Framework "+version
        print "Lastest build: "+time.ctime(os.path.getmtime("Server.py"))

    def do_exit(self, args):
        '''exit: Exits from console.'''
        s = raw_input("This will shut down all nodes. Continue? [y/n] ")
        if s in ["y","Y","yes","Yes","YES"]:
            self.MS.shutdownNodes()
            print "Shutting down master node..."
            exitConsole()

    def do_EOF(self,args):
        '''EOF (^D): Exits from console.'''
        print ""
        return self.do_exit(args)



if __name__ == '__main__':
    # Start node server in separate thread
    NS = nodeServer(quiet=False)
    nodeServerThread = Thread(target = startNodeServer, args = (NS,))
    nodeServerThread.daemon = True
    nodeServerThread.start()

    # Start master server in a separate thread
    MS = masterServer(NS, quiet=False)
    # masterServerThread = Thread(target = masterServer, args = (NS,))
    # masterServerThread.daemon = True
    # masterServerThread.start()

    # time.sleep(5)
    # NS.printInfo()

    console = Console(MS, NS)
    console.cmdloop()


    # print "Sending file"
    # NS.servers[0].pushCurrentJob()
    # print "Sent."

    # time.sleep(3)

    while True: # Loop forever to keep daemon threads alive but be able to exit with ^C
        time.sleep(2)

    # for server in servers:
    #     if server != None:
    #         server.close()

    # nodeServer(quiet=False)

    # s=Server(addresses[0], nodes[0])
    # s.sendFile("1.png")
    # # # s.sendFile("2.png")
    # # # s.sendFile("3.png")
    # s.close()



